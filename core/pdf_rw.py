import os
from dataclasses import dataclass
from typing import Optional, Tuple

import pdfplumber
from PyPDF2 import PdfReader, PdfWriter
from sqlalchemy.ext.asyncio import AsyncSession

from config import config
from services.printed_codes import register_code_if_new
from .patterns import *
import asyncio

@dataclass(frozen=True)
class CutResult:
    head_path: Optional[Path]
    shortage: int


# 🔧 helpers (оффлоад синхронщины в поток)
async def _to_thread(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)


def _write_pdf(writer: PdfWriter, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "wb") as f:
        writer.write(f)

def _replace_file(tmp_path: Path, target: Path) -> None:
    os.replace(tmp_path, target)

def _strip_all_ws(s: str) -> str:
    return re.sub(r"\s+", "", s).lower()

def _ascii_prefix(line: str) -> Optional[str]:
    m = RE_ASCII_PREFIX.match(line)
    return m.group(1) if m else None


def _extract_code_from_text(text: str) -> Optional[str]:
    """
    Возвращает GS1-код строго формата:
      (01)<14 цифр>(21)<ASCII-serial>
    Поддерживает переносы: сериал может быть в следующих строках и не с начала строки.
    """
    if not text:
        return None

    # 1) Вся конструкция в одной строке (со скобками)
    m_one = RE_GS1_PAREN_ONELINE.search(text)
    if m_one:
        return re.sub(r"\s+", "", m_one.group(0))

    # Подготовка строк
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return None

    def pack(head: str, tail: str) -> str:
        return re.sub(r"\s+", "", head) + re.sub(r"\s+", "", tail)

    LOOKAHEAD = 4  # сколько строк вперёд смотреть для серийника

    # 2) Со скобками, но сериал вынесен на следующие строки
    for i, ln in enumerate(lines):
        # голова до "(21)"
        m_head = re.search(r"\(\s*01\s*\)\s*\d{14}\s*\(\s*21\s*\)", ln)
        if not m_head:
            continue

        head = ln[:m_head.end()]
        tail_same = ln[m_head.end():]

        # сериал прямо после (21) в этой же строке — в любом месте
        m_ser_same = RE_ASCII_ANY.search(tail_same)
        if m_ser_same:
            return pack(head, m_ser_same.group(0))

        # сериал на одной из следующих строк (не обязательно с начала)
        for j in range(i + 1, min(i + 1 + LOOKAHEAD, len(lines))):
            m_next = RE_ASCII_ANY.search(lines[j])
            if m_next:
                return pack(head, m_next.group(0))

        # склеенный буфер хвост+следующие строки (иногда разрывы мешают)
        glued = tail_same + " " + " ".join(lines[i + 1:min(i + 1 + LOOKAHEAD, len(lines))])
        m_glued = RE_ASCII_ANY.search(glued)
        if m_glued:
            return pack(head, m_glued.group(0))

    # 3) Без скобок: "01<14>21" как подстрока строки
    #    (разрешаем мусор вокруг, главное — сама подпоследовательность)
    RE_GS1_NOPAREN_ANY = re.compile(r"01\s*\d{14}\s*21")
    for i, ln in enumerate(lines):
        m_head_inline = RE_GS1_NOPAREN_ANY.search(ln)
        if not m_head_inline:
            continue

        head = ln[m_head_inline.start(): m_head_inline.end()]
        tail_same = ln[m_head_inline.end():]

        # сериал на этой же строке
        m_ser_same = RE_ASCII_ANY.search(tail_same)
        if m_ser_same:
            return pack(head, m_ser_same.group(0))

        # сериал в следующих строках (разрешаем «не с начала»)
        for j in range(i + 1, min(i + 1 + LOOKAHEAD, len(lines))):
            m_next = RE_ASCII_ANY.search(lines[j])
            if m_next:
                return pack(head, m_next.group(0))

        # склеенный буфер
        glued = tail_same + " " + " ".join(lines[i + 1:min(i + 1 + LOOKAHEAD, len(lines))])
        m_glued = RE_ASCII_ANY.search(glued)
        if m_glued:
            return pack(head, m_glued.group(0))

    return None

def read_pdf(file_path: str | Path) -> str:
    path = Path(file_path)
    parts: list[str] = []
    try:
        with pdfplumber.open(str(path)) as pdf:
            for p in pdf.pages:
                t = p.extract_text()
                if t:
                    parts.append(t.strip())
    except FileNotFoundError:
        print(f"[read_pdf] not found: {path}")
        return ""
    except Exception as e:
        print(f"[read_pdf] failed {path}: {e}")
        return ""
    return "\n".join(parts)


# ---- поиск PDF по (артикул, размер)
def _compile_size_token(size_raw: str) -> re.Pattern:
    """
    Жёсткое совпадение конкретного значения размера пользователя (а не любого).
    - нормализуем тире к '-'
    - допускаем '-', '–', '/', между числами
    - границы токена (не буквы/цифры слева/справа)
    """
    s = re.sub(r"\s+", "", str(size_raw)).upper()
    s = s.replace("–", "-").replace("—", "-")
    if re.fullmatch(r"[2-5]?(?:XS|S|M|L|XL|XXL|XXXL)", s):
        return re.compile(rf"(?<![A-Z0-9]){re.escape(s)}(?![A-Z0-9])", re.IGNORECASE | re.MULTILINE)
    token = re.escape(s).replace(r"\-", r"[–\-\/]")
    return re.compile(rf"(?<!\w){token}(?!\w)", re.IGNORECASE | re.MULTILINE)

def find_pdfs_by_article_size_all(article: str, size: str) -> list[Path]:
    results: list[Path] = []
    if not article or not size:
        return results

    a_no_ws = _strip_all_ws(str(article))
    size_regex = _compile_size_token(size)

    for pdf_file in PDF_DIR.glob("*.pdf"):
        try:
            raw_text = read_pdf(pdf_file)
        except Exception as e:
            print(e)
            continue

        # нормализуем тире в тексте перед проверкой размера
        raw_text_norm = raw_text.replace("–", "-").replace("—", "-")

        if a_no_ws not in _strip_all_ws(raw_text):
            continue

        # размер — по нормализованному
        if size_regex.search(raw_text_norm):
            results.append(pdf_file)

    results.sort(key=lambda p: p.name.lower())
    return results

def _build_tail_writer(reader: PdfReader, total: int, keep_indexes: set[int]) -> PdfWriter:
    w = PdfWriter()
    for i in range(total):
        if i in keep_indexes:
            w.add_page(reader.pages[i])
    return w

def _extract_page_code(pl_pdf, page_index: int) -> Optional[str]:
    txt = pl_pdf.pages[page_index].extract_text(x_tolerance=1.0, y_tolerance=1.0) or ""
    return _extract_code_from_text(txt)

async def cut_first_n_pages_unique(session: AsyncSession, src_pdf: Path | str, n: int) -> Tuple[Optional[Path], int]:
    src = Path(src_pdf)
    if n <= 0:
        return None, 0

    tmp_dir = src.parent / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    try:
        reader = await _to_thread(PdfReader, str(src))
    except FileNotFoundError:
        print(f"[cut_first_n_pages_unique] not found: {src}")
        return None, n
    except Exception as e:
        print(f"[cut_first_n_pages_unique] PdfReader error for {src}: {e}")
        return None, n

    total_pages = len(reader.pages)
    to_delete: set[int] = set()
    head_writer = PdfWriter()
    unique_taken = 0

    def _read_texts():
        with pdfplumber.open(str(src)) as pl:
            return [pl.pages[i].extract_text(x_tolerance=1.0, y_tolerance=1.0) or "" for i in range(len(pl.pages))]

    try:
        texts = await _to_thread(_read_texts)
    except FileNotFoundError:
        print(f"[cut_first_n_pages_unique] not found while reading: {src}")
        return None, n
    except Exception as e:
        print(f"[cut_first_n_pages_unique] pdfplumber error for {src}: {e}")
        return None, n

    for i in range(total_pages):
        if unique_taken >= n:
            break
        try:
            code = _extract_code_from_text(texts[i])
            if not code:
                continue
            is_new = await register_code_if_new(session, code)
            if is_new:
                head_writer.add_page(reader.pages[i])
                to_delete.add(i)
                unique_taken += 1
            else:
                to_delete.add(i)
        except Exception as e:
            print(f"[cut_first_n_pages_unique] page {i} error: {e}")
            continue

    if unique_taken == 0:
        if to_delete:
            keep = set(range(total_pages)) - to_delete
            tail_writer = _build_tail_writer(reader, total_pages, keep)
            if len(tail_writer.pages) > 0:
                tail_tmp = tmp_dir / f"{src.stem}__tail_tmp.pdf"
                await _to_thread(_write_pdf, tail_writer, tail_tmp)
                await _to_thread(_replace_file, tail_tmp, src)
            else:
                try:
                    await _to_thread(src.unlink, True)
                except Exception as e:
                    print(f"[cut_first_n_pages_unique] unlink error: {e}")
        return None, n

    head_out = tmp_dir / f"{src.stem}__head_{unique_taken}.pdf"
    await _to_thread(_write_pdf, head_writer, head_out)

    keep = set(range(total_pages)) - to_delete
    if keep:
        tail_writer = _build_tail_writer(reader, total_pages, keep)
        tail_tmp = tmp_dir / f"{src.stem}__tail_tmp.pdf"
        await _to_thread(_write_pdf, tail_writer, tail_tmp)
        await _to_thread(_replace_file, tail_tmp, src)
    else:
        try:
            await _to_thread(src.unlink, True)
        except Exception as e:
            print(f"[cut_first_n_pages_unique] unlink error: {e}")

    return head_out, max(0, n - unique_taken)

def merge_pdfs(pdf_paths: list[Path | str], output_path: Path | str) -> Path:
    writer = PdfWriter()
    for p in pdf_paths:
        pth = Path(p)
        if not pth.exists():
            continue
        reader = PdfReader(str(pth))
        for page in reader.pages:
            writer.add_page(page)
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "wb") as f:
        writer.write(f)
    return out

def _normalize_columns(df) -> tuple[int, int, int]:
    required = {"артикул", "размер", "количество"}
    cols_norm = [str(c).strip().lower() for c in df.columns]
    colset = set(cols_norm)
    if not required.issubset(colset):
        missing = required - colset
        raise ValueError(f"В df нет обязательных колонок: {', '.join(sorted(missing))}")
    return cols_norm.index("артикул"), cols_norm.index("размер"), cols_norm.index("количество")

def _append_shortage(shortages: list[str], article: str, size: str, amount: int) -> None:
    shortages.append(f"{article} - размер: {size}, не хватило: {amount}")

async def build_pdf_from_dataframe(df, output_path: Path | str | None = None) -> tuple[Optional[Path], Optional[str]]:
    idx_article, idx_size, idx_qty = _normalize_columns(df)
    cut_parts: list[Path] = []
    shortages: list[str] = []

    async with config.AsyncSessionLocal() as session:
        for _, row in df.iterrows():
            article = str(row.iloc[idx_article]).strip()
            size = str(row.iloc[idx_size]).strip()
            try:
                qty = int(row.iloc[idx_qty])
            except Exception as e:
                print(e)
                continue
            if qty <= 0:
                continue

            # оффлоад поиска по PDF (внутри синхронное чтение файлов)
            try:
                pdf_paths = await _to_thread(find_pdfs_by_article_size_all, article, size)
            except Exception as e:
                print(e)
                pdf_paths = []

            if not pdf_paths:
                _append_shortage(shortages, article, size, qty)
                continue

            remaining = qty
            for src_pdf_path in pdf_paths:
                if remaining <= 0: break
                try:
                    print(f"Check {src_pdf_path}")
                    part_path, shortage = await cut_first_n_pages_unique(session, src_pdf_path, remaining)
                    took_now = max(0, remaining - shortage)
                    if took_now > 0 and part_path is not None:
                        try:
                            rr = await _to_thread(PdfReader, str(part_path))
                            if len(rr.pages) > 0:
                                cut_parts.append(part_path)
                            else:
                                try:
                                    await _to_thread(Path(part_path).unlink, True)
                                except Exception as e:
                                    print(e)
                        except Exception as e:
                            print(e)
                    remaining -= took_now
                except Exception as e:
                    print(e)
                    pass

            if remaining > 0:
                _append_shortage(shortages, article, size, remaining)

        await session.commit()

    if not cut_parts:
        report = "\n".join(shortages) if shortages else None
        return None, report

    # оффлоад слияния PDF
    try:
        result_path = await _to_thread(merge_pdfs, cut_parts, output_path or (PDF_DIR / "result.pdf"))
    except Exception as e:
        print(e)
        result_path = None

    # оффлоад удаления временных частей
    for p in cut_parts:
        try:
            await _to_thread(Path(p).unlink, True)
        except Exception as e:
            print(e)

    report = "\n".join(shortages) if shortages else None
    return result_path, report
