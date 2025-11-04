import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Tuple

import pdfplumber
from PyPDF2 import PdfReader, PdfWriter
from sqlalchemy.ext.asyncio import AsyncSession

from config import config
from services.printed_codes import register_code_if_new
from .patterns import *  # используем единые паттерны/директорию

@dataclass(frozen=True)
class CutResult:
    head_path: Optional[Path]
    shortage: int

def _assert_exists(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Файл {path} не найден")

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
    Возвращает GS1-код строго в формате:
      (01)<14 цифр>(21)<ASCII-serial>
    Допустим перенос: сериал может быть на следующей строке.
    Любые не-ASCII (напр. 'голубой') после серийника игнорируются.
    """
    if not text:
        return None

    # Разбиваем на строки и чистим
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return None

    # --- ВАРИАНТ A: всё в одной строке ---
    # Регэкс ограничивает сериал только печатным ASCII, так что кириллица не попадёт.
    m_one = RE_GS1_PAREN_ONELINE.search(text)
    if m_one:
        return re.sub(r"\s+", "", m_one.group(0))

    # Хелпер: собрать нормализованную "голову" и сериал
    def _pack(head_line: str, serial_ascii: str) -> str:
        head = re.sub(r"\s+", "", head_line)
        tail = re.sub(r"\s+", "", serial_ascii)
        return head + tail

    # --- ВАРИАНТ B: (01)…(21) в строке i, сериал может быть:
    #   - сразу за (21) на той же строке (непрерывный ASCII),
    #   - либо на следующей/через одну строке как ASCII-префикс.
    for i, ln in enumerate(lines):
        if "(01)" in ln and "(21)" in ln:
            # выцепляем «голову» вплоть до (включая) (21)
            m_head = re.search(r"\(\s*01\s*\)\s*\d{14}\s*\(\s*21\s*\)", ln)
            if not m_head:
                # нет корректной головы — пробуем следующий кейс
                continue

            head_line = ln[: m_head.end()]  # до конца '(21)'
            tail_same = ln[m_head.end():]  # всё, что после '(21)' в этой строке

            # 1) сериал на той же строке — непрерывный ASCII-префикс
            m_ser_same = re.match(r"\s*([!-~]{4,})", tail_same)
            if m_ser_same:
                return _pack(head_line, m_ser_same.group(1))

            # 2) сериал на следующей/через одну строке — ASCII-префикс строки
            for j in range(i + 1, min(i + 3, len(lines))):
                m_ser_next = RE_ASCII_PREFIX_LINE.match(lines[j])
                if m_ser_next:
                    serial_ascii = m_ser_next.group(1)
                    # Требуем хотя бы 4 ASCII-символа
                    if len(serial_ascii) >= 4:
                        return _pack(head_line, serial_ascii)
            # если дошли сюда — сериал не нашли, продолжаем поиск по другим строкам
            # (но чаще всего этого достаточно)

    # --- ВАРИАНТ C: без скобок (заголовок '01<14>21' на строке i + сериал ниже) ---
    for i, ln in enumerate(lines):
        if RE_GS1_NOPAREN_HEADLINE.match(ln):
            # сериал в этой же строке (на всякий случай)
            m_same = re.search(r"(?:\(\s*21\s*\)|21)\s*([!-~]{4,})", ln, re.IGNORECASE)
            if m_same:
                head = re.sub(r"\s+", "", ln[:m_same.start(1)])
                tail = re.sub(r"\s+", "", m_same.group(1))
                return head + tail
            # или сериал в одной из следующих строк
            for j in range(i + 1, min(i + 3, len(lines))):
                m_next = RE_ASCII_PREFIX_LINE.match(lines[j])
                if m_next and len(m_next.group(1)) >= 4:
                    head = re.sub(r"\s+", "", ln)
                    tail = re.sub(r"\s+", "", m_next.group(1))
                    return head + tail

    # --- Fallback: ничего не нашли ---
    return None


def read_pdf(file_path: str | Path) -> str:
    path = Path(file_path); _assert_exists(path)
    parts: list[str] = []
    with pdfplumber.open(str(path)) as pdf:
        for p in pdf.pages:
            t = p.extract_text()
            if t: parts.append(t.strip())
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
        except Exception:
            continue

        # нормализуем тире в тексте перед проверкой размера
        raw_text_norm = raw_text.replace("–", "-").replace("—", "-")

        # статья ищется по "сплющенному" тексту (устойчиво к переносам)
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
    src = Path(src_pdf); _assert_exists(src)
    if n <= 0:
        return None, 0

    tmp_dir = src.parent / "tmp"; tmp_dir.mkdir(parents=True, exist_ok=True)
    reader = PdfReader(str(src))
    total_pages = len(reader.pages)

    to_delete: set[int] = set()
    head_writer = PdfWriter()
    unique_taken = 0

    with pdfplumber.open(str(src)) as pl_pdf:
        for i in range(total_pages):
            if unique_taken >= n: break
            code = _extract_page_code(pl_pdf, i)
            if not code: continue
            is_new = await register_code_if_new(session, code)
            if is_new:
                head_writer.add_page(reader.pages[i])
                to_delete.add(i)
                unique_taken += 1
            else:
                to_delete.add(i)

    if unique_taken == 0:
        if to_delete:
            keep = set(range(total_pages)) - to_delete
            tail_writer = _build_tail_writer(reader, total_pages, keep)
            if len(tail_writer.pages) > 0:
                tail_tmp = tmp_dir / f"{src.stem}__tail_tmp.pdf"
                _write_pdf(tail_writer, tail_tmp)
                _replace_file(tail_tmp, src)
            else:
                src.unlink(missing_ok=True)
        return None, n

    head_out = tmp_dir / f"{src.stem}__head_{unique_taken}.pdf"
    _write_pdf(head_writer, head_out)

    keep = set(range(total_pages)) - to_delete
    if keep:
        tail_writer = _build_tail_writer(reader, total_pages, keep)
        tail_tmp = tmp_dir / f"{src.stem}__tail_tmp.pdf"
        _write_pdf(tail_writer, tail_tmp)
        _replace_file(tail_tmp, src)
    else:
        src.unlink(missing_ok=True)

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
            size    = str(row.iloc[idx_size]).strip()
            try:
                qty = int(row.iloc[idx_qty])
            except Exception:
                continue
            if qty <= 0:
                continue

            pdf_paths = find_pdfs_by_article_size_all(article, size)
            if not pdf_paths:
                _append_shortage(shortages, article, size, qty)
                continue

            remaining = qty
            for src_pdf_path in pdf_paths:
                if remaining <= 0: break
                try:
                    part_path, shortage = await cut_first_n_pages_unique(session, src_pdf_path, remaining)
                    took_now = max(0, remaining - shortage)
                    if took_now > 0 and part_path is not None:
                        rr = PdfReader(str(part_path))
                        if len(rr.pages) > 0:
                            cut_parts.append(part_path)
                        else:
                            Path(part_path).unlink(missing_ok=True)
                    remaining -= took_now
                except Exception:
                    pass

            if remaining > 0:
                _append_shortage(shortages, article, size, remaining)

        await session.commit()

    if not cut_parts:
        report = "\n".join(shortages) if shortages else None
        return None, report

    result_path = merge_pdfs(cut_parts, output_path or (PDF_DIR / "result.pdf"))
    for p in cut_parts:
        Path(p).unlink(missing_ok=True)

    report = "\n".join(shortages) if shortages else None
    return result_path, report
