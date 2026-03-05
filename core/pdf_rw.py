import os
import time
from dataclasses import dataclass
from typing import Optional, Tuple

import pdfplumber
from PyPDF2 import PdfReader, PdfWriter
from sqlalchemy.ext.asyncio import AsyncSession

from config import config
from services.printed_codes import register_code_if_new, bulk_register_codes, get_all_codes
from .patterns import *
import asyncio

from .pdf_splitter import _safe_name

# глобальная "бронь" кодов на время одной сборки
_codes_lock = asyncio.Lock()

# кеш текста PDF для поиска по артикулу/размеру
_pdf_text_cache: dict[Path, str] = {}

# локи на каждый pdf, т.к. мы модифицируем исходник (удаляем страницы)
_pdf_locks: dict[Path, asyncio.Lock] = {}
_pdf_locks_lock = asyncio.Lock()


@dataclass(frozen=True)
class CutResult:
    head_path: Optional[Path]
    shortage: int


# 🔧 helpers (оффлоад синхронщины в поток)

def get_pdf_text_cached(p: Path) -> str:
    """NEW: кешируем read_pdf для ускорения поиска PDF по артикулу/размеру."""
    t = _pdf_text_cache.get(p)
    if t is None:
        t = read_pdf(p)
        _pdf_text_cache[p] = t
    return t

def invalidate_pdf_cache(p: Path) -> None:
    """NEW: сбрасываем кеш, если PDF был изменён (мы его урезали)."""
    _pdf_text_cache.pop(p, None)


async def get_pdf_lock(p: Path) -> asyncio.Lock:
    """NEW: гарантируем один lock на путь."""
    async with _pdf_locks_lock:
        lock = _pdf_locks.get(p)
        if lock is None:
            lock = asyncio.Lock()
            _pdf_locks[p] = lock
        return lock


def _norm_size_for_fname(size: str) -> str:
    s = str(size).strip()
    s = s.replace("–", "-").replace("—", "-")
    s = re.sub(r"\s+", "", s)
    return s


async def claim_code(code: str, used_codes: set[str], staged_codes_global: set[str]) -> bool:
    """
    NEW: атомарно (с lock) проверяем, что код ещё не выдавался
    (ни в БД, ни в текущей сборке), и "бронируем" его.
    """
    async with _codes_lock:
        if code in used_codes or code in staged_codes_global:
            return False
        staged_codes_global.add(code)
        return True




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
    Ищем GS1: (01)<14 цифр>(21)<ASCII-serial> (со/без скобок).
    Серийник может идти сразу после (21) или на следующих строках — не обязательно с начала.
    После сборки валидируем общую длину 27..31.
    """
    if not text:
        return None

    # 0) Всё в одной строке со скобками
    m_one = RE_GS1_PAREN_ONELINE.search(text)
    if m_one:
        candidate = re.sub(r"\s+", "", m_one.group(0))
        return candidate if 27 <= len(candidate) <= 35 else None

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return None

    def pack(head: str, tail: str) -> Optional[str]:
        s = re.sub(r"\s+", "", head) + re.sub(r"\s+", "", tail)
        return s if 27 <= len(s) <= 31 else None

    LOOKAHEAD = 4  # сколько строк дальше смотрим

    # 1) Со скобками: "(01)…..(21)" в строке i, серийник на этой же или следующих строках (в любом месте)
    head_pat = re.compile(r"\(\s*01\s*\)\s*\d{14}\s*\(\s*21\s*\)")  # только "голову"
    for i, ln in enumerate(lines):
        mh = head_pat.search(ln)
        if not mh:
            continue
        head = ln[:mh.end()]
        tail_same = ln[mh.end():]

        # серийник может быть где угодно в хвосте строки (НЕ только с начала)
        m_same = RE_ASCII_ANY.search(tail_same)
        if m_same:
            cand = pack(head, m_same.group(0))
            if cand:
                return cand

        # либо на одной из следующих строк — тоже не обязательно с начала
        for j in range(i + 1, min(i + 1 + LOOKAHEAD, len(lines))):
            m_next = RE_ASCII_ANY.search(lines[j])
            if m_next:
                cand = pack(head, m_next.group(0))
                if cand:
                    return cand

        # попробуем «склеить» хвост и пару следующих строк на случай разрывов
        glued = tail_same + " " + " ".join(lines[i + 1:min(i + 1 + LOOKAHEAD, len(lines))])
        m_glued = RE_ASCII_ANY.search(glued)
        if m_glued:
            cand = pack(head, m_glued.group(0))
            if cand:
                return cand

    # 2) Без скобок: «01\d{14}21» как подстрока, серийник далее (где угодно)
    noparen_head_any = re.compile(r"01\s*\d{14}\s*21")
    for i, ln in enumerate(lines):
        mh = noparen_head_any.search(ln)
        if not mh:
            continue
        head = ln[mh.start():mh.end()]
        tail_same = ln[mh.end():]

        m_same = RE_ASCII_ANY.search(tail_same)
        if m_same:
            cand = pack(head, m_same.group(0))
            if cand:
                return cand

        for j in range(i + 1, min(i + 1 + LOOKAHEAD, len(lines))):
            m_next = RE_ASCII_ANY.search(lines[j])
            if m_next:
                cand = pack(head, m_next.group(0))
                if cand:
                    return cand

        glued = tail_same + " " + " ".join(lines[i + 1:min(i + 1 + LOOKAHEAD, len(lines))])
        m_glued = RE_ASCII_ANY.search(glued)
        if m_glued:
            cand = pack(head, m_glued.group(0))
            if cand:
                return cand

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
    """
    1) FAST: поиск по имени (в OUT_DIR и PDF_DIR)
    2) если article в df обрезан — ищем по префиксу (до '/')
    3) размер-число/диапазон — матчим как префикс (158*, 140-146*, 50*)
    4) FALLBACK: старый медленный поиск по тексту
    """
    if not article or not size:
        return []

    # где лежат нарезанные файлы
    search_dirs = []
    search_dirs.append(PDF_DIR)       # на всякий случай

    size_raw = _norm_size_for_fname(size)
    size_s = _safe_name(size_raw)

    # full art (с цветом/хвостом)
    art_full_s = _safe_name(str(article))
    # prefix art (до '/'), чтобы матчить случаи "темно" vs "темно-синий"
    art_prefix = str(article).split("/", 1)[0].strip()
    art_prefix_s = _safe_name(art_prefix)

    def _glob_all(pattern: str) -> list[Path]:
        acc: list[Path] = []
        for d in search_dirs:
            try:
                acc.extend(d.glob(pattern))
            except Exception:
                continue
        acc.sort(key=lambda p: p.name.lower())
        return acc

    # если размер начинается с цифры (50, 158, 140-146, 152-158 и т.д.) — разрешаем хвост типа "_РОСТ"
    size_prefixable = bool(re.match(r"^\d", size_raw))

    # ---------- FAST 1: полное совпадение по статье
    article_base, color = article.split("/", 1) if "/" in article else (article, "")
    article_base_s = _safe_name(article_base)
    color_s = _safe_name(color)

    if size_prefixable:
        pattern1 = f"{article_base_s}-{color_s}__{size_s}*__*.pdf"
    else:
        pattern1 = f"{article_base_s}-{color_s}__{size_s}__*.pdf"

    res = _glob_all(pattern1)
    if res:
        return res

    # ---------- FAST 2: только префикс артикула (без цвета)
    # if size_prefixable:
    #     pattern2 = f"{art_prefix_s}*__{size_s}*__*.pdf"
    # else:
    #     pattern2 = f"{art_prefix_s}*__{size_s}__*.pdf"
    #
    # res = _glob_all(pattern2)
    # if res:
    #     return res

    # ---------- FALLBACK (медленный поиск по тексту)
    results: list[Path] = []

    art_prefix, color = str(article).split("/", 1) if "/" in str(article) else (str(article), "")
    color = color.lower()

    a_no_ws = _strip_all_ws(art_prefix)
    size_regex = _compile_size_token(size)

    all_pdfs: list[Path] = []
    for d in search_dirs:
        try:
            all_pdfs.extend(d.glob("*.pdf"))
        except Exception:
            pass

    for i, pdf_file in enumerate(all_pdfs):
        if not pdf_file.exists():
            print(f"[File doesn`t exists {pdf_file} - skipped]")
            continue
        # print(f"[Check file {i} of {len(all_pdfs)}]")
        try:
            raw_text = get_pdf_text_cached(pdf_file)
        except Exception:
            continue

        raw_text_norm = raw_text.replace("–", "-").replace("—", "-")

        if a_no_ws not in _strip_all_ws(raw_text):
            continue

        if color and color not in raw_text.lower():
            continue

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

async def cut_first_n_pages_unique_checkonly(
    src_pdf: Path | str,
    n: int,
    used_codes: set[str],
    staged_codes_global: set[str],  # CHANGED: теперь глобальный staged
) -> Tuple[Optional[Path], int, list[str]]:
    """
    CHANGED:
    - НЕ читаем все тексты страниц сразу
    - идём по страницам до набора n
    - уникальность через claim_code(... staged_codes_global ...)
    - после модификации src инвалидируем кеш текста (важно для find)
    """
    src = Path(src_pdf)
    if n <= 0:
        return None, 0, []

    tmp_dir = src.parent / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    try:
        reader = await _to_thread(PdfReader, str(src))
    except Exception:
        return None, n, []

    total_pages = len(reader.pages)
    to_delete: set[int] = set()
    head_writer = PdfWriter()
    unique_taken = 0
    picked_codes: list[str] = []

    # CHANGED: открываем pdfplumber один раз и идём по страницам
    try:
        with pdfplumber.open(str(src)) as pl:
            for i in range(total_pages):
                if unique_taken >= n:
                    break

                txt = pl.pages[i].extract_text(x_tolerance=1.0, y_tolerance=1.0) or ""
                code = _extract_code_from_text(txt)
                if not code:
                    continue

                ok = await claim_code(code, used_codes, staged_codes_global)
                if not ok:
                    # код уже был выдан ранее (или уже взят другой строкой)
                    to_delete.add(i)
                    continue

                picked_codes.append(code)
                head_writer.add_page(reader.pages[i])
                to_delete.add(i)
                unique_taken += 1
    except Exception:
        return None, n, []

    # если ничего не взяли — но могли удалить дубли
    if unique_taken == 0:
        if to_delete:
            keep = set(range(total_pages)) - to_delete
            tail_writer = _build_tail_writer(reader, total_pages, keep)
            if len(tail_writer.pages) > 0:
                tail_tmp = tmp_dir / f"{src.stem}__tail_tmp.pdf"
                await _to_thread(_write_pdf, tail_writer, tail_tmp)
                await _to_thread(_replace_file, tail_tmp, src)
                invalidate_pdf_cache(src)  # NEW
            else:
                try:
                    await _to_thread(src.unlink, True)
                except Exception:
                    pass
                invalidate_pdf_cache(src)  # NEW
        return None, n, []

    head_out = tmp_dir / f"{src.stem}__head_{unique_taken}.pdf"
    await _to_thread(_write_pdf, head_writer, head_out)

    keep = set(range(total_pages)) - to_delete
    if keep:
        tail_writer = _build_tail_writer(reader, total_pages, keep)
        tail_tmp = tmp_dir / f"{src.stem}__tail_tmp.pdf"
        await _to_thread(_write_pdf, tail_writer, tail_tmp)
        await _to_thread(_replace_file, tail_tmp, src)
        invalidate_pdf_cache(src)  # NEW
    else:
        try:
            await _to_thread(src.unlink, True)
        except Exception:
            pass
        invalidate_pdf_cache(src)  # NEW

    return head_out, max(0, n - unique_taken), picked_codes


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



async def _process_order_row(
    row_no: int,
    row,
    idx_article: int,
    idx_size: int,
    idx_qty: int,
    used_codes: set[str],
    staged_codes_global: set[str],  # CHANGED
) -> tuple[int, list[Path], set[str], list[str]]:
    """
    CHANGED:
    - staged_local больше не нужен для уникальности
    - возвращаем staged_local только для совместимости результата (можно пустой)
    - режем каждый src_pdf_path под lock, чтобы не было гонок
    """
    parts: list[Path] = []
    staged_local: set[str] = set()     # оставим пустым/для совместимости
    shortages_local: list[str] = []

    article = str(row.iloc[idx_article]).strip()
    size    = str(row.iloc[idx_size]).strip()

    try:
        qty = int(row.iloc[idx_qty])
    except Exception:
        return row_no, parts, staged_local, shortages_local

    if qty <= 0:
        return row_no, parts, staged_local, shortages_local

    try:
        pdf_paths = await _to_thread(find_pdfs_by_article_size_all, article, size)
    except Exception:
        pdf_paths = []

    print(f"[row {row_no}] {article=} {size=} qty={qty} found_pdfs={len(pdf_paths)}")

    if not pdf_paths:
        _append_shortage(shortages_local, article, size, qty)
        return row_no, parts, staged_local, shortages_local

    remaining = qty

    for src_pdf_path in pdf_paths:
        if remaining <= 0:
            break

        src_pdf_path = Path(src_pdf_path)
        lock = await get_pdf_lock(src_pdf_path)  # NEW

        async with lock:
            print(f"Check {src_pdf_path}")

            part_path, shortage, picked_codes = await cut_first_n_pages_unique_checkonly(
                src_pdf_path,
                remaining,
                used_codes,
                staged_codes_global,  # CHANGED
            )

        took_now = max(0, remaining - shortage)

        if took_now > 0 and part_path is not None:
            try:
                rr = await _to_thread(PdfReader, str(part_path))
                if len(rr.pages) > 0:
                    parts.append(Path(part_path))
                else:
                    try:
                        await _to_thread(Path(part_path).unlink, True)
                    except Exception:
                        pass
            except Exception:
                pass

        remaining -= took_now

        # staged_local можно пополнить для дебага/совместимости
        if picked_codes:
            staged_local.update(picked_codes)

    if remaining > 0:
        _append_shortage(shortages_local, article, size, remaining)

    print(f"[row {row_no}] parts={len(parts)} remaining_short={remaining}")

    return row_no, parts, staged_local, shortages_local


async def build_pdf_from_dataframe(
    df,
    output_path: Path | str | None = None,
) -> tuple[Optional[Path], Optional[str]]:
    idx_article, idx_size, idx_qty = _normalize_columns(df)

    PARALLELISM = 8
    sem = asyncio.Semaphore(PARALLELISM)

    rows = list(df.iterrows())
    total = len(rows)

    done = 0
    inflight = 0
    lock = asyncio.Lock()
    t0 = time.time()

    def _fmt_eta(done_: int) -> str:
        if done_ <= 0:
            return "ETA: ?"
        elapsed = time.time() - t0
        per = elapsed / done_
        eta = per * (total - done_)
        return f"ETA: {int(eta)}s"

    # NEW: глобальные коды на время сборки
    staged_codes_global: set[str] = set()

    async with config.AsyncSessionLocal() as session:
        async with session.begin():
            used_codes = await get_all_codes(session)

            async def _run_one(row_no: int, row_obj):
                nonlocal inflight, done

                async with sem:
                    async with lock:
                        inflight += 1
                        print(f"[{row_no+1}/{total}] START  inflight={inflight}")

                    try:
                        return await _process_order_row(
                            row_no=row_no,
                            row=row_obj,
                            idx_article=idx_article,
                            idx_size=idx_size,
                            idx_qty=idx_qty,
                            used_codes=used_codes,
                            staged_codes_global=staged_codes_global,  # CHANGED
                        )
                    finally:
                        async with lock:
                            inflight -= 1
                            done += 1
                            print(f"[{done}/{total}] DONE   inflight={inflight}  {_fmt_eta(done)}")

            tasks = [_run_one(i, row) for i, (_, row) in enumerate(rows)]
            results = await asyncio.gather(*tasks)
            results.sort(key=lambda x: x[0])

            cut_parts: list[Path] = []
            shortages: list[str] = []

            for _, parts, _staged_local, shortages_local in results:
                if parts:
                    cut_parts.extend(parts)
                if shortages_local:
                    shortages.extend(shortages_local)

            if not cut_parts:
                return None, ("\n".join(shortages) if shortages else None)

            try:
                result_path = await _to_thread(
                    merge_pdfs,
                    cut_parts,
                    output_path or (PDF_DIR / "result.pdf"),
                )
            except Exception:
                return None, ("\n".join(shortages) if shortages else None)

            # CHANGED: пишем в БД все реально "забронированные" коды
            try:
                await bulk_register_codes(session, staged_codes_global)
            except Exception as e:
                try:
                    Path(result_path).unlink(missing_ok=True)
                except Exception:
                    pass
                raise e

    for p in cut_parts:
        try:
            await _to_thread(Path(p).unlink, True)
        except Exception:
            pass

    report = "\n".join(shortages) if shortages else None
    return Path(result_path), report
