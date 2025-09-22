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


# ==============================
# Константы / пути / регулярки
# ==============================

PDF_DIR = Path("pdf-codes")
PDF_DIR.mkdir(exist_ok=True)

_RE_GTIN = re.compile(r"^0\d{13,}$")
_RE_SERIAL = re.compile(r"^[\x20-\x7E]{4,}$")
_RE_ASCII_PREFIX = re.compile(r"^([\x21-\x7E]{4,})")  # видимый ASCII без ведущего пробела


# ==============================
# Типы данных
# ==============================

@dataclass(frozen=True)
class CutResult:
    """Результат «вырезания» страниц из PDF."""
    head_path: Optional[Path]  # путь к файлу с вырезанной «шапкой» (None, если не вырезали)
    shortage: int              # нехватка страниц (>= 0)


# ==============================
# Небольшие утилиты
# ==============================

def _assert_exists(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Файл {path} не найден")

def _write_pdf(writer: PdfWriter, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "wb") as f:
        writer.write(f)

def _replace_file(tmp_path: Path, target: Path) -> None:
    """Безопасная замена файла на диске."""
    os.replace(tmp_path, target)

def _ascii_prefix(line: str) -> Optional[str]:
    """Возвращает ведущую подпоследовательность видимых ASCII-символов (если длина >= 4)."""
    m = _RE_ASCII_PREFIX.match(line)
    return m.group(1) if m else None

def _page_lines(pl_page) -> list[str]:
    """Достаёт строки текста со страницы pdfplumber с малой толерантностью."""
    txt = pl_page.extract_text(x_tolerance=1.0, y_tolerance=1.0) or ""
    return [ln.strip() for ln in txt.splitlines() if ln.strip()]

def _extract_code_from_lines(lines: Iterable[str]) -> Optional[str]:
    """
    Ищем код сразу после GTIN. Если строка склеена — берём ASCII-префикс.
    Фоллбэк: первая строка, начинающаяся с видимых ASCII.
    """
    after_gtin = False
    for ln in lines:
        if _RE_GTIN.match(ln):
            after_gtin = True
            continue
        if after_gtin:
            prefix = _ascii_prefix(ln)
            if prefix:
                return prefix

    for ln in lines:
        prefix = _ascii_prefix(ln)
        if prefix:
            return prefix
    return None


# ==============================
# Работа с PDF-контентом
# ==============================

def read_pdf(file_path: str | Path) -> str:
    """
    Считывает весь текст из PDF файла с помощью pdfplumber.
    :return: текст всех страниц одной строкой
    """
    path = Path(file_path)
    _assert_exists(path)

    text_parts: list[str] = []
    with pdfplumber.open(str(path)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text.strip())
    return "\n".join(text_parts)

async def save_pdf_file(data: bytes, filename: str, user_id: int) -> Path:
    """
    Сохраняет PDF в директорию pdf-codes.
    Имя файла = <user_id>_<filename>.
    """
    save_path = PDF_DIR / f"{user_id}_{filename}"
    with open(save_path, "wb") as f:
        f.write(data)
    return save_path

def find_pdf_by_article_size(article: str, size: str) -> Optional[str]:
    """
    Ищет PDF, где встречаются И артикул, И «Размер: <size>».
    Возвращает имя файла (str) или None.
    """
    a = str(article).strip()
    s = str(size).strip()
    if not a or not s:
        return None

    for pdf_file in PDF_DIR.glob("*.pdf"):
        try:
            text = read_pdf(pdf_file)
        except Exception as e:
            print(f"⚠️ Ошибка при чтении {pdf_file}: {e}")
            continue

        if a in text and f"Размер: {s}" in text:
            return pdf_file.name
    return None

def merge_pdfs(pdf_paths: list[Path | str], output_path: Path | str) -> Path:
    """
    Склеивает список PDF в один файл output_path.
    Пропускает отсутствующие файлы.
    """
    writer = PdfWriter()
    for p in pdf_paths:
        pth = Path(p)
        if not pth.exists():
            print(f"⚠️ Пропускаю отсутствующий файл при склейке: {pth}")
            continue
        reader = PdfReader(str(pth))
        for page in reader.pages:
            writer.add_page(page)

    out = Path(output_path)
    _write_pdf(writer, out)
    return out


# ==============================
# Логика вырезания уникальных страниц
# ==============================

def _build_tail_writer(reader: PdfReader, total: int, keep_indexes: set[int]) -> PdfWriter:
    """Создаёт writer из страниц с индексами, которые нужно оставить."""
    tail_writer = PdfWriter()
    for i in range(total):
        if i in keep_indexes:
            tail_writer.add_page(reader.pages[i])
    return tail_writer

def _extract_page_code(pl_pdf, page_index: int) -> Optional[str]:
    """Код со страницы по её индексу."""
    lines = _page_lines(pl_pdf.pages[page_index])
    if not lines:
        return None
    return _extract_code_from_lines(lines)

async def cut_first_n_pages_unique(
    session: AsyncSession,
    src_pdf: Path | str,
    n: int,
) -> Tuple[Optional[Path], int]:
    """
    Вырезает первые n страниц, содержащие НОВЫЕ коды (через register_code_if_new).
    Дубликаты из начала удаляются из исходника. Возвращает (путь к шапке, нехватка).
    """
    src = Path(src_pdf)
    _assert_exists(src)
    if n <= 0:
        return None, 0

    tmp_dir = src.parent / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    reader = PdfReader(str(src))
    total_pages = len(reader.pages)

    to_delete: set[int] = set()
    head_writer = PdfWriter()
    unique_taken = 0

    # Открываем pdfplumber один раз — читаем коды постранично
    with pdfplumber.open(str(src)) as pl_pdf:
        for i in range(total_pages):
            if unique_taken >= n:
                break

            code = _extract_page_code(pl_pdf, i)
            if not code:
                # нет кода — страницу не трогаем
                continue

            is_new = await register_code_if_new(session, code)
            if is_new:
                head_writer.add_page(reader.pages[i])
                to_delete.add(i)
                unique_taken += 1
            else:
                # дубликат кода — тоже удаляем из исходника
                to_delete.add(i)

    # Если не взяли ни одной уникальной — могли лишь удалить дубли
    if unique_taken == 0:
        if to_delete:
            keep = set(range(total_pages)) - to_delete
            tail_writer = _build_tail_writer(reader, total_pages, keep)
            if len(tail_writer.pages) > 0:
                tail_tmp = tmp_dir / f"{src.stem}__tail_tmp.pdf"
                _write_pdf(tail_writer, tail_tmp)
                _replace_file(tail_tmp, src)
            else:
                try:
                    src.unlink()
                except FileNotFoundError:
                    pass
        return None, max(0, n - unique_taken)

    # Пишем шапку (вырезанные уникальные)
    head_out = tmp_dir / f"{src.stem}__head_{unique_taken}.pdf"
    _write_pdf(head_writer, head_out)

    # Пересобираем исходник без удалённых
    keep = set(range(total_pages)) - to_delete
    if keep:
        tail_writer = _build_tail_writer(reader, total_pages, keep)
        tail_tmp = tmp_dir / f"{src.stem}__tail_tmp.pdf"
        _write_pdf(tail_writer, tail_tmp)
        _replace_file(tail_tmp, src)
    else:
        try:
            src.unlink()
        except FileNotFoundError:
            pass

    return head_out, max(0, n - unique_taken)


# ==============================
# Построение PDF по датафрейму
# ==============================

def _normalize_columns(df) -> tuple[int, int, int]:
    """
    Валидирует и нормализует названия колонок.
    Требуются: 'артикул','размер','количество'.
    Возвращает индексы этих колонок.
    """
    required = {"артикул", "размер", "количество"}
    cols_norm = [str(c).strip().lower() for c in df.columns]
    colset = set(cols_norm)
    if not required.issubset(colset):
        missing = required - colset
        raise ValueError(f"В df нет обязательных колонок: {', '.join(sorted(missing))}")

    return (
        cols_norm.index("артикул"),
        cols_norm.index("размер"),
        cols_norm.index("количество"),
    )

def _append_shortage(shortages: list[str], article: str, size: str, amount: int) -> None:
    shortages.append(f"{article} - размер: {size}, не хватило: {amount}")

async def build_pdf_from_dataframe(df, output_path: Path | str | None = None) -> tuple[Optional[Path], Optional[str]]:
    """
    Проходит по df ('артикул','размер','количество'):
      - ищет PDF по (артикул+размер),
      - вырезает первые 'количество' страниц, но только с НОВЫМИ кодами (consume),
      - копит фрагменты для склейки,
      - собирает общий отчёт о нехватках страниц (в т.ч. если PDF не найден).
    Возвращает (путь к итоговому PDF или None, текст отчёта или None).
    """
    idx_article, idx_size, idx_qty = _normalize_columns(df)

    cut_parts: list[Path] = []
    shortages: list[str] = []

    # одна сессия на всю сборку
    async with config.AsyncSessionLocal() as session:
        for _, row in df.iterrows():
            article = str(row.iloc[idx_article]).strip()
            size = str(row.iloc[idx_size]).strip()

            # безопасное приведение к int
            try:
                qty = int(row.iloc[idx_qty])
            except Exception:
                continue
            if qty <= 0:
                continue

            pdf_name = find_pdf_by_article_size(article, size)
            if not pdf_name:
                _append_shortage(shortages, article, size, qty)
                continue

            src_pdf_path = PDF_DIR / pdf_name
            try:
                part_path, shortage = await cut_first_n_pages_unique(session, src_pdf_path, qty)
                if shortage > 0:
                    _append_shortage(shortages, article, size, shortage)

                if part_path is not None:
                    rr = PdfReader(str(part_path))
                    if len(rr.pages) > 0:
                        cut_parts.append(part_path)
            except Exception:
                # любая ошибка при резке — считаем полной нехваткой
                _append_shortage(shortages, article, size, qty)

        # фиксируем зарегистрированные коды
        await session.commit()

    if not cut_parts:
        report = "\n".join(shortages) if shortages else None
        return None, report

    result_path = merge_pdfs(cut_parts, output_path or (PDF_DIR / "result.pdf"))

    # очистка временных кусков
    for p in cut_parts:
        try:
            Path(p).unlink(missing_ok=True)
        except Exception:
            pass

    report = "\n".join(shortages) if shortages else None
    return result_path, report
