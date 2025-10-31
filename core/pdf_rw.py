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
from .patterns import *

# ==============================
# Константы / пути / регулярки
# ==============================
#
# PDF_DIR = Path("pdf-codes")
# PDF_DIR.mkdir(exist_ok=True)
#
# _RE_GTIN = re.compile(r"^0\d{13,}$")
# _RE_SERIAL = re.compile(r"^[\x20-\x7E]{4,}$")
# _RE_ASCII_PREFIX = re.compile(r"^([\x21-\x7E]{4,})")  # видимый ASCII без ведущего пробела
#
# # 1) Со скобками — всё в одной строке
# _RE_GS1_PAREN_ONELINE = re.compile(
#     r"\(\s*01\s*\)\s*\d{14}\s*\(\s*21\s*\)\s*[!-~]{4,}",
#     re.IGNORECASE
# )
#
# _RE_GS1_NOPAREN_HEADLINE = re.compile(
#     r"^\s*01\s*\d{14}\s*21\s*$",
#     re.IGNORECASE
# )
# _RE_ASCII_PREFIX_LINE = re.compile(r"^\s*([!-~]{4,})")

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

def _compile_size_regex(size_raw: str) -> re.Pattern:
    """
    Строгое совпадение размера как отдельного токена.
    - Буквенные размеры (XS, S, M, L, XL, XXL, XXXL, 2XL..5XL) — не должны иметь рядом букв/цифр.
    - Числовые и диапазоны (50, 50-52, 50/52) — требуем границы токена.
    """
    s = re.sub(r"\s+", "", str(size_raw)).upper()

    # Буквенные размеры (+ 2XL..5XL)
    if re.fullmatch(r"[2-5]?(?:XS|S|M|L|XL|XXL|XXXL)", s):
        # нет буквы/цифры слева и справа
        return re.compile(
            rf"(?:размер:\s*)?(?<![A-Z0-9]){re.escape(s)}(?![A-Z0-9])",
            re.IGNORECASE | re.MULTILINE,
        )

    # Числовые/диапазоны: разрешаем -, –, /
    token = re.escape(s).replace(r"\-", r"[–\-\/]")
    return re.compile(
        rf"(?:размер:\s*)?(?<!\w){token}(?!\w)",
        re.IGNORECASE | re.MULTILINE,
    )


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
    m = RE_ASCII_PREFIX.match(line)
    return m.group(1) if m else None

def _page_lines(pl_page) -> list[str]:
    """Достаёт строки текста со страницы pdfplumber с малой толерантностью."""
    txt = pl_page.extract_text(x_tolerance=1.0, y_tolerance=1.0) or ""
    return [ln.strip() for ln in txt.splitlines() if ln.strip()]

def _strip_all_ws(s: str) -> str:
    """Нижний регистр + удалить все пробельные символы (включая \\n, \\t)."""
    return re.sub(r"\s+", "", s).lower()

def _extract_code_from_lines(lines: Iterable[str]) -> Optional[str]:
    """
    Ищем код сразу после GTIN. Если строка склеена — берём ASCII-префикс.
    Фоллбэк: первая строка, начинающаяся с видимых ASCII.
    """
    after_gtin = False
    for ln in lines:
        if RE_GTIN.match(ln):
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


def _extract_code_from_text(text: str) -> Optional[str]:
    """
    A) '(01)<14>(21)<ASCII…>' — в одной строке → вернуть целиком (без пробелов).
    B) '01<14>21' — «голова» на строке i; сериал — в одной из ближайших последующих строк,
       ищем первый префикс печатного ASCII (пропускаем кириллицу/служебные строки).
    Иначе — кода нет.
    """
    if not text:
        return None

    # A) со скобками в одну строку
    m = RE_GS1_PAREN_ONELINE.search(text)
    if m:
        return re.sub(r"\s+", "", m.group(0))

    # B) без скобок: заголовок + сериал в ближайших строках
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    max_lookahead = 5  # сколько следующих строк просматривать в поисках ASCII-сериала
    for i, ln in enumerate(lines):
        if RE_GS1_NOPAREN_HEADLINE.match(ln):
            # попробуем из этой же строки (на всякий случай)
            m_same = re.search(r"(?:\(\s*21\s*\)|21)\s*([!-~]{4,})", ln, re.IGNORECASE)
            if m_same:
                head = re.sub(r"\s+", "", ln[:m_same.start(1)])
                tail = re.sub(r"\s+", "", m_same.group(1))
                return head + tail

            # смотрим ближайшие N строк на сериал (ASCII-префикс)
            for j in range(i + 1, min(i + 1 + max_lookahead, len(lines))):
                m_next = RE_ASCII_PREFIX_LINE.match(lines[j])
                if m_next:
                    head = re.sub(r"\s+", "", ln)               # 01<14>21
                    tail = re.sub(r"\s+", "", m_next.group(1))  # ASCII-сериал
                    return head + tail

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


# ==============================
# Поиск PDF по артикулу и размеру
# ==============================

def _normalize_for_search(s: str) -> str:
    """Убираем переводы строк/многопробел — удобно искать артикул, порванный переносами."""
    return re.sub(r"\s+", " ", s).strip().lower()


def find_pdfs_by_article_size_all(article: str, size: str) -> list[Path]:
    """
    Возвращает ВСЕ PDF из PDF_DIR, где встречаются И артикул, И размер.
    Порядок — по имени файла.
    """
    results: list[Path] = []
    if article is None or size is None:
        return results

    a_no_ws = _strip_all_ws(str(article))
    s = str(size).strip()
    if not a_no_ws or not s:
        return results

    size_regex = _compile_size_regex(s)

    for pdf_file in PDF_DIR.glob("*.pdf"):
        try:
            raw_text = read_pdf(pdf_file)
        except Exception as e:
            print(f"⚠️ Ошибка при чтении {pdf_file}: {e}")
            continue

        # 🔧 нормализуем длинные тире — помогает при нестабильной верстке
        raw_text_norm = raw_text.replace("–", "-").replace("—", "-")

        # 1) Артикул ищем по «сплющенному» тексту (устойчиво к переносам)
        text_no_ws = _strip_all_ws(raw_text)
        if a_no_ws not in text_no_ws:
            continue

        # 2) Размер ищем ТОЛЬКО по исходному (но нормализованному) тексту,
        #    чтобы не ломать границы токенов
        if size_regex.search(raw_text_norm):
            results.append(pdf_file)

    results.sort(key=lambda p: p.name.lower())
    return results


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
    """Код со страницы по её индексу (учитывает GS1-пару и fallback)."""
    txt = pl_pdf.pages[page_index].extract_text(x_tolerance=1.0, y_tolerance=1.0) or ""
    return _extract_code_from_text(txt)

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
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "wb") as f:
        writer.write(f)
    return out


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

            # Ищем ВСЕ файлы с таким артикулом/размером
            pdf_paths = find_pdfs_by_article_size_all(article, size)
            if not pdf_paths:
                _append_shortage(shortages, article, size, qty)
                continue

            remaining = qty
            took_total = 0

            for src_pdf_path in pdf_paths:
                if remaining <= 0:
                    break

                try:
                    part_path, shortage = await cut_first_n_pages_unique(session, src_pdf_path, remaining)
                    # cut_first_n_pages_unique возвращает shortage >= 0 относительно запрошенных remaining
                    took_now = max(0, remaining - shortage)

                    if took_now > 0 and part_path is not None:
                        # убедимся, что не пустой
                        rr = PdfReader(str(part_path))
                        if len(rr.pages) > 0:
                            cut_parts.append(part_path)
                        else:
                            try:
                                Path(part_path).unlink(missing_ok=True)
                            except Exception:
                                pass

                    took_total += took_now
                    remaining -= took_now

                except Exception:
                    # любая ошибка при резке из этого файла — считаем как будто из него 0
                    # и пробуем следующий файл
                    pass

            if remaining > 0:
                # не хватило страниц даже после перебора всех файлов
                _append_shortage(shortages, article, size, remaining)

        # фиксируем зарегистрированные коды один раз
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
