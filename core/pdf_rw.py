import os

import re
from pathlib import Path
from typing import Tuple, Optional

import pdfplumber
from PyPDF2 import PdfReader, PdfWriter
from sqlalchemy.ext.asyncio import AsyncSession

from config import config


from services.printed_codes import register_code_if_new

PDF_DIR = Path("pdf-codes")
PDF_DIR.mkdir(exist_ok=True)
_RE_GTIN = re.compile(r"^0\d{13,}$")
_RE_SERIAL = re.compile(r"^[\x20-\x7E]{4,}$")

def read_pdf(file_path: str | Path) -> str:
    """
    Считывает весь текст из PDF файла с помощью pdfplumber.
    :param file_path: путь до pdf файла
    :return: текст всех страниц одной строкой
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Файл {path} не найден")

    text_parts = []
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


def find_pdf_by_article_size(article: str, size: str) -> str | None:
    """
    Ищет PDF, где встречаются И артикул, И размер (оба как подстроки).
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


def _extract_code_from_lines(lines: list[str]) -> str | None:
    """Ищем код сразу после GTIN. Если склейка, отрезаем по первому не-ASCII."""
    after_gtin = False
    for ln in lines:
        if _RE_GTIN.match(ln):
            after_gtin = True
            continue
        if after_gtin:
            # берём ведущую подпоследовательность печатных ASCII
            m = re.match(r"^([\x21-\x7E]{4,})", ln)  # !..~, без пробела в начале
            if m:
                return m.group(1)

    # fallback: первая строка, начинающаяся с печатных ASCII (если GTIN не нашли)
    for ln in lines:
        m = re.match(r"^([\x21-\x7E]{4,})", ln)
        if m:
            return m.group(1)
    return None

def _extract_page_code_pdfplumber(pl_page) -> str | None:
    # маленькие толерансы, чтобы строки не склеивались
    txt = pl_page.extract_text(x_tolerance=1.0, y_tolerance=1.0) or ""
    lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
    if not lines:
        return None
    return _extract_code_from_lines(lines)

async def cut_first_n_pages_unique(
    session: AsyncSession,
    src_pdf: Path | str,
    n: int,
) -> Tuple[Optional[Path], int]:
    src = Path(src_pdf)
    if not src.exists():
        raise FileNotFoundError(f"Файл не найден: {src}")
    if n <= 0:
        return None, 0

    tmp_dir = (src.parent / "tmp")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    reader = PdfReader(str(src))
    total = len(reader.pages)

    delete_indexes: set[int] = set()
    head_writer = PdfWriter()
    unique_taken = 0

    # открываем pdfplumber один раз и читаем коды по страницам
    with pdfplumber.open(str(src)) as pl_pdf:
        for i in range(total):
            if unique_taken >= n:
                break

            pl_page = pl_pdf.pages[i]
            code = _extract_page_code_pdfplumber(pl_page)
            if not code:
                # нет кода — не трогаем страницу
                continue

            is_new = await register_code_if_new(session, code)
            if is_new:
                head_writer.add_page(reader.pages[i])
                delete_indexes.add(i)
                unique_taken += 1
            else:
                delete_indexes.add(i)

    # если не взяли ни одной уникальной — но могли удалить дубли
    if unique_taken == 0:
        if delete_indexes:
            tail_writer = PdfWriter()
            for i in range(total):
                if i not in delete_indexes:
                    tail_writer.add_page(reader.pages[i])
            if len(tail_writer.pages) > 0:
                tail_tmp = tmp_dir / f"{src.stem}__tail_tmp.pdf"
                with open(tail_tmp, "wb") as f:
                    tail_writer.write(f)
                os.replace(tail_tmp, src)
            else:
                try: src.unlink()
                except FileNotFoundError: pass
        shortage = max(0, n - unique_taken)
        return None, shortage

    # пишем head
    head_out = tmp_dir / f"{src.stem}__head_{unique_taken}.pdf"
    with open(head_out, "wb") as f:
        head_writer.write(f)

    # пересобираем исходник без удалённых страниц
    remain = total - len(delete_indexes)
    if remain > 0:
        tail_writer = PdfWriter()
        for i in range(total):
            if i not in delete_indexes:
                tail_writer.add_page(reader.pages[i])
        tail_tmp = tmp_dir / f"{src.stem}__tail_tmp.pdf"
        with open(tail_tmp, "wb") as f:
            tail_writer.write(f)
        os.replace(tail_tmp, src)
    else:
        try: src.unlink()
        except FileNotFoundError: pass

    shortage = max(0, n - unique_taken)
    return head_out, shortage


def merge_pdfs(pdf_paths: list[Path | str], output_path: Path | str) -> Path:
    """
    Склеивает список PDF в один файл output_path.
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

async def build_pdf_from_dataframe(df, output_path: Path | str | None = None) -> tuple[Path | None, str | None]:
    """
    Проходит по df ('артикул','размер','количество'):
      - ищет PDF по (артикул+размер),
      - вырезает первые 'количество' страниц, но только с НОВЫМИ кодами (consume),
      - копит фрагменты для склейки,
      - собирает общий отчёт о нехватках страниц (в т.ч. если PDF не найден).
    Возвращает (путь к итоговому PDF или None, текст отчёта или None).
    """
    required = {"артикул", "размер", "количество"}
    cols_norm = [str(c).strip().lower() for c in df.columns]
    colset = set(cols_norm)
    if not required.issubset(colset):
        missing = required - colset
        raise ValueError(f"В df нет обязательных колонок: {', '.join(missing)}")

    idx_article = cols_norm.index("артикул")
    idx_size = cols_norm.index("размер")
    idx_qty = cols_norm.index("количество")

    cut_parts: list[Path] = []
    shortages: list[str] = []

    # одна сессия на всю сборку
    async with config.AsyncSessionLocal() as session:
        for _, row in df.iterrows():
            article = str(row.iloc[idx_article]).strip()
            size = str(row.iloc[idx_size]).strip()

            try:
                qty = int(row.iloc[idx_qty])
            except Exception:
                # некорректное число — пропуск строки
                continue

            if qty <= 0:
                continue

            pdf_name = find_pdf_by_article_size(article, size)
            if not pdf_name:
                # нет подходящего PDF — полная нехватка
                shortages.append(f"{article} - размер: {size}, не хватило: {qty}")
                continue

            src_pdf_path = PDF_DIR / pdf_name
            try:
                # режем с учётом уникальных кодов
                part_path, shortage = await cut_first_n_pages_unique(session, src_pdf_path, qty)
                if shortage > 0:
                    shortages.append(f"{article} - размер: {size}, не хватило: {shortage}")

                if part_path is not None:
                    rr = PdfReader(str(part_path))
                    if len(rr.pages) > 0:
                        cut_parts.append(part_path)
            except Exception as e:
                # любая ошибка при резке — считаем полной нехваткой
                shortages.append(f"{article} - размер: {size}, не хватило: {qty}")

        # фиксируем зарегистрированные коды
        await session.commit()

    if not cut_parts:
        report = "\n".join(shortages) if shortages else None
        return None, report

    if output_path is None:
        output_path = PDF_DIR / "result.pdf"

    result_path = merge_pdfs(cut_parts, output_path)

    # очистка временных кусков
    for p in cut_parts:
        try:
            Path(p).unlink(missing_ok=True)
        except Exception:
            pass

    report = "\n".join(shortages) if shortages else None
    return result_path, report