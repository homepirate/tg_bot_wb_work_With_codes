# services/pdf_inventory_report.py
from __future__ import annotations

from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple, List
import io
import re

import pandas as pd
import pdfplumber
from PyPDF2 import PdfReader

from .pdf_rw import PDF_DIR


# ===== Регулярки (поддержка обоих вариантов верстки) =====
# Артикул: режем до "Цвет" (если склеено), иначе до конца строки.
_RE_ART = re.compile(r"Артикул\s+(.+?)(?:\s*Цвет\b|$)", re.IGNORECASE)
# режем по "Цвет" даже если он прилип к слову (без \b)
_RE_ART_ALT1 = re.compile(r"арт\.\s*([A-Z0-9_]+/\S+)", re.IGNORECASE)
# Общий токен "XXX/yyy" (лат/цифры/подчёркивания до '/', затем кир/лат/цифры/дефисы/подчёрки)
_RE_ART_ALT2 = re.compile(r"\b([A-Z0-9_]+/[A-Za-zА-Яа-я0-9_\-]+)\b", re.IGNORECASE)

_RE_COLOR = re.compile(r"Цвет:\s*([^\r\n]+)", re.IGNORECASE)
_RE_NAME_COLOR = re.compile(r"Балаклава\s+(.+?)\s+р\.", re.IGNORECASE | re.DOTALL)
_RE_COLOR_TOKEN = re.compile(r"Цвет", re.IGNORECASE)

# Числовые размеры: 56-60, 56–60, 56/58, одиночное 56
_RE_SIZE_NUMERIC = re.compile(r"\b\d{2}(?:[–\-\/]\d{2})?\b")
# Буквенные размеры и пары: XS, L/XL, S-M, 3XL и т.п.
_RE_SIZE_ALPHA = re.compile(
    r"\b(?:(?:[2-5]?XS)|(?:[2-5]?S)|(?:[2-5]?M)|(?:[2-5]?L)|(?:[2-5]?XL)|(?:[2-5]?XXL)|(?:[2-5]?XXXL))(?:[\/\-–](?:[2-5]?(?:XS|S|M|L|XL|XXL|XXXL)))?\b",
    re.IGNORECASE,
)
_SIZE_WORDS = {
    "ONE SIZE", "ONESIZE", "UNI", "UNISIZE", "UNIVERSAL",
    "УНИВЕРСАЛЬНЫЙ", "ЕДИНЫЙ РАЗМЕР", "ДЕТСКИЙ", "ПОДРОСТКОВЫЙ",
}
_RE_SIZE_WORD = re.compile(r"\b[A-Za-zА-Яа-яЁё\- ]{3,}\b", re.IGNORECASE)


# ===== Утилиты парсинга =====
def _heal_linebreaks(raw: str) -> str:
    """
    Склеивает разрывы внутри токенов и после '/':
      'бел\\nый' -> 'белый', '/\\n' -> '/'
    """
    t = raw or ""
    t = re.sub(r"/\s*\n\s*", "/", t)  # '/\n' -> '/'
    t = re.sub(r"([A-Za-zА-Яа-яЁё])\s*\n\s*([A-Za-zА-Яа-яЁё])", r"\1\2", t)  # 'сло\nво' -> 'слово'
    t = re.sub(r"[ \t]+", " ", t)
    return t


def _first_page_text(pdf_path: Path) -> str:
    with pdfplumber.open(str(pdf_path)) as pdf:
        if not pdf.pages:
            return ""
        raw = pdf.pages[0].extract_text() or ""
        return _heal_linebreaks(raw)


def _clean_size(s: str) -> str:
    s = s.strip()
    s = re.sub(r"[–—]", "-", s)                 # нормализуем тире
    s = re.sub(r"\s*([\-\/])\s*", r"\1", s)     # пробелы вокруг - и /
    s = re.sub(r"\s+", " ", s)
    return s


def _extract_size_from_text(text: str) -> Optional[str]:
    # 1) Явная метка "Размер:"
    m = re.search(r"Размер:\s*([^\r\n]+)", text, re.IGNORECASE)
    if m:
        return _clean_size(m.group(1))
    # 2) Буквенные сочетания
    m = _RE_SIZE_ALPHA.search(text)
    if m:
        return _clean_size(m.group(0).upper())
    # 3) Числовые
    m = _RE_SIZE_NUMERIC.search(text)
    if m:
        return _clean_size(m.group(0))
    # 4) Словесные
    words_upper = {w.upper() for w in _SIZE_WORDS}
    for m in _RE_SIZE_WORD.finditer(text):
        cand = _clean_size(m.group(0))
        if cand.upper() in words_upper:
            return cand.upper() if re.search(r"[A-Za-z]", cand) else cand
    return None


def _dedupe_concat(s: str) -> str:
    """Схлопывает дубли «X X X» слитые подряд без разделителя: 'XX' -> 'X', 'XXX' -> 'X'."""
    while True:
        m = re.fullmatch(r"(.+?)\1+", s)
        if not m:
            return s
        s = m.group(1)


def _cleanup_article(s: str) -> str:
    # отрезаем всё после любого вхождения "Цвет" (в т.ч. слитного), убираем двоеточие и дубли
    s = _RE_COLOR_TOKEN.split(s, maxsplit=1)[0]
    s = s.rstrip(":").strip()
    s = _dedupe_concat(s)
    return s



def _extract_article(text: str) -> Optional[str]:
    # 1) 'Артикул ...' до 'Цвет'
    m = _RE_ART.search(text)
    if m:
        return _cleanup_article(m.group(1).strip())
    # 2) 'арт. XXX/yyy'
    m = _RE_ART_ALT1.search(text)
    if m:
        return _cleanup_article(m.group(1).strip())
    # 3) общий токен "XXX/yyy"
    m = _RE_ART_ALT2.search(text)
    if m:
        return _cleanup_article(m.group(1).strip())
    return None


def _extract_color(text: str, article: Optional[str]) -> Optional[str]:
    m = _RE_COLOR.search(text)
    if m:
        return m.group(1).strip()
    m = _RE_NAME_COLOR.search(text)
    if m:
        return m.group(1).strip()
    if article and "/" in article:
        return article.split("/", 1)[1].strip()
    return None


def _extract_meta_from_first_page(pdf_path: Path) -> Tuple[str, str, str]:
    """
    Универсальный парсер: работает и для «первого», и для «второго» варианта макета.
    Возвращает (артикул, размер, цвет); пустые строки, если не нашли.
    """
    txt = _first_page_text(pdf_path)
    article = _extract_article(txt) or ""
    size = _extract_size_from_text(txt) or ""
    color = _extract_color(txt, article) or ""
    if article:
        article = _cleanup_article(article)  # финальная страховка
    return article, size, color


def _pages_count(pdf_path: Path) -> int:
    return len(PdfReader(str(pdf_path)).pages)


def _is_tmp_name(name: str) -> bool:
    n = name.lower()
    return ("__head_" in n) or ("__tail_" in n) or ("tmp" in n)


# ===== Публичная функция: сканирует директорию и возвращает BYTES Excel =====
async def build_inventory_report_excel_bytes(
    directory: Path | str = PDF_DIR,
    include_tmp_files: bool = False,
) -> tuple[bytes, str]:
    """
    Сканирует директорию и возвращает (bytes, filename) Excel-файла (ничего не пишем на диск).
    Колонки: артикул | размер | цвет | количество
    """
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)

    rows: List[dict] = []
    for pdf_path in sorted(directory.glob("*.pdf")):
        name = pdf_path.name
        if not include_tmp_files and _is_tmp_name(name):
            continue
        try:
            article, size, color = _extract_meta_from_first_page(pdf_path)
            count = _pages_count(pdf_path)
        except Exception:
            # битые/нечитаемые — пропускаем
            continue

        rows.append({
            "артикул": article,
            "размер": size,
            "цвет": color,
            "количество": count,
        })

    df = pd.DataFrame(rows, columns=["артикул", "размер", "цвет", "количество"])

    # Собираем Excel в памяти
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"report_{ts}.xlsx"
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="report")
    buf.seek(0)
    return buf.read(), filename


__all__ = [
    "build_inventory_report_excel_bytes",
]
