# core/pdf_report_builder.py
from datetime import datetime
from typing import Optional, Tuple
from pathlib import Path
import io
import re

import pandas as pd
import pdfplumber
from PyPDF2 import PdfReader

from .patterns import (
    PDF_DIR,
    RE_COLOR, RE_NAME_COLOR, RE_COLOR_DASH_LINE, RE_COLOR_TOKEN,
    RE_SIZE_LABEL, RE_SIZE_ALPHA, RE_SIZE_NUMERIC, RE_SIZE_WORD, SIZE_WORDS,
    RE_ART, RE_ART_ALT1, RE_ART_ALT2,
)
from .text_clean import clean_for_parsing, normalize_dashes, strip_gs1, clean_color_value

__all__ = ["build_inventory_report_excel_bytes"]


# ---------- базовые утилиты ----------

def _is_tmp_name(name: str) -> bool:
    n = (name or "").lower()
    return ("__head_" in n) or ("__tail_" in n) or ("tmp" in n)


def _color_from_filename(path: Path) -> Optional[str]:
    """
    Имена сплиттера: <art>__<size>__<color>__<N>p_<ts>.pdf
    Берём 3-й сегмент между двойными подчёркиваниями.
    """
    base = path.stem  # без .pdf
    parts = base.split("__")
    if len(parts) >= 3:
        raw = parts[2]
        # Np_ и дата идут уже после следующего "__" — нам не мешают
        raw = re.sub(r"__\d+p_.*$", "", raw)
        return clean_color_value(raw)
    return None

def _first_page_text(pdf_path: Path) -> str:
    """Текст 1-й страницы с такими же толерансами и очисткой, как в сплиттере."""
    with pdfplumber.open(str(pdf_path)) as pdf:
        if not pdf.pages:
            return ""
        raw = pdf.pages[0].extract_text(x_tolerance=1.0, y_tolerance=1.0) or ""
    t = clean_for_parsing(raw)
    t = normalize_dashes(t)
    return t


def _dedupe_concat(s: str) -> str:
    """Схлопывает дубли 'XXX' -> 'X' при склейке переносов."""
    while True:
        m = re.fullmatch(r"(.+?)\1+", s)
        if not m:
            return s
        s = m.group(1)


def _cleanup_article(s: str) -> str:
    # отрезаем всё после «Цвет», убираем хвостовые тире/двоеточие, схлопываем дубли
    s = RE_COLOR_TOKEN.split(s, maxsplit=1)[0]
    s = re.sub(r"[-–—]+$", "", s).strip().rstrip(":").strip()
    return _dedupe_concat(s)


def _clean_size(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[–—]", "-", s)                 # нормализуем типы тире
    s = re.sub(r"\s*([\-\/])\s*", r"\1", s)     # пробелы вокруг - и /
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ---------- извлечение метаданных ----------

def _extract_article(text: str) -> Optional[str]:
    m = RE_ART.search(text) or RE_ART_ALT1.search(text) or RE_ART_ALT2.search(text)
    if not m:
        return None
    val = _cleanup_article(m.group(1))
    return val or None


def _extract_size_from_text(text: str) -> Optional[str]:
    """Размер из текста: та же логика, что в сплиттере, с предварительным срезом GS1."""
    t = strip_gs1(text)

    m = RE_SIZE_LABEL.search(t)
    if m:
        s = m.group(1)
    else:
        m = RE_SIZE_ALPHA.search(t)
        if m:
            s = m.group(0).upper()
        else:
            m = RE_SIZE_NUMERIC.search(t)
            if m:
                s = m.group(0)
            else:
                s = None
                words_upper = {w.upper() for w in SIZE_WORDS}
                for mm in RE_SIZE_WORD.finditer(t):
                    cand = mm.group(0)
                    if cand.upper() in words_upper:
                        s = cand
                        break
    return _clean_size(s) if s else None


def _extract_color(text: str, article: Optional[str], filename_hint: Optional[str] = None) -> Optional[str]:
    """
    Порядок:
    1) Цвет: <...>
    2) <Голова-товара> <цвет> р.
    3) Отдельная строка вида "- черный"
    4) Подсказка из имени файла (если есть)
    5) Хвост после "/" в артикула (XXX/цвет)
    Все значения проходят через clean_color_value().
    """
    t = strip_gs1(text)

    m = RE_COLOR.search(t)
    if m:
        c = clean_color_value(m.group(1))
        if c: return c

    m = RE_NAME_COLOR.search(t)  # теперь ловит «Шапка-ушанка молочный р.»
    if m:
        c = clean_color_value(m.group(1))
        if c: return c

    m = RE_COLOR_DASH_LINE.search(t)
    if m:
        c = clean_color_value(m.group(1))
        if c: return c

    if filename_hint:
        c = clean_color_value(filename_hint)
        if c: return c

    if article and "/" in article:
        c = clean_color_value(article.split("/", 1)[1])
        if c: return c

    return None



def _extract_meta_from_first_page(pdf_path: Path) -> Tuple[str, str, str]:
    txt = _first_page_text(pdf_path)     # нормализованный текст
    filename_color = _color_from_filename(pdf_path)

    article = _extract_article(txt) or ""
    size    = _extract_size_from_text(txt) or ""
    color   = _extract_color(txt, article, filename_hint=filename_color) or ""

    if article:
        article = _cleanup_article(article)

    return article, size, color


def _pages_count(pdf_path: Path) -> int:
    return len(PdfReader(str(pdf_path)).pages)


# ---------- публичная функция отчёта ----------

async def build_inventory_report_excel_bytes(
    directory: Path | str = PDF_DIR,
    include_tmp_files: bool = False,
) -> tuple[bytes, str]:
    """
    Сканирует директорию с PDF и возвращает Excel-отчёт (bytes, filename).
    Колонки: артикул | размер | цвет | количество
    Дубликаты (по троице ключей) агрегируются суммой страниц.
    """
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)

    def _first_token(s: str) -> str:
        parts = str(s or "").strip().split()
        return parts[0] if parts else ""

    rows = []
    for pdf_path in sorted(directory.glob("*.pdf")):
        name = pdf_path.name
        if not include_tmp_files and _is_tmp_name(name):
            continue
        try:
            article, size, color = _extract_meta_from_first_page(pdf_path)
            count = _pages_count(pdf_path)
        except Exception:
            # Битые/нечитаемые — пропускаем
            continue

        rows.append({
            "артикул": (article or "").strip(),
            "размер":  _first_token(size),
            "цвет":    (color or "").strip().lower(),
            "количество": int(count),
        })

    df = pd.DataFrame(rows, columns=["артикул", "размер", "цвет", "количество"])

    if not df.empty:
        df = (
            df.groupby(["артикул", "размер", "цвет"], as_index=False, dropna=False)
              .agg({"количество": "sum"})
              .sort_values(["артикул", "размер", "цвет"], ignore_index=True)
        )

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"report_{ts}.xlsx"
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="report")
    buf.seek(0)
    return buf.read(), filename
