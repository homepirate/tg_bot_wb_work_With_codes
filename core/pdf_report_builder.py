from datetime import datetime
from typing import Optional, Tuple
import io

import pandas as pd
import pdfplumber
from PyPDF2 import PdfReader

from .patterns import *


__all__ = [
    "build_inventory_report_excel_bytes",
]


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
    # Удаляем GS1-блоки, чтобы сериал не мешал распознаванию (например, '(21)5l-...' → '5L').
    text = re.sub(r"\(01\)\s*\d{14}", " ", text)
    text = re.sub(r"\(21\)\s*[!-~]{4,}", " ", text)

    # 1) Явная метка "Размер:"
    m = re.search(r"Размер:\s*([^\r\n]+)", text, re.IGNORECASE)
    if m:
        return _clean_size(m.group(1))
    # 2) Буквенные сочетания
    m = RE_SIZE_ALPHA.search(text)
    if m:
        return _clean_size(m.group(0).upper())
    # 3) Числовые
    m = RE_SIZE_NUMERIC.search(text)
    if m:
        return _clean_size(m.group(0))
    # 4) Словесные
    words_upper = {w.upper() for w in SIZE_WORDS}
    for m in RE_SIZE_WORD.finditer(text):
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
    s = RE_COLOR_TOKEN.split(s, maxsplit=1)[0]
    s = s.rstrip(":").strip()
    s = _dedupe_concat(s)
    return s


def _extract_article(text: str) -> Optional[str]:
    # 1) 'Артикул ...' до 'Цвет'
    m = RE_ART.search(text)
    if m:
        return _cleanup_article(m.group(1).strip())
    # 2) 'арт. XXX/yyy'
    m = RE_ART_ALT1.search(text)
    if m:
        return _cleanup_article(m.group(1).strip())
    # 3) общий токен "XXX/yyy"
    m = RE_ART_ALT2.search(text)
    if m:
        return _cleanup_article(m.group(1).strip())
    return None


def _extract_color(text: str, article: Optional[str]) -> Optional[str]:
    # 1️⃣ Прямое упоминание "Цвет: ..."
    m = RE_COLOR.search(text)
    if m:
        return m.group(1).strip()

    # 2️⃣ Конструкции вида "Манишка черный р." / "Балаклава белая р."
    m = RE_NAME_COLOR.search(text)
    if m:
        return m.group(1).strip()

    # 3️⃣ Строки, где цвет отдельно через дефис, например "-черный" или "— белый"
    m = RE_COLOR_DASH_LINE.search(text)
    if m:
        return m.group(1).strip()

    # 4️⃣ Фоллбэк: если цвет закодирован в артикуле "XXX/цвет"
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
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)

    def _first_token(s: str) -> str:
        parts = str(s or "").strip().split()
        return parts[0] if parts else ""

    rows: list[dict] = []
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
            "артикул": str(article or "").strip(),
            "размер": _first_token(size),            # ← фикс
            "цвет": str(color or "").strip().lower(),
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
