from pathlib import Path
from typing import Optional, Tuple, Dict, List
import re
import os

import pdfplumber
from PyPDF2 import PdfReader, PdfWriter

PDF_DIR = Path("pdf-codes")
OUT_DIR = PDF_DIR
OUT_DIR.mkdir(parents=True, exist_ok=True)


# Регулярки для вытаскивания полей со страницы
_RE_ART   = re.compile(r"Артикул\s+(.+)", re.IGNORECASE)
_RE_COLOR = re.compile(r"Цвет:\s*([^\r\n]+)", re.IGNORECASE)
_RE_SIZE  = re.compile(r"Размер:\s*([^\r\n]+)", re.IGNORECASE)
# Числовые размеры: 56-60, 56–60, 56/58, одиночное число 56
_RE_SIZE_NUMERIC = re.compile(r"\b\d{2}(?:[–\-\/]\d{2})?\b")

_RE_SIZE_ALPHA = re.compile(
    r"""
    \b(
        (?:XS|S|M|L|XL|XXL|XXXL)                          # обычные
        |
        (?:[2-5](?:XS|XL|XXL|XXXL))                       # 2XL, 3XL, 4XL, 5XL, а также 2XS и т.п.
    )
    (?:[\/\-–]
        (?:XS|S|M|L|XL|XXL|XXXL|[2-5](?:XS|XL|XXL|XXXL))  # пары: S/M, L–XL, 3XL/4XL и т.п.
    )?
    \b
    """,
    re.IGNORECASE | re.VERBOSE,
)
# Общие «словесные» размеры (можно расширять по мере встреч)
_SIZE_WORDS = {
    "ONE SIZE", "ONESIZE", "UNI", "UNISIZE", "UNIVERSAL",
    "УНИВЕРСАЛЬНЫЙ", "ЕДИНЫЙ РАЗМЕР", "ДЕТСКИЙ", "ПОДРОСТКОВЫЙ",
}
# одиночное слово (латиница/кириллица), чтобы поймать «универсальный» и пр.
_RE_SIZE_WORD = re.compile(r"\b[A-Za-zА-Яа-яЁё\- ]{3,}\b", re.IGNORECASE)


def _clean_size(s: str) -> str:
    # нормализуем дефисы, убираем лишние пробелы вокруг разделителей
    s = s.strip()
    s = re.sub(r"[–—]", "-", s)
    s = re.sub(r"\s*([\-\/])\s*", r"\1", s)
    s = re.sub(r"\s+", " ", s)
    return s

# рядом с твоими регулярками
_RE_SIZE_TOKEN = re.compile(r"\b\d{2}-\d{2}\b")  # 56-60, 56-58 и т.п.
_RE_NAME_COLOR = re.compile(r"Балаклава\s+(.+?)\s+р\.", re.IGNORECASE | re.DOTALL)

def _heal_linebreaks(raw: str) -> str:
    """
    Склеивает разорванные переносами слова и конструкции вида:
      'бел\\nый' -> 'белый'
      '/\\n'     -> '/'
    Оставляем обычные пробелы/переводы строк как есть.
    """
    t = raw
    # 1) Убрать перевод строки сразу после слеша: '/\n' -> '/'
    t = re.sub(r"/\s*\n\s*", "/", t)
    # 2) Склеить разорванные слова: 'сло\nво' -> 'слово'
    t = re.sub(r"([A-Za-zА-Яа-яЁё])\s*\n\s*([A-Za-zА-Яа-яЁё])", r"\1\2", t)
    # 3) Нормализовать множественные пробелы вокруг точек/знаков (немного косметики)
    t = re.sub(r"[ \t]+", " ", t)
    return t


def _extract_size_from_text(text: str) -> Optional[str]:
    # вырезаем GS1-блоки, чтобы сериал не мешал распознаванию размера
    text = re.sub(r"\(01\)\s*\d{14}", " ", text)
    text = re.sub(r"\(21\)\s*[!-~]{4,}", " ", text)

    # 1) Явная метка "Размер:"
    m = re.search(r"Размер:\s*([^\r\n]+)", text, re.IGNORECASE)
    if m:
        return _clean_size(m.group(1))

    # 2) Буквенные комбинации
    m = _RE_SIZE_ALPHA.search(text)
    if m:
        return _clean_size(m.group(0).upper())

    # 3) Числовые варианты
    m = _RE_SIZE_NUMERIC.search(text)
    if m:
        return _clean_size(m.group(0))

    # 4) Словесные
    for m in _RE_SIZE_WORD.finditer(text):
        cand = _clean_size(m.group(0))
        if cand.upper() in {w.upper() for w in _SIZE_WORDS}:
            return cand.upper() if re.search(r"[A-Za-z]", cand) else cand

    return None



async def _save_temp_pdf(data: bytes, filename: str, user_id: int) -> Path:
    """Сохраняем во временную папку и возвращаем путь (для последующего удаления)."""
    tmp_dir = PDF_DIR / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = tmp_dir / f"{user_id}_{filename}"
    with open(tmp_path, "wb") as f:
        f.write(data)
    return tmp_path


def _safe_name(s: str) -> str:
    s = s.strip()
    s = re.sub(r"[^\w\-\.\s/]+", "_", s, flags=re.UNICODE)
    s = s.replace(" ", "_").replace("/", "-")
    return s[:120] if len(s) > 120 else s

def _extract_page_meta(pl_page) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    raw = pl_page.extract_text(x_tolerance=1.0, y_tolerance=1.0) or ""
    txt = _heal_linebreaks(raw)

    lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
    text  = "\n".join(lines)

    art: Optional[str] = None
    size: Optional[str] = None
    color: Optional[str] = None

    # --- Артикул (как было у тебя, можно оставить без изменений) ---
    m = _RE_ART.search(text)
    if m:
        art = m.group(1).strip()
    if not art:
        m = re.search(r"арт\.\s*([A-Z0-9_]+/\S+)", text, re.IGNORECASE)
        if m:
            art = m.group(1).strip()
    if not art:
        cand = re.findall(r"\b(OA_[A-Za-z0-9_]+/\S+)", text)
        if cand:
            art = cand[-1].strip()

    # --- Размер (новая логика) ---
    size = _extract_size_from_text(txt)

    # --- Цвет (как было у тебя) ---
    m = _RE_COLOR.search(text)
    if m:
        color = m.group(1).strip()
    else:
        m = re.search(r"Балаклава\s+(.+?)\s+р\.", text, re.IGNORECASE | re.DOTALL)
        if m:
            color = m.group(1).strip()
        if not color and art and "/" in art:
            color = art.split("/", 1)[1].strip()

    return art, size, color

def split_pdf_by_meta(src_pdf: Path | str) -> dict:
    """
    Делит входной PDF на несколько по ключу (Артикул, Размер, Цвет).
    В КАЖДУЮ группу попадают ВСЕ страницы, у которых найден этот ключ.
    Возвращает отчёт:
    {
      "outputs": [{"path": Path, "pages": int, "key": (art, size, color)}...],
      "skipped_without_meta": int,
      "total_pages": int
    }
    """
    src = Path(src_pdf)
    if not src.exists():
        raise FileNotFoundError(f"Файл не найден: {src}")

    reader = PdfReader(str(src))
    total = len(reader.pages)

    groups: Dict[Tuple[str, str, str], PdfWriter] = {}
    skipped_without_meta = 0

    with pdfplumber.open(str(src)) as pl_pdf:
        for i in range(total):
            art, size, color = _extract_page_meta(pl_pdf.pages[i])
            if not (art and size and color):
                skipped_without_meta += 1
                continue

            key = (art, size, color)
            if key not in groups:
                groups[key] = PdfWriter()
            groups[key].add_page(reader.pages[i])

    outputs = []
    for (art, size, color), writer in groups.items():
        if len(writer.pages) == 0:
            continue
        fname = f"{_safe_name(art)}__{_safe_name(size)}__{_safe_name(color)}__{len(writer.pages)}p.pdf"
        out_path = OUT_DIR / fname
        tmp = OUT_DIR / (fname + ".__tmp")
        with open(tmp, "wb") as f:
            writer.write(f)
        os.replace(tmp, out_path)
        outputs.append({"path": out_path, "pages": len(writer.pages), "key": (art, size, color)})

    return {
        "outputs": outputs,
        "skipped_without_meta": skipped_without_meta,
        "total_pages": total,
    }
