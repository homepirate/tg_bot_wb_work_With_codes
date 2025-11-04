from pathlib import Path
from typing import Optional, Tuple, Dict, List
import re
import os

import pdfplumber
from PyPDF2 import PdfReader, PdfWriter
from .patterns import *
from datetime import datetime
from .text_clean import clean_for_parsing, strip_gs1, normalize_dashes, clean_color_value


# PDF_DIR = Path("pdf-codes")
OUT_DIR = PDF_DIR
OUT_DIR.mkdir(parents=True, exist_ok=True)

def _cleanup_article(s: str) -> str:
    # отрезаем всё после "Цвет", убираем двоеточие/хвостовой дефис и дубли
    s = RE_COLOR_TOKEN.split(s, maxsplit=1)[0]
    s = s.rstrip(":").strip()
    # убрать висящий дефис в конце (после склейки переносов)
    s = re.sub(r"[-–—]+$", "", s).strip()
    # схлопнуть «XX» → «X» если внезапно склеилось дважды
    while True:
        m = re.fullmatch(r"(.+?)\1+", s)
        if not m: break
        s = m.group(1)
    return s

def _extract_article(text: str) -> Optional[str]:
    # 1) обычный «Артикул ...»
    m = RE_ART.search(text)
    if m:
        return _cleanup_article(m.group(1).strip())

    # 2) 'арт. XXX/yyy'
    m = RE_ART_ALT1.search(text)
    if m:
        return _cleanup_article(m.group(1).strip())

    # 3) fallback: «Артикул» на СВОЕЙ строке, значение — на следующей
    lines = [ln.strip() for ln in (text or "").splitlines()]
    for i, ln in enumerate(lines):
        if re.fullmatch(r"артикул[:.]?", ln, flags=re.IGNORECASE):
            if i + 1 < len(lines) and lines[i+1]:
                return _cleanup_article(lines[i+1].strip())

    # 4) общий токен XXX/yyy
    m = RE_ART_ALT2.search(text)
    if m:
        return _cleanup_article(m.group(1).strip())

    return None

def _clean_size(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[–—]", "-", s)              # нормализуем тире
    s = re.sub(r"\s*([\-\/])\s*", r"\1", s)  # пробелы вокруг - и /
    s = re.sub(r"\s+", " ", s).strip()
    return s.split(" ", 1)[0] if s else ""


def _unglue_labels(t: str) -> str:
    # вставляем разделитель перед метками, если они прилипли
    # пример: "...мужскойАртикулLT..." -> "...мужской\nАртикул LT..."
    t = re.sub(r"(?<!^)(Артикул)(?=\S)", r"\n\1 ", t, flags=re.IGNORECASE)
    t = re.sub(r"(?<!^)(Цвет\s*:)(?=\S)",   r"\n\1 ", t, flags=re.IGNORECASE)
    t = re.sub(r"(?<!^)(Размер\s*:)(?=\S)", r"\n\1 ", t, flags=re.IGNORECASE)
    return t

def _heal_linebreaks(raw: str) -> str:
    t = raw or ""
    # '/\n' -> '/'
    t = re.sub(r"/\s*\n\s*", "/", t)
    # перенос с дефисом внутри слова: 'сло-\nво' -> 'слово'
    t = re.sub(r"([A-Za-zА-Яа-яЁё])-\s*\n\s*([A-Za-zА-Яа-яЁё])", r"\1\2", t)
    # обычный перенос внутри слова: 'сло\nво' -> 'слово'
    t = re.sub(r"([A-Za-zА-Яа-яЁё])\s*\n\s*([A-Za-zА-Яа-яЁё])", r"\1\2", t)
    # если дефис-цвет оказался один на отдельной строке после склейки — оставим как есть
    t = re.sub(r"[ \t]+", " ", t)
    t = _unglue_labels(t)
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
    m = RE_SIZE_ALPHA.search(text)
    if m:
        return _clean_size(m.group(0).upper())

    # 3) Числовые варианты
    m = RE_SIZE_NUMERIC.search(text)
    if m:
        return _clean_size(m.group(0))

    # 4) Словесные
    for m in RE_SIZE_WORD.finditer(text):
        cand = _clean_size(m.group(0))
        if cand.upper() in {w.upper() for w in SIZE_WORDS}:
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
    txt = clean_for_parsing(raw)
    txt = normalize_dashes(txt)

    txt_wo_gs1 = strip_gs1(txt)

    lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
    text  = "\n".join(lines)

    # ---- Артикул
    m = RE_ART.search(text) or RE_ART_ALT1.search(text) or RE_ART_ALT2.search(text)
    art = None
    if m:
        # обрубить всё после "Цвет"
        val = RE_COLOR_TOKEN.split(m.group(1), maxsplit=1)[0]
        art = re.sub(r"[-–—]+$", "", val).strip()
    else:
        # запасной вариант: «Артикул» на своей строке
        for i, ln in enumerate(text.splitlines()):
            if re.fullmatch(r"артикул[:.]?", ln, flags=re.IGNORECASE):
                ls = text.splitlines()
                if i+1 < len(ls) and ls[i+1].strip():
                    val = RE_COLOR_TOKEN.split(ls[i+1], maxsplit=1)[0]
                    art = re.sub(r"[-–—]+$", "", val).strip()
                break
        if not art:
            m = RE_ART_ALT2.search(text)
            if m:
                val = RE_COLOR_TOKEN.split(m.group(1), maxsplit=1)[0]
                art = re.sub(r"[-–—]+$", "", val).strip()
    if art:
        # схлопнуть «XX» → «X» (редкий дефект)
        while True:
            mm = re.fullmatch(r"(.+?)\1+", art)
            if not mm:
                break
            art = mm.group(1)

    # ---- Размер (из очищенного от GS1 текста)
    size = None
    m = RE_SIZE_LABEL.search(txt_wo_gs1)
    if m:
        size = m.group(1)
    if not size:
        m = RE_SIZE_ALPHA.search(txt_wo_gs1)
        if m: size = m.group(0).upper()
    if not size:
        m = RE_SIZE_NUMERIC.search(txt_wo_gs1)
        if m: size = m.group(0)
    if not size:
        for m in RE_SIZE_WORD.finditer(txt_wo_gs1):
            cand = m.group(0)
            if cand.upper() in {w.upper() for w in SIZE_WORDS}:
                size = cand
                break
    if size:
        size = re.sub(r"[–—]", "-", size)
        size = re.sub(r"\s*([\-\/])\s*", r"\1", size)
        size = re.sub(r"\s+", " ", size).strip()
        # брать только первый токен (на случай «58 (обхват)»)
        size = size.split(" ", 1)[0]

    # ---- Цвет: только из текста без GS1 + агрессивная чистка
    color = ""
    m = RE_COLOR.search(txt_wo_gs1)
    if m:
        color = clean_color_value(m.group(1))
    if not color:
        m = RE_NAME_COLOR.search(txt_wo_gs1)
        if m:
            color = clean_color_value(m.group(1))
    if not color:
        m = RE_COLOR_DASH_LINE.search(txt_wo_gs1)
        if m:
            color = clean_color_value(m.group(1))
    if not color and art and "/" in art:
        color = clean_color_value(art.split("/", 1)[1])

    return (art or None), (size or None), (color or None)



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
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        fname = f"{_safe_name(art)}__{_safe_name(size)}__{_safe_name(color)}__{len(writer.pages)}p_{ts}.pdf"
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
