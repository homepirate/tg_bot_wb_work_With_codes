from pathlib import Path
from typing import Optional, Tuple, Dict, List
import re
import os

import pdfplumber
from PyPDF2 import PdfReader, PdfWriter

PDF_DIR = Path("pdf-codes")
OUT_DIR = PDF_DIR
OUT_DIR.mkdir(parents=True, exist_ok=True)


async def _save_temp_pdf(data: bytes, filename: str, user_id: int) -> Path:
    """Сохраняем во временную папку и возвращаем путь (для последующего удаления)."""
    tmp_dir = PDF_DIR / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = tmp_dir / f"{user_id}_{filename}"
    with open(tmp_path, "wb") as f:
        f.write(data)
    return tmp_path

# Регулярки для вытаскивания полей со страницы
_RE_ART   = re.compile(r"Артикул\s+(.+)", re.IGNORECASE)
_RE_COLOR = re.compile(r"Цвет:\s*([^\r\n]+)", re.IGNORECASE)
_RE_SIZE  = re.compile(r"Размер:\s*([^\r\n]+)", re.IGNORECASE)

def _safe_name(s: str) -> str:
    s = s.strip()
    s = re.sub(r"[^\w\-\.\s/]+", "_", s, flags=re.UNICODE)
    s = s.replace(" ", "_").replace("/", "-")
    return s[:120] if len(s) > 120 else s

def _extract_page_meta(pl_page) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Возвращает (article, size, color) с одной страницы.
    """
    txt = pl_page.extract_text(x_tolerance=1.0, y_tolerance=1.0) or ""
    lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
    text = "\n".join(lines)

    art = None
    color = None
    size = None

    m = _RE_ART.search(text)
    if m:
        art = m.group(1).strip()

    m = _RE_COLOR.search(text)
    if m:
        color = m.group(1).strip()

    m = _RE_SIZE.search(text)
    if m:
        size = m.group(1).strip()

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
