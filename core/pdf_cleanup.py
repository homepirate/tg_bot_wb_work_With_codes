from pathlib import Path
import pdfplumber
from PyPDF2 import PdfReader

from sqlalchemy.ext.asyncio import AsyncSession
from services.printed_codes import get_all_codes
from core.pdf_rw import (
    PDF_DIR,
    _extract_code_from_text,
    _build_tail_writer,
    _write_pdf,
    _replace_file,
)

def _is_tmp_name(name: str) -> bool:
    """Фильтруем временные файлы (__head_, __tail_, tmp)."""
    n = name.lower()
    return ("__head_" in n) or ("__tail_" in n) or ("tmp" in n)


async def purge_known_codes_in_dir(
    session: AsyncSession,
    directory: Path | str = PDF_DIR,
    include_tmp_files: bool = False,
) -> dict:
    """
    Проходит по всем PDF в директории и удаляет страницы,
    где код уже присутствует в таблице printed_code.
    Использует локальный set всех кодов (один SQL-запрос).
    """

    root = Path(directory)
    root.mkdir(parents=True, exist_ok=True)

    stats = {
        "files_scanned": 0,
        "files_modified": 0,
        "files_deleted": 0,
        "pages_scanned": 0,
        "pages_deleted": 0,
        "details": [],
    }

    # 1️⃣ Получаем все существующие коды одним запросом
    all_codes = await get_all_codes(session)
    stats["details"].append(f"📦 Загружено {len(all_codes)} кодов из БД")

    # 2️⃣ Проходим по всем PDF
    files = sorted(root.glob("*.pdf"))
    for i, pdf_path in enumerate(files):
        # print(f"Cleanup {i} of {len(files)}")
        name = pdf_path.name
        if not include_tmp_files and _is_tmp_name(name):
            continue

        stats["files_scanned"] += 1
        try:
            reader = PdfReader(str(pdf_path))
        except Exception as e:
            stats["details"].append(f"⚠️ {name}: не удалось открыть ({e})")
            continue

        total_pages = len(reader.pages)
        if total_pages == 0:
            pdf_path.unlink(missing_ok=True)
            stats["files_deleted"] += 1
            stats["details"].append(f"🗑 {name}: пустой файл удалён")
            continue

        keep_indexes = set()
        deleted_here = 0

        try:
            with pdfplumber.open(str(pdf_path)) as pl_pdf:
                for i in range(total_pages):
                    stats["pages_scanned"] += 1
                    txt = pl_pdf.pages[i].extract_text(x_tolerance=1.0, y_tolerance=1.0) or ""
                    code = _extract_code_from_text(txt)
                    if code and code in all_codes:
                        deleted_here += 1
                        continue
                    keep_indexes.add(i)
        except Exception as e:
            stats["details"].append(f"⚠️ {name}: ошибка чтения ({e})")
            continue

        if deleted_here == 0:
            continue

        stats["pages_deleted"] += deleted_here

        if not keep_indexes:
            pdf_path.unlink(missing_ok=True)
            stats["files_deleted"] += 1
            stats["details"].append(f"🗑 {name}: удалён полностью (все коды известны)")
            continue

        # Пересобираем PDF без удалённых страниц
        try:
            writer = _build_tail_writer(reader, total_pages, keep_indexes)
            tmp_dir = pdf_path.parent / "tmp"
            tmp_dir.mkdir(parents=True, exist_ok=True)
            tmp_path = tmp_dir / f"{pdf_path.stem}__purged_tmp.pdf"
            _write_pdf(writer, tmp_path)
            _replace_file(tmp_path, pdf_path)
            stats["files_modified"] += 1
            stats["details"].append(
                f"✂️ {name}: удалено {deleted_here} из {total_pages} страниц"
            )
        except Exception as e:
            stats["details"].append(f"⚠️ {name}: ошибка записи ({e})")

    return stats
