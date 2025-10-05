# services/return_pdf.py
from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any
import asyncio

import pdfplumber
from sqlalchemy.ext.asyncio import AsyncSession

from .pdf_rw import _extract_page_code
from .pdf_splitter import split_pdf_by_meta
from models.printed_code import PrintedCode


# --------- sync helpers (уводим в поток) ---------

def _collect_all_codes_sync(src_pdf: Path | str) -> List[str]:
    """
    Открывает PDF и проходит по всем страницам, извлекая код функцией
    core.pdf_rw._extract_page_code(pl_pdf, page_index).
    Возвращает список уникальных кодов в порядке появления.
    """
    src = Path(src_pdf)
    if not src.exists():
        raise FileNotFoundError(f"Файл не найден: {src}")

    codes: List[str] = []
    with pdfplumber.open(str(src)) as pl_pdf:
        for i in range(len(pl_pdf.pages)):
            code = _extract_page_code(pl_pdf, i)
            if code:
                codes.append(code)

    # дедупликация с сохранением порядка
    seen = set()
    uniq: List[str] = []
    for c in codes:
        if c not in seen:
            seen.add(c)
            uniq.append(c)
    return uniq


# ------------------- main API --------------------

async def return_pdf(session: AsyncSession, src_pdf: Path | str) -> Dict[str, Any]:
    """
    1) Собираем ВСЕ коды по всем страницам через core.pdf_rw._extract_page_code.
    2) Удаляем найденные коды из printed_codes (если какой-то не найден — это нормально).
    3) Для всего PDF вызываем split_pdf_by_meta (внутри имена уже с временной меткой).
    4) Возвращаем сводку.
    """
    src = Path(src_pdf)
    if not src.exists():
        raise FileNotFoundError(f"Файл не найден: {src}")

    # 1) собрать коды (в отдельном потоке, т.к. синхронно и тяжело)
    codes: List[str] = await asyncio.to_thread(_collect_all_codes_sync, src)

    # 2) удалить найденные коды из БД (без ошибок, если записи не существует)
    deleted_codes: List[str] = []
    if codes:
        for code in codes:
            row = await session.get(PrintedCode, code)
            if row:
                await session.delete(row)
                deleted_codes.append(code)
        if deleted_codes:
            await session.commit()

    # 3) разрезать и сохранить PDF (тоже уводим в поток)
    report = await asyncio.to_thread(split_pdf_by_meta, src)

    # 4) результат
    return {
        "codes": codes,                         # все найденные коды в документе
        "deleted_codes": deleted_codes,         # реально удалённые из БД
        "saved": [str(x["path"]) for x in report.get("outputs", [])],
        "skipped_without_meta": report.get("skipped_without_meta", 0),
        "total_pages": report.get("total_pages", 0),
    }
