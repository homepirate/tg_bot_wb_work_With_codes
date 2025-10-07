# services/exception_codes_import.py
from __future__ import annotations

import re
from io import BytesIO
from typing import Dict, Any

import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession

from services.printed_codes import register_code_if_new

ALLOWED_PREFIXES = ("01046", "01029")

def _normalize_code(s: str) -> str:
    s = (s or "").strip()
    # убрать все виды пробелов, в т.ч. неразрывные
    s = re.sub(r"\s+", "", s, flags=re.UNICODE)
    # иногда Excel вставляет невидимые управляющие символы
    s = re.sub(r"[\u200B-\u200D\uFEFF]", "", s)  # zero-width
    s = s.replace("(", "").replace(")", "")
    return s

def _is_valid_code(s_norm: str) -> bool:
    return (
        (s_norm.startswith(ALLOWED_PREFIXES[0]) or s_norm.startswith(ALLOWED_PREFIXES[1]))
        and re.fullmatch(r"01\d{14}21[!-~]+", s_norm) is not None
    )

async def import_exception_codes(session: AsyncSession, data: bytes) -> Dict[str, Any]:
    try:
        df = pd.read_excel(BytesIO(data), header=None, dtype=str)
        print(df)
    except Exception as e:
        return {"ok": False, "error": f"Не удалось прочитать Excel: {e}"}

    if df.empty or df.shape[1] < 1:
        return {"ok": False, "error": "Файл пустой или не содержит ни одной колонки."}

    col = df.iloc[:, 0].astype(str)

    col_norm = col.apply(_normalize_code)
    non_empty = col_norm[col_norm != ""]
    if non_empty.empty:
        return {"ok": False, "error": "Первая колонка пуста."}

    first_non_empty = non_empty.iloc[0]
    if not (first_non_empty.startswith(ALLOWED_PREFIXES[0]) or first_non_empty.startswith(ALLOWED_PREFIXES[1])):
        return {"ok": False, "error": "Первая непустая строка не начинается с 01046 или 01029. Файл отклонён."}

    added = 0
    duplicates = 0
    invalid = 0
    seen: set[str] = set()

    for s in non_empty:  # работаем только с непустыми строками
        if not _is_valid_code(s):
            invalid += 1
            continue
        if s in seen:
            continue
        seen.add(s)

        try:
            is_new = await register_code_if_new(session, s)
            if is_new:
                added += 1
            else:
                duplicates += 1
        except Exception:
            invalid += 1

    await session.commit()

    return {
        "ok": True,
        "added": added,
        "duplicates": duplicates,
        "invalid": invalid,
        "total_unique_parsed": len(seen),
    }
