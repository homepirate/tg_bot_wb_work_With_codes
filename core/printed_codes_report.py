# core/printed_codes_report.py
from io import BytesIO
from datetime import datetime

import pandas as pd
from sqlalchemy import text

from config import config
from services.printed_codes import get_all_codes


async def build_printed_codes_report_excel_bytes() -> tuple[bytes, str]:
    """
    Формирует Excel-файл со ВСЕМИ записями из таблицы printed_codes.
    Возвращает: (байты_файла, имя_файла).
    """
    codes = set()
    async with config.AsyncSessionLocal() as session:
        codes: set[str] = await get_all_codes(session)

    # делаем DataFrame с одной колонкой
    df = pd.DataFrame(sorted(codes), columns=["code"])

    buf = BytesIO()
    with pd.ExcelWriter(buf) as writer:
        df.to_excel(writer, index=False, sheet_name="codes")

    buf.seek(0)
    filename = f"printed_codes_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.xlsx"
    return buf.getvalue(), filename