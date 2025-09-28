import re
from collections import defaultdict
from typing import Optional, Dict, List, Tuple

import pandas as pd
from sqlalchemy import insert

from config import config
from models import OrderLog
from services.access_service import is_user_allowed

REQUIRED_COLS = {"артикул", "размер", "количество"}


_RE_RU = re.compile(r"^\s*(?P<art>.+?)\s*-\s*размер:\s*(?P<size>\d+)\s*,\s*не хватило:\s*(?P<n>\d+)\s*$")

def _parse_shortages_report(report: Optional[str]) -> Dict[Tuple[str, str], List[int]]:
    """
    Возвращает карту {(art, size_str): [n1, n2, ...]}.
    Поддерживает строки:
      - "AAA:42 не хватило: 2"
      - "AAA - размер: 42, не хватило: 2"
    """
    out: Dict[Tuple[str, str], List[int]] = defaultdict(list)
    if not report:
        return out

    for raw in report.splitlines():
        line = raw.strip()
        if not line:
            continue
        m = _RE_RU.match(line)
        if not m:
            continue
        art = m.group("art").strip()
        size = str(m.group("size")).strip()   # ключ всегда строкой
        n = int(m.group("n"))
        out[(art, size)].append(n)

    return out

async def log_orders_from_df(df: pd.DataFrame, shortages_report: Optional[str], user_id: int) -> int:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    if not REQUIRED_COLS.issubset(set(df.columns)):
        missing = REQUIRED_COLS - set(df.columns)
        raise ValueError(f"В df нет обязательных колонок: {', '.join(missing)}")

    shortages_map = _parse_shortages_report(shortages_report)

    rows = []
    for _, row in df.iterrows():
        art = str(row["артикул"]).strip()
        size_str = str(row["размер"]).strip()
        try:
            qty_req = int(row["количество"])
        except Exception:
            continue


        short = int(shortages_map[(art, size_str)].pop(0)) if shortages_map.get((art, size_str)) else 0
        qty_sent = max(qty_req - short, 0)

        rows.append({
            "user_id": user_id,       # временно, обновим ниже
            "article": art,
            "size": size_str,
            "qty_requested": qty_req,
            "qty_sent": qty_sent,
            "shortage": short,
        })

    if not rows:
        return 0

    async with config.AsyncSessionLocal() as session:
        allowed = await is_user_allowed(session, user_id)  # <-- bool
        if not allowed:
            for r in rows:
                r["user_id"] = None
        else:
            for r in rows:
                r["user_id"] = user_id

        await session.execute(insert(OrderLog).values(rows))
        await session.commit()

    return len(rows)
