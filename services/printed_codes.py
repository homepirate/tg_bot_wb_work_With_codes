from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from models.printed_code import PrintedCode


async def code_exists(session: AsyncSession, code: str) -> bool:
    """Проверка наличия кода (если прям нужно где-то отдельно)."""
    res = await session.execute(
        select(PrintedCode.code).where(PrintedCode.code == code)
    )
    return res.scalar_one_or_none() is not None



async def register_code_if_new(session: AsyncSession, code: str) -> bool:
    """
    True -> код действительно НОВЫЙ и добавлен;
    False -> код уже был.
    """
    stmt = (
        pg_insert(PrintedCode)
        .values(code=code)
        .on_conflict_do_nothing(index_elements=["code"])
        .returning(PrintedCode.code)
    )
    res = await session.execute(stmt)
    return res.scalar_one_or_none() is not None


async def get_all_codes(session: AsyncSession) -> set[str]:
    res = await session.execute(select(PrintedCode.code))
    return {row[0] for row in res.fetchall() if row[0]}



async def bulk_register_codes(session: AsyncSession, codes: set[str]) -> int:
    """
    Массовая регистрация кодов. Возвращает, сколько реально вставлено.
    """
    if not codes:
        return 0
    stmt = (
        pg_insert(PrintedCode)
        .values([{"code": c} for c in codes])
        .on_conflict_do_nothing(index_elements=["code"])
        .returning(PrintedCode.code)
    )
    res = await session.execute(stmt)
    # сколько вернулось — столько реально вставилось
    inserted = [row[0] for row in res.fetchall()]
    return len(inserted)