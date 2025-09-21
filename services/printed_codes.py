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