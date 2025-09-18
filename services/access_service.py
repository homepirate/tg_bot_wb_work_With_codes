from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import UserRole
from models.allowed_user import AllowedUser

async def is_user_allowed(session: AsyncSession, user_id: int) -> bool:
    """
    Проверяет, есть ли пользователь в таблице allowed_users.
    """
    result = await session.execute(
        select(AllowedUser.user_id).where(AllowedUser.user_id == user_id)
    )
    return result.scalar_one_or_none() is not None



async def get_user_role(session: AsyncSession, user_id: int) -> UserRole | None:
    """
    Возвращает роль пользователя или None, если его нет в таблице.
    """
    result = await session.execute(
        select(AllowedUser.role).where(AllowedUser.user_id == user_id)
    )
    return result.scalar_one_or_none()


async def is_user_admin(session: AsyncSession, user_id: int) -> bool:
    """
    Проверяет, является ли пользователь админом.
    """
    role = await get_user_role(session, user_id)
    return role == UserRole.ADMIN