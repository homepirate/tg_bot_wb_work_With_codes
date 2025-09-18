from typing import Callable, Awaitable, Dict, Any
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, Message, CallbackQuery
from sqlalchemy.ext.asyncio import async_sessionmaker

from services.access_service import is_user_allowed


class DBAccessControlMiddleware(BaseMiddleware):
    def __init__(self, session_factory: async_sessionmaker):
        self.session_factory = session_factory

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        user = data.get("event_from_user")

        # Если нет пользователя (например, service updates) — пропускаем дальше
        if user is None:
            return await handler(event, data)

        # Проверка доступа через короткоживущую сессию (без глобальной)
        try:
            async with self.session_factory() as session:
                allowed = await is_user_allowed(session, user.id)
        except Exception as e:
            # Не даём упасть пайплайну, логируем и показываем аккуратное сообщение
            print(f"[DBAccessControl] DB error while checking access for user {user.id}: {e}")
            allowed = False

        if not allowed:
            if isinstance(event, CallbackQuery):
                try:
                    await event.answer()  # закрыть «крутилку»
                except Exception:
                    pass
                if event.message:
                    await event.message.answer("⛔️ У вас нет доступа.")
            elif isinstance(event, Message):
                await event.answer("⛔️ У вас нет доступа.")
            else:
                print(f"⛔️ Пользователь {user.id} не имеет доступа, type={type(event)}")
            return  # блокируем обработку дальше

        # доступ разрешён — продолжаем цепочку
        return await handler(event, data)
