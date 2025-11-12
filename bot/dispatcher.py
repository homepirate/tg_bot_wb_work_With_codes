from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.memory import MemoryStorage

from config import Config, config
from db_access_control import DBAccessControlMiddleware
from .handlers import router as handlers_router


from bot.job_queue import configure as jq_configure, start as jq_start, stop as jq_stop
from bot.jobs_orders import process_orders_job


async def _on_startup(bot: Bot, **_):
    jq_configure(process_orders_job, concurrency=2)
    await jq_start()


async def _on_shutdown(bot: Bot, **_):
    await jq_stop()

async def start_bot():
    bot = Bot(
        token=Config.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher(storage=MemoryStorage())

    dp.message.outer_middleware(DBAccessControlMiddleware(config.AsyncSessionLocal))

    dp.include_router(handlers_router)

    dp.startup.register(_on_startup)
    dp.shutdown.register(_on_shutdown)


    print("Бот запущен...")

    await dp.start_polling(bot)

