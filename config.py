from dotenv import load_dotenv
import os

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

load_dotenv()

class Config:
    BOT_TOKEN = os.getenv("BOT_TOKEN")


    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_NAME = os.getenv("DB_NAME")

    DATABASE_URL = (
        f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    engine = create_async_engine(
        DATABASE_URL,  # postgresql+asyncpg://...
        pool_pre_ping=True,  # ← проверяет коннект перед использованием (пересоздаст при разрыве)
        pool_recycle=1800,  # ← рецикл коннекта раз в 30 минут (меньше idle-timeout на сервере)
        pool_size=5,
        max_overflow=10,
        echo=True,
    )

    AsyncSessionLocal = async_sessionmaker(
        engine, expire_on_commit=False, class_=AsyncSession
    )

    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN not found in environment variables.")


config = Config()