import os
from io import BytesIO

import pandas as pd
from aiogram import Router, F
from aiogram.types import Message, FSInputFile
from aiogram.filters import Command

import re

from core.pdf_rw import save_pdf_file, build_pdf_from_dataframe, PDF_DIR
from services.access_service import is_user_admin
from services.order_logging import log_orders_from_df
from .utils import _download_document_bytes
from config import config

router = Router()
# ORDER_PATTERN = r"^((?:[\w\-]+/)*\d+):([\w\-.]+):(\d+)$"
REQUIRED_COLS = {"артикул", "размер", "количество"}


@router.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer("Привет! Я бот для работы с кодами заказов.\nОтправь заказ в формате эксель: с заголовками: артикул, размер, количество")


@router.message(
    F.document & (
        (F.document.mime_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet") |
        (F.document.mime_type == "application/vnd.ms-excel") |
        (F.document.file_name.endswith(".xlsx")) |
        (F.document.file_name.endswith(".xls"))
    )
)
async def handle_orders_excel(message: Message):
    try:
        # скачиваем файл
        data = await _download_document_bytes(message.bot, message.document.file_id)
        df = pd.read_excel(BytesIO(data))

        # нормализуем имена колонок
        df.columns = [str(c).strip().lower() for c in df.columns]

        # проверка обязательных колонок
        if not REQUIRED_COLS.issubset(df.columns):
            missing = REQUIRED_COLS - set(df.columns)
            await message.answer(f"❌ В файле не хватает колонок: {', '.join(missing)}")
            return

        await message.answer("✅ В файле есть все нужные колонки: артикул, размер, количество.")

        # вызываем сборку итогового PDF
        result_path, shortages_report = build_pdf_from_dataframe(df, PDF_DIR / "result.pdf")

        try:
            inserted = await log_orders_from_df(df, shortages_report, message.from_user.id)
        except Exception as e:
            # логируем, но не ломаем основной поток
            print(f"⚠️ Ошибка логирования заказов: {e}")

        if not result_path:
            msg = "⚠️ Не удалось собрать итоговый PDF: нет совпадений по артикулам/размерам."
            if shortages_report:
                msg += f"\n\n{shortages_report}"
            await message.answer(msg)
            return

        await message.answer_document(FSInputFile(result_path, filename="result.pdf"))


        if shortages_report:
            await message.answer(shortages_report)

        try:
            os.remove(result_path)
        except Exception as e:
            print(f"⚠️ Не удалось удалить {result_path}: {e}")

    except Exception as e:
        await message.answer(f"⚠️ Ошибка при обработке Excel: {e}")


@router.message(
    F.document & (F.document.mime_type == "application/pdf")
)
async def handle_pdf(message: Message):
    user_id = message.from_user.id
    document = message.document

#TODO: пока коментируем проверку прав
    # async with config.AsyncSessionLocal() as session:  # открываем сессию вручную
    #     if not await is_user_admin(session, user_id):
    #         await message.answer("⛔️ У вас нет прав отправлять PDF.")
    #         return

    # если дошли сюда — это админ
    await message.answer("✅ PDF принят.")
    data = await _download_document_bytes(message.bot, document.file_id)

    saved_path = await save_pdf_file(data, document.file_name, user_id)

    await message.answer(f"Файл сохранен: {saved_path}")
