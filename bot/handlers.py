import os
from io import BytesIO
from pathlib import Path

import pandas as pd
from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, FSInputFile, BufferedInputFile
from aiogram.filters import Command

import re

from core.pdf_report_builder import build_inventory_report_excel_bytes
from core.pdf_rw import build_pdf_from_dataframe, PDF_DIR
from core.pdf_splitter import split_pdf_by_meta, _save_temp_pdf
# from core.return_from_photo import return_by_photo
from core.return_pdf import return_pdf
from services.access_service import is_user_admin
from services.order_logging import log_orders_from_df
from .keyboards import main_kb
from .states import ReturnCode
from .utils import _download_document_bytes, _safe_filename
from config import config

router = Router()
REQUIRED_COLS = {"артикул", "размер", "количество"}


@router.message(Command("id"))
async def get_id(message: Message):
    await message.answer(f"{message.from_user.id}")

@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()

    await message.answer(
        "Привет! Я бот для работы с кодами заказов.\n"
        "Отправь заказ в формате эксель: с заголовками: артикул, размер, количество\n"
        "Сформировать отчет — /report",
        reply_markup=main_kb(),
    )


@router.message(F.text == "Вернуть код")
async def on_return_code(message: Message, state: FSMContext):
    await state.set_state(ReturnCode.waiting_for_file)
    await message.answer(
        "Пришлите **PDF** или **фото** с заказом.\n"
        "После получения обработаю файл и верну код.",
        reply_markup=main_kb(),
    )


@router.message(ReturnCode.waiting_for_file, F.document)
async def on_pdf_from_state(message: Message, state: FSMContext):
    doc = message.document
    is_pdf = (doc.mime_type == "application/pdf") or (doc.file_name and doc.file_name.lower().endswith(".pdf"))
    if not is_pdf:
        await message.answer("Нужен PDF-файл (или пришлите фото). Попробуйте ещё раз.")
        return

    filename = _safe_filename(doc.file_name or "order.pdf")
    dest_dir = Path("pdf-codes") / "tmp"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / filename

    # Скачиваем файл
    await message.bot.download(doc, destination=dest_path)

    await message.answer(f"PDF получен: `{dest_path}`\nОбрабатываю…", parse_mode="Markdown")
    async with config.AsyncSessionLocal() as session:
        result = await return_pdf(session, dest_path)

    summary = (
        f"Найдено кодов: {len(result['codes'])}\n"
        f"Удалено из БД: {len(result['deleted_codes'])}\n"
        f"Сохранено файлов: {len(result['saved'])}"
    )

    await message.answer(summary)


    # Выходим из состояния (или оставьте состояние, если ждёте ещё файлы)
    await state.clear()


@router.message(Command("report"))
async def generate_report(message: Message):
    try:
        data, filename = await build_inventory_report_excel_bytes()
        await message.answer_document(
            BufferedInputFile(data, filename=filename),
            caption="Отчёт готов."
        )
    except Exception as e:
        await message.answer(f"Не удалось сформировать отчёт: {e}")

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
        result_path, shortages_report = await build_pdf_from_dataframe(df, PDF_DIR / "result.pdf")

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

    async with config.AsyncSessionLocal() as session:  # открываем сессию вручную
        if not await is_user_admin(session, user_id):
            await message.answer("⛔️ У вас нет прав отправлять PDF.")
            return

    # если дошли сюда — это админ
    await message.answer("✅ PDF принят. Разделяю по (артикул, размер, цвет)…")

    data = await _download_document_bytes(message.bot, document.file_id)
    src_tmp_path = await _save_temp_pdf(data, document.file_name, user_id)

    try:
        report = split_pdf_by_meta(src_tmp_path)

        if not report["outputs"]:
            msg = (
                "Готово. Но ни одного файла собрать не удалось.\n"
                f"• Всего страниц: {report['total_pages']}\n"
                f"• Пропущено без метаданных: {report['skipped_without_meta']}\n"
                f"Проверь, что на страницах есть «Артикул …», «Размер: …», «Цвет: …»."
            )
            await message.answer(msg)
            return

        lines = [
            "📄 Готово! Сохранены файлы:",
            *(f"• {o['path'].name} — {o['pages']} стр.  [{o['key'][0]} | {o['key'][1]} | {o['key'][2]}]"
              for o in report["outputs"]),
            "",
            f"Пропущено без метаданных: {report['skipped_without_meta']}",
        ]
        await message.answer("\n".join(lines))
    finally:
        try:
            src_tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        # Если временная папка опустела — можно подчистить
        tmp_dir = src_tmp_path.parent
        try:
            if tmp_dir.exists() and not any(tmp_dir.iterdir()):
                tmp_dir.rmdir()
        except Exception:
            pass