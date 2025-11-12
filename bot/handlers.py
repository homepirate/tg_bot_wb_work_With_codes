import os
from io import BytesIO
from pathlib import Path

import pandas as pd
from aiogram import Router, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, FSInputFile, BufferedInputFile
from aiogram.filters import Command

import re

from core.exception_codes_import import import_exception_codes
from core.pdf_cleanup import purge_known_codes_in_dir
from core.pdf_report_builder import build_inventory_report_excel_bytes
from core.pdf_rw import build_pdf_from_dataframe, PDF_DIR
from core.pdf_splitter import split_pdf_by_meta, _save_temp_pdf
# from core.return_from_photo import return_by_photo
from core.return_pdf import return_pdf
from services.access_service import is_user_admin
from services.order_logging import log_orders_from_df
from .keyboards import main_kb
from .states import ReturnCode, ImportExceptions
from .utils import _download_document_bytes, _safe_filename, answer_long, send_pdf_safely, FileTooBigError, \
    build_shortages_excel_bytes
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
        "Сформировать отчет — /report\nЗапустить проверку и очистку использованных кодов - /cleanup",
        reply_markup=main_kb(),
    )


@router.message(F.text == "Вернуть код")
async def on_return_code(message: Message, state: FSMContext):
    await state.set_state(ReturnCode.waiting_for_file)
    await message.answer(
        "Пришлите **PDF** с заказом.\n"
        "После получения обработаю файл и верну код.",
        reply_markup=main_kb(),
    )

@router.message(F.text == "Добавить коды в таблицу исключкений")
async def on_add_exceptions_click(message: Message, state: FSMContext):
    await state.set_state(ImportExceptions.waiting_for_excel)
    await message.answer(
        "Пришлите Excel (.xlsx/.xls) с кодами.\n"
        "Важно: первая строка файла должна содержать префикс 01046 или 01029.\n"
        "Коды будут добавлены в таблицу исключений.",
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


@router.message(
    ImportExceptions.waiting_for_excel,
    F.document & (
        (F.document.mime_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet") |
        (F.document.mime_type == "application/vnd.ms-excel") |
        (F.document.file_name.endswith(".xlsx")) |
        (F.document.file_name.endswith(".xls"))
    )
)
async def on_exceptions_excel(message: Message, state: FSMContext):
    user_id = message.from_user.id

    async with config.AsyncSessionLocal() as session:  # открываем сессию вручную
        if not await is_user_admin(session, user_id):
            await message.answer("⛔️ У вас нет прав отправлять PDF.")
            return

    try:
        data = await _download_document_bytes(message.bot, message.document.file_id)
    except Exception as e:
        await message.answer(f"Не удалось скачать файл: {e}")
        return

    async with config.AsyncSessionLocal() as session:
        report = await import_exception_codes(session, data)

    if not report.get("ok"):
        await message.answer(f"❌ {report.get('error', 'Файл отклонён')}")
        await state.clear()
        return

    report_text_lines = [
        "✅ Импорт завершён.",
        f"Всего уникальных в файле: {report.get('total_unique_parsed', 0)}",
        f"Добавлено новых: {report.get('added', 0)}",
        f"Уже были в БД: {report.get('duplicates', 0)}",
    ]
    invalid = int(report.get("invalid", 0) or 0)
    if invalid:
        report_text_lines.append(f"Проблемных записей: {invalid}")

    msg_text = "\n".join(report_text_lines).strip() or "✅ Импорт завершён."
    await message.answer(msg_text)
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



@router.message(Command("cleanup"))
async def cleanup_codes(message: Message):
    user_id = message.from_user.id
    async with config.AsyncSessionLocal() as session:
        if not await is_user_admin(session, user_id):
            await message.answer("⛔️ У вас нет прав на очистку PDF.")
            return

        await message.answer("🧹 Начинаю очистку PDF от уже известных кодов...")
        stats = await purge_known_codes_in_dir(session)

    summary = (
        f"📂 Файлов просмотрено: {stats['files_scanned']}\n"
        f"✏️  Изменено: {stats['files_modified']}\n"
        f"🗑  Удалено: {stats['files_deleted']}\n"
        f"📄 Страниц просмотрено: {stats['pages_scanned']}\n"
        f"❌ Страниц удалено: {stats['pages_deleted']}"
    )
    await message.answer(summary)

    if stats["details"]:
        await answer_long(message, "Подробности:\n" + "\n".join(stats["details"]))


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

        try:
            if shortages_report:
                xls_bytes, xls_name = await build_shortages_excel_bytes(shortages_report)
                await message.answer_document(
                    BufferedInputFile(xls_bytes, filename=xls_name),
                    caption="📉 Недостачи по позициям"
                )
        except Exception as e:
            print(f"⚠️ Не удалось собрать Excel с недостачами: {e}", flush=True)


        if not result_path:
            msg = "⚠️ Не удалось собрать итоговый PDF: нет совпадений по артикулам/размерам."
            if shortages_report:
                msg += f"\n\n{shortages_report}"
            await message.answer(msg)
            return

        await send_pdf_safely(message, result_path, filename="result.pdf")

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
    try:
        data = await _download_document_bytes(message.bot, message.document.file_id)
    except FileTooBigError:
        await message.answer(
            "⚠️ Файл слишком большой для скачивания ботом (>\u00A020 MB). "
            "Разбейте на части."
        )
        return
    except TelegramBadRequest as e:
        await message.answer(f"Не удалось получить файл: {e}")
        return

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

        text = "\n".join(lines)
        await answer_long(message, text)
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