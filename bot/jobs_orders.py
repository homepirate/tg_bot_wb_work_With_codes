# bot/jobs_orders.py
import io
from pathlib import Path
from typing import Any

import pandas as pd
from aiogram.types import BufferedInputFile, FSInputFile
from aiogram import Bot

from .utils import build_shortages_excel_bytes
from core.patterns import PDF_DIR
from core.pdf_rw import build_pdf_from_dataframe


async def process_orders_job(payload: dict[str, Any]):
    """
    payload = {
      "chat_id": int,
      "progress_msg_id": int,
      "df_bytes": bytes,   # Excel-файл как bytes (чтобы не держать pandas в handler)
      "filename": str | None
    }
    """
    chat_id = payload["chat_id"]
    msg_id  = payload["progress_msg_id"]
    df_bytes = payload["df_bytes"]
    filename = payload.get("filename") or "orders.xlsx"
    bot: Bot = payload["bot"]   # передаём bot в payload

    # Парсим DF в воркере
    df = pd.read_excel(io.BytesIO(df_bytes))
    df.columns = [str(c).strip().lower() for c in df.columns]

    # Обновим статус
    try:
        await bot.edit_message_text(
            chat_id=chat_id, message_id=msg_id,
            text="🔧 Обработка заказа: парсинг и подготовка…"
        )
    except Exception:
        pass

    # Сборка PDF
    result_path, shortages_report = await build_pdf_from_dataframe(df, PDF_DIR / "result.pdf")

    # Недостачи → Excel
    if shortages_report:
        try:
            xls_bytes, xls_name = await build_shortages_excel_bytes(shortages_report)
            await bot.send_document(
                chat_id=chat_id,
                document=BufferedInputFile(xls_bytes, filename=xls_name),
                caption="📉 Недостачи по позициям"
            )
        except Exception:
            # не роняем
            pass

    # Итог: PDF или сообщение что нет совпадений
    if not result_path:
        msg = "⚠️ Не удалось собрать итоговый PDF: нет совпадений по артикулам/размерам."
        if shortages_report:
            msg += f"\n\n{shortages_report}"
        await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=msg)
        return

    await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text="📦 Отправляю результат…")
    await send_pdf_safely_for_bot(bot, chat_id, result_path, filename="result.pdf")

    # финал
    try:
        Path(result_path).unlink(missing_ok=True)
    except Exception:
        pass
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text="✅ Готово.")
    except Exception:
        pass


async def send_pdf_safely_for_bot(bot: Bot, chat_id: int, pdf_path: Path | str, *, filename: str | None = None) -> None:
    """Тот же send_pdf_safely, но без Message — пригоден для фонового воркера."""
    from PyPDF2 import PdfReader, PdfWriter
    import zipfile, os

    TG_MAX_UPLOAD = 49 * 1024 * 1024
    p = Path(pdf_path)
    if not p.exists():
        await bot.send_message(chat_id, "⚠️ Файл для отправки не найден.")
        return

    show_name = filename or p.name
    size = p.stat().st_size

    if size <= TG_MAX_UPLOAD:
        await bot.send_document(chat_id, FSInputFile(p, filename=show_name))
        return

    # ZIP попытка
    zip_path = p.with_suffix(".zip")
    try:
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
            z.write(p, arcname=show_name)
        if zip_path.stat().st_size <= TG_MAX_UPLOAD:
            await bot.send_document(chat_id, FSInputFile(zip_path, filename=zip_path.name),
                                    caption="Файл превышал лимит, отправлен в ZIP.")
            try:
                zip_path.unlink(missing_ok=True)
            except Exception:
                pass
            return
    except Exception:
        try:
            zip_path.unlink(missing_ok=True)
        except Exception:
            pass

    # Резка по страницам
    try:
        reader = PdfReader(str(p))
    except Exception as e:
        await bot.send_message(chat_id, f"⚠️ Не удалось открыть PDF: {e}")
        return

    total_pages = len(reader.pages)
    if total_pages == 0:
        await bot.send_message(chat_id, "⚠️ PDF пустой.")
        return

    approx_pages = max(1, int(total_pages * (TG_MAX_UPLOAD / max(1, size))))
    part_idx = 1
    start = 0
    while start < total_pages:
        end = min(total_pages, start + approx_pages)
        writer = PdfWriter()
        for i in range(start, end):
            writer.add_page(reader.pages[i])

        part_path = p.with_name(f"{p.stem}__part{part_idx}.pdf")
        with open(part_path, "wb") as f:
            writer.write(f)

        while part_path.stat().st_size > TG_MAX_UPLOAD and (end - start) > 1:
            end = start + max(1, (end - start) // 2)
            try:
                part_path.unlink(missing_ok=True)
            except Exception:
                pass
            writer = PdfWriter()
            for i in range(start, end):
                writer.add_page(reader.pages[i])
            with open(part_path, "wb") as f:
                writer.write(f)

        if part_path.stat().st_size > TG_MAX_UPLOAD and (end - start) == 1:
            try:
                part_path.unlink(missing_ok=True)
            except Exception:
                pass
            await bot.send_message(
                chat_id,
                "⚠️ Даже одна страница превышает лимит Telegram. Уменьшите качество/размер PDF."
            )
            return

        await bot.send_document(chat_id, FSInputFile(part_path, filename=part_path.name),
                                caption=f"Часть {part_idx}")
        try:
            part_path.unlink(missing_ok=True)
        except Exception:
            pass

        start = end
        part_idx += 1
