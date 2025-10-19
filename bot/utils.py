import re
from typing import Iterable, Optional
from aiogram.types.input_file import BufferedInputFile
import io

import zipfile
from pathlib import Path

from aiogram.types import Message, FSInputFile
from PyPDF2 import PdfReader, PdfWriter

# Лимит загрузки файлов ботом ~50 МБ; оставим запас
TG_MAX_UPLOAD = 49 * 1024 * 1024
TG_TEXT_LIMIT = 4096
SAFE_CHUNK = 4000  # небольшой запас, чтобы не упереться в лимит с форматированием


async def _download_document_bytes(bot, file_id: str) -> bytes:
    file = await bot.get_file(file_id)
    buf = io.BytesIO()
    await bot.download(file, buf)
    buf.seek(0)
    return buf.getvalue()


SAFE_NAME_RE = re.compile(r"[^a-zA-Z0-9_.\-а-яА-ЯёЁ]")

def _safe_filename(name: str, fallback: str = "file") -> str:
    name = name or fallback
    name = SAFE_NAME_RE.sub("_", name)
    return name[:128]


def _chunk_lines(lines: Iterable[str], limit: int = SAFE_CHUNK):
    """Бьём по строкам, чтобы куски не превышали limit символов."""
    buf = []
    size = 0
    for ln in lines:
        # +1 за '\n' при join
        add = len(ln) + (1 if buf else 0)
        if size + add > limit and buf:
            yield "\n".join(buf)
            buf = [ln]
            size = len(ln)
        else:
            buf.append(ln)
            size += add
    if buf:
        yield "\n".join(buf)

async def answer_long(
    message: Message,
    text: str,
    *,
    chunk_limit: int = SAFE_CHUNK,
    parse_mode: Optional[str] = None,
    disable_web_page_preview: bool = True,
    as_file_threshold: int = 100_000,  # если >100k символов — лучше файлом
) -> None:
    if not text:
        return

    if len(text) > as_file_threshold:
        bio = io.BytesIO(text.encode("utf-8"))
        await message.answer_document(
            BufferedInputFile(bio.getvalue(), filename="result.txt"),
            caption="См. содержимое в приложенном файле.",
        )
        return

    if len(text) <= chunk_limit:
        await message.answer(
            text,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
        )
        return

    # Разбивка по строкам — чтобы не рвать слова/форматирование
    for part in _chunk_lines(text.splitlines(), limit=chunk_limit):
        await message.answer(
            part,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
        )



async def send_pdf_safely(message: Message, pdf_path: Path | str, *, filename: str | None = None) -> None:
    """
    Отправляет PDF с учётом лимита Telegram Bot API (~50MB для upload):
      1) если помещается — отправляет как есть;
      2) иначе пробует заZIPовать;
      3) если всё ещё велик — режет PDF на части и отправляет по очереди.

    :param message: aiogram Message
    :param pdf_path: путь к исходному PDF
    :param filename: имя файла, под которым показать пользователю (опционально)
    """
    p = Path(pdf_path)
    if not p.exists():
        await message.answer("⚠️ Файл для отправки не найден.")
        return

    show_name = filename or p.name
    size = p.stat().st_size

    # 1) Влезает — шлём сразу
    if size <= TG_MAX_UPLOAD:
        await message.answer_document(FSInputFile(p, filename=show_name))
        return

    # 2) Пробуем ZIP
    zip_path = p.with_suffix(".zip")
    try:
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
            z.write(p, arcname=show_name)
        if zip_path.stat().st_size <= TG_MAX_UPLOAD:
            await message.answer_document(
                FSInputFile(zip_path, filename=zip_path.name),
                caption="Файл превышал лимит, отправлен в ZIP."
            )
            try:
                zip_path.unlink(missing_ok=True)
            except Exception:
                pass
            return
    except Exception:
        # если упаковка не удалась — перейдём к разбиению
        try:
            zip_path.unlink(missing_ok=True)
        except Exception:
            pass

    # 3) Режем на части по страницам
    try:
        reader = PdfReader(str(p))
    except Exception as e:
        await message.answer(f"⚠️ Не удалось открыть PDF: {e}")
        return

    total_pages = len(reader.pages)
    if total_pages == 0:
        await message.answer("⚠️ PDF пустой.")
        return

    # Прикидка количества страниц на часть по доле размера
    # (потом при необходимости уменьшим в цикле)
    approx_pages = max(1, int(total_pages * (TG_MAX_UPLOAD / max(1, size))))

    part_idx = 1
    start = 0
    while start < total_pages:
        end = min(total_pages, start + approx_pages)

        # Собираем кусок
        writer = PdfWriter()
        for i in range(start, end):
            writer.add_page(reader.pages[i])

        part_path = p.with_name(f"{p.stem}__part{part_idx}.pdf")
        with open(part_path, "wb") as f:
            writer.write(f)

        # Если часть всё ещё крупнее лимита — уменьшаем окно (делим пополам) пока не влезет
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

        # Если даже 1 страница не влезает — сообщаем
        if part_path.stat().st_size > TG_MAX_UPLOAD and (end - start) == 1:
            try:
                part_path.unlink(missing_ok=True)
            except Exception:
                pass
            await message.answer(
                "⚠️ Даже одна страница превышает лимит Telegram для ботов. "
                "Уменьшите качество/размер PDF (DPI/сжатие) или отправьте ссылкой."
            )
            return

        # Отправляем часть и чистим временный файл
        await message.answer_document(
            FSInputFile(part_path, filename=part_path.name),
            caption=f"Часть {part_idx}"
        )
        try:
            part_path.unlink(missing_ok=True)
        except Exception:
            pass

        start = end
        part_idx += 1
