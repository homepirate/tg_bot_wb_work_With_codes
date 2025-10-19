import re
from typing import Iterable, Optional
from aiogram.types import Message
from aiogram.types.input_file import BufferedInputFile
import io

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
