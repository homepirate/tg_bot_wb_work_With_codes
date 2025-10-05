import re
from io import BytesIO


async def _download_document_bytes(bot, file_id: str) -> bytes:
    file = await bot.get_file(file_id)
    buf = BytesIO()
    await bot.download(file, buf)
    buf.seek(0)
    return buf.getvalue()


SAFE_NAME_RE = re.compile(r"[^a-zA-Z0-9_.\-а-яА-ЯёЁ]")

def _safe_filename(name: str, fallback: str = "file") -> str:
    name = name or fallback
    name = SAFE_NAME_RE.sub("_", name)
    return name[:128]