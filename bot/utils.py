from io import BytesIO


async def _download_document_bytes(bot, file_id: str) -> bytes:
    file = await bot.get_file(file_id)
    buf = BytesIO()
    await bot.download(file, buf)
    buf.seek(0)
    return buf.getvalue()