from __future__ import annotations
import re
from pathlib import Path

# Папка с PDF (без сайд-эффекта mkdir — создавайте там, где реально пишете файлы)
PDF_DIR = Path("pdf-codes")
PDF_DIR.mkdir(exist_ok=True)


# ==============================
# Общие регулярки / токены
# ==============================
RE_GTIN            = re.compile(r"^0\d{13,}$")
RE_SERIAL          = re.compile(r"^[\x20-\x7E]{4,}$")
RE_ASCII_PREFIX    = re.compile(r"^([\x21-\x7E]{4,})")
RE_ASCII_PREFIX_LINE = re.compile(r"^\s*([!-~]{4,})")

# ==============================
# GS1 коды
# ==============================
# (01)<14>(21)<ASCII...> — в одну строку (со скобками)
RE_GS1_PAREN_ONELINE = re.compile(
    r"\(\s*01\s*\)\s*\d{14}\s*\(\s*21\s*\)\s*[!-~]{4,}",
    re.IGNORECASE,
)
# 01<14>21 — «голова» на своей строке (без скобок)
RE_GS1_NOPAREN_HEADLINE = re.compile(
    r"^\s*01\s*\d{14}\s*21\s*$",
    re.IGNORECASE,
)

# ==============================
# Артикул / Цвет / Лейблы
# ==============================
RE_ART = re.compile(
    r"Артикул\s*[:\-]?\s*(.+?)(?=(?:\s*Цвет\s*:|\s*Размер\s*:|$))",
    re.IGNORECASE,
)
RE_ART_ALT1     = re.compile(r"арт\.\s*([A-Z0-9_]+/\S+)", re.IGNORECASE)
RE_ART_ALT2     = re.compile(r"\b([A-Z0-9_]+/[A-Za-zА-Яа-я0-9_\-]+)\b", re.IGNORECASE)
RE_COLOR        = re.compile(r"Цвет:\s*([^\r\n]+)", re.IGNORECASE)
RE_NAME_COLOR   = re.compile(r"Балаклава\s+(.+?)\s+р\.", re.IGNORECASE | re.DOTALL)
RE_COLOR_TOKEN  = re.compile(r"Цвет", re.IGNORECASE)

# ==============================
# Размеры
# ==============================
RE_SIZE_LABEL   = re.compile(r"Размер:\s*([^\r\n]+)", re.IGNORECASE)
RE_SIZE_NUMERIC = re.compile(r"\b\d{2}(?:[–\-\/]\d{2})?\b")

# Буквенные: цифра допускается только перед XS/XL/XXL/XXXL (не перед одиночным L/S/M)
RE_SIZE_ALPHA = re.compile(
    r"""
    \b(
        (?:XS|S|M|L|XL|XXL|XXXL)
        |
        (?:[2-5](?:XS|XL|XXL|XXXL))
    )
    (?:[\/\-–]
        (?:XS|S|M|L|XL|XXL|XXXL|[2-5](?:XS|XL|XXL|XXXL))
    )?
    \b
    """,
    re.IGNORECASE | re.VERBOSE,
)

SIZE_WORDS   = {"ONE SIZE", "ONESIZE", "UNI", "UNISIZE", "UNIVERSAL", "УНИВЕРСАЛЬНЫЙ", "ЕДИНЫЙ РАЗМЕР", "ДЕТСКИЙ", "ПОДРОСТКОВЫЙ"}
RE_SIZE_WORD = re.compile(r"\b[A-Za-zА-Яа-яЁё\- ]{3,}\b", re.IGNORECASE)



