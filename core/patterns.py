import re
from pathlib import Path

# Директория с PDF
PDF_DIR = Path("pdf-codes")
PDF_DIR.mkdir(exist_ok=True)

# ==== GS1 блоки, которые часто «прилипают» к цвету/размеру ====
GS1_01 = re.compile(r"\(01\)\s*\d{14}", re.IGNORECASE)
GS1_21 = re.compile(r"\(21\)\s*[!-~]{4,}", re.IGNORECASE)

# Стоп-маркеры конца значения «Цвет»
COLOR_STOP = re.compile(
    r"\s*(?:Артикул|Размер|$|\(01\)|\(21\)|\b\d{1,3}(?:\s*[-/]\s*\d{1,3})?\b).*",
    re.IGNORECASE,
)

# ==== Общие регулярки / токены ====
RE_GTIN              = re.compile(r"^0\d{13,}$")
RE_SERIAL            = re.compile(r"^[\x20-\x7E]{4,}$")
RE_ASCII_PREFIX      = re.compile(r"^([\x21-\x7E]{4,})")
RE_ASCII_PREFIX_LINE = re.compile(r"^\s*([!-~]{4,})")
RE_ASCII_ANY = re.compile(r"[!-~]{4,}")


SERIAL_MIN = 9
SERIAL_MAX = 13

RE_ASCII_RUN = re.compile(r"[!-~]{%d,}" % SERIAL_MIN)

RE_GS1_NOPAREN_ANY = re.compile(r"01\s*\d{14}\s*21", re.IGNORECASE)

# GS1 линии
RE_GS1_PAREN_ONELINE    = re.compile(r"\(\s*01\s*\)\s*\d{14}\s*\(\s*21\s*\)\s*[!-~]{4,}", re.IGNORECASE)
RE_GS1_NOPAREN_HEADLINE = re.compile(r"^\s*01\s*\d{14}\s*21\s*$", re.IGNORECASE)

# Артикул / Цвет / Лейблы
RE_ART        = re.compile(r"Артикул\s*[:\-]?\s*(.+?)(?=(?:\s*Цвет\s*:|\s*Размер\s*:|$))", re.IGNORECASE)
RE_ART_ALT1   = re.compile(r"арт\.\s*([A-Z0-9_]+/\S+)", re.IGNORECASE)
RE_ART_ALT2   = re.compile(r"\b([A-Z0-9_]+/[A-Za-zА-Яа-я0-9_\-]+)\b", re.IGNORECASE)
RE_COLOR      = re.compile(r"Цвет:\s*([^\r\n]+)", re.IGNORECASE)
RE_NAME_COLOR = re.compile(
    r"(?:Балаклава|Манишка|Шапка(?:-[^\s]+)?|Перчатки|Варежки|Снуд|Капор|Полумаска|Бафф|Шарф|Косынка)\s+([A-Za-zА-Яа-яЁё\- ]+?)\s+р\.",
    re.IGNORECASE | re.DOTALL,
)
RE_COLOR_DASH_LINE = re.compile(r"^[\-\—]\s*([A-Za-zА-Яа-яЁё\- ]{2,})\s*$", re.IGNORECASE | re.MULTILINE)
RE_COLOR_TOKEN     = re.compile(r"Цвет", re.IGNORECASE)

# Размеры
RE_SIZE_LABEL   = re.compile(r"Размер:\s*([^\r\n]+)", re.IGNORECASE)
RE_SIZE_NUMERIC = re.compile(r"\b\d{1,3}\s*(?:[–\-\/]\s*\d{1,3})?\b")  # 56, 56-60, 58/60, 110 и пр.
RE_SIZE_ALPHA = re.compile(
    r"""
    \b(
        (?:XS|S|M|L|XL|XXL|XXXL)                               # одиночные буквенные
        |
        (?:(?:[2-9]|1[0-9])(?:XS|XL|XXL|XXXL))                 # 2XL..19XL (и 2XS..19XS на всякий)
    )
    (?:[\/\-–]
        (?:
            (?:XS|S|M|L|XL|XXL|XXXL)
            |
            (?:(?:[2-9]|1[0-9])(?:XS|XL|XXL|XXXL))
        )
    )?
    \b
    """,
    re.IGNORECASE | re.VERBOSE,
)
SIZE_WORDS   = {
    "ONE SIZE","ONESIZE","UNI","UNISIZE","UNIVERSAL",
    "УНИВЕРСАЛЬНЫЙ","ЕДИНЫЙ РАЗМЕР","ДЕТСКИЙ","ПОДРОСТКОВЫЙ"
}
RE_SIZE_WORD = re.compile(r"\b[A-Za-zА-Яа-яЁё\- ]{3,}\b", re.IGNORECASE)
