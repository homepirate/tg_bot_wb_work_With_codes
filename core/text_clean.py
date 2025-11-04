# core/text_clean.py
import re
from .patterns import GS1_01, GS1_21, COLOR_STOP

# эвристики против мусорных латинских хвостов
_MIXED_LATIN = re.compile(r"(?=.*[A-Z])(?=.*[a-z])[A-Za-z]+")   # clSV, rAu, YoN, и т.п.
_PURE_LATIN  = re.compile(r"^[A-Za-z]+$")                       # сплошь латиница
_ONLY_CYR    = re.compile(r"^[А-Яа-яЁё\-]+$")                    # «черный», «молочный», «сине-зелёный»

def normalize_dashes(text: str) -> str:
    return (text or "").replace("–", "-").replace("—", "-")

def strip_gs1(text: str) -> str:
    t = GS1_01.sub(" ", text or "")
    t = GS1_21.sub(" ", t)
    return t

def clean_for_parsing(raw: str) -> str:
    """Подготовка текста: склейка переносов внутри слов и перед '/', небольшая 'разлепка' лейблов."""
    t = raw or ""
    t = re.sub(r"/\s*\n\s*", "/", t)
    t = re.sub(r"([A-Za-zА-Яа-яЁё])-\s*\n\s*([A-Za-zА-Яа-яЁё])", r"\1\2", t)
    t = re.sub(r"([A-Za-zА-Яа-яЁё])\s*\n\s*([A-Za-zА-Яа-яЁё])", r"\1\2", t)
    t = re.sub(r"[ \t]+", " ", t)
    # «разлепить» метки, если прилипли к слову до них
    t = re.sub(r"(?<!^)(Артикул)(?=\S)", r"\n\1 ", t, flags=re.IGNORECASE)
    t = re.sub(r"(?<!^)(Цвет\s*:)(?=\S)",   r"\n\1 ", t, flags=re.IGNORECASE)
    t = re.sub(r"(?<!^)(Размер\s*:)(?=\S)", r"\n\1 ", t, flags=re.IGNORECASE)
    return t

def clean_color_value(s: str) -> str:
    """
    1) режет по стоп-маркерам (Размер, (01), (21), числа-диапазоны, конец строки)
    2) чистит недопустимые символы
    3) убирает мусорные латинские хвосты/токены (B, clSV, rRAu и т.д.)
    4) оставляет до 3 осмысленных слов, приоритет — кириллица
    """
    s = COLOR_STOP.sub("", s or "")
    s = re.sub(r"[^\w\- А-Яа-яЁё]", " ", s)
    s = re.sub(r"[_\t]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip(" -")

    if not s:
        return s

    # отбрасываем любые латинские символы в конце строки после кириллицы
    s = re.sub(r"(?<=[А-Яа-яЁё])\s*[A-Z]+$", "", s)

    tokens = s.split()
    cleaned = []

    for t in tokens:
        # кириллическое слово — всегда оставляем
        if re.fullmatch(r"[А-Яа-яЁё\-]+", t):
            cleaned.append(t)
            continue
        # чистая латиница, типа clSV — выкидываем
        if re.fullmatch(r"[A-Za-z]+", t):
            continue
        # смешанный мусор типа YccORq — выкидываем
        if re.search(r"[A-Z]", t) and re.search(r"[a-z]", t):
            continue
        # всё остальное (цифро-буквенное) — оставляем
        cleaned.append(t)

    cleaned = cleaned[:3]
    return " ".join(cleaned).strip(" -")
