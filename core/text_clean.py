import re
from .patterns import GS1_01, GS1_21, COLOR_STOP

def normalize_dashes(text: str) -> str:
    return (text or "").replace("–", "-").replace("—", "-")

def strip_gs1(text: str) -> str:
    t = GS1_01.sub(" ", text or "")
    t = GS1_21.sub(" ", t)
    return t

# def clean_for_parsing(raw: str) -> str:
#     """Склейка переносов и «разлепление» лейблов Артикул/Цвет/Размер."""
#     t = raw or ""
#     t = re.sub(r"/\s*\n\s*", "/", t)
#     t = re.sub(r"([A-Za-zА-Яа-яЁё])-\s*\n\s*([A-Za-zА-Яа-яЁё])", r"\1\2", t)
#     t = re.sub(r"([A-Za-zА-Яа-яЁё])\s*\n\s*([A-Za-zА-Яа-яЁё])", r"\1\2", t)
#     t = re.sub(r"[ \t]+", " ", t)
#     t = re.sub(r"(?<!^)(Артикул)(?=\S)", r"\n\1 ", t, flags=re.IGNORECASE)
#     t = re.sub(r"(?<!^)(Цвет\s*:)(?=\S)",   r"\n\1 ", t, flags=re.IGNORECASE)
#     t = re.sub(r"(?<!^)(Размер\s*:)(?=\S)", r"\n\1 ", t, flags=re.IGNORECASE)
#     return t


def clean_for_parsing(raw: str) -> str:
    """Корректная склейка переносов в артикулах и обычном тексте."""
    t = raw or ""

    # 1) Склейка внутри артикула: после слэша — БЕЗ пробела
    #    /бир\nюзовый → /бирюзовый
    t = re.sub(
        r"/([A-Za-zА-Яа-яЁё]+)\s*\n\s*([A-Za-zА-Яа-яЁё]+)",
        r"/\1\2",
        t
    )

    # 2) Склейка дефиса между частями слов
    #    темно-\nсиний → темно-синий
    t = re.sub(
        r"([A-Za-zА-Яа-яЁё])-\s*\n\s*([A-Za-zА-Яа-яЁё])",
        r"\1-\2",
        t
    )

    # 3) Остальные переносы — превращаем в пробел
    #    Columbia\nтемно-синий → Columbia темно-синий
    t = re.sub(
        r"([A-Za-zА-Яа-яЁё])\s*\n\s*([A-Za-zА-Яа-яЁё])",
        r"\1 \2",
        t
    )

    # 4) Нормализация пробелов
    t = re.sub(r"[ \t]+", " ", t)

    # 5) Разлепление меток
    t = re.sub(r"(?<!^)(Артикул)(?=\S)",   r"\n\1 ", t, flags=re.IGNORECASE)
    t = re.sub(r"(?<!^)(Цвет\s*:)(?=\S)",  r"\n\1 ", t, flags=re.IGNORECASE)
    t = re.sub(r"(?<!^)(Размер\s*:)(?=\S)", r"\n\1 ", t, flags=re.IGNORECASE)

    return t


def clean_color_value(s: str) -> str:
    """
    1) отрезает по стоп-маркерам (Размер, (01), (21), числа/диапазоны, конец строки)
    2) чистит недопустимые символы
    3) убирает мусорные латинские хвосты (B, clSV, rAu и т.п.)
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
        if re.fullmatch(r"[А-Яа-яЁё\-]+", t):
            cleaned.append(t); continue
        if re.fullmatch(r"[A-Za-z]+", t):
            continue
        if re.search(r"[A-Z]", t) and re.search(r"[a-z]", t):
            continue
        cleaned.append(t)

    cleaned = cleaned[:3]
    return " ".join(cleaned).strip(" -")
