import os
from pathlib import Path
import pdfplumber
from PyPDF2 import PdfReader, PdfWriter


PDF_DIR = Path("pdf-codes")
PDF_DIR.mkdir(exist_ok=True)

def read_pdf(file_path: str | Path) -> str:
    """
    Считывает весь текст из PDF файла с помощью pdfplumber.
    :param file_path: путь до pdf файла
    :return: текст всех страниц одной строкой
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Файл {path} не найден")

    text_parts = []
    with pdfplumber.open(str(path)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text.strip())

    return "\n".join(text_parts)


async def save_pdf_file(data: bytes, filename: str, user_id: int) -> Path:
    """
    Сохраняет PDF в директорию pdf-codes.
    Имя файла = <user_id>_<filename>.
    """
    save_path = PDF_DIR / f"{user_id}_{filename}"
    with open(save_path, "wb") as f:
        f.write(data)
    return save_path


def find_pdf_by_article_size(article: str, size: str) -> str | None:
    """
    Ищет PDF, где встречаются И артикул, И размер (оба как подстроки).
    Возвращает имя файла (str) или None.
    """
    a = str(article).strip()
    s = str(size).strip()
    if not a or not s:
        return None

    for pdf_file in PDF_DIR.glob("*.pdf"):
        try:
            text = read_pdf(pdf_file)
        except Exception as e:
            print(f"⚠️ Ошибка при чтении {pdf_file}: {e}")
            continue

        if a in text and f"Размер: {s}" in text:
            return pdf_file.name

    return None


# def extract_first_n_pages(src_pdf: Path | str, n: int) -> Path:
#     """
#     Вырезает первые n страниц из src_pdf в отдельный временный файл рядом с PDF_DIR/tmp.
#     Возвращает путь к новому PDF.
#     Если n > кол-ва страниц, берём максимально доступное.
#     """
#     src = Path(src_pdf)
#     if not src.exists():
#         raise FileNotFoundError(f"Файл не найден: {src}")
#
#     tmp_dir = PDF_DIR / "tmp"
#     tmp_dir.mkdir(parents=True, exist_ok=True)
#
#     reader = PdfReader(str(src))
#     total = len(reader.pages)
#     take = max(0, min(int(n), total))
#
#     writer = PdfWriter()
#     for i in range(take):
#         writer.add_page(reader.pages[i])
#
#     out_path = tmp_dir / f"{src.stem}__first_{take}.pdf"
#     with open(out_path, "wb") as f:
#         writer.write(f)
#
#     return out_path

def cut_first_n_pages(src_pdf: Path | str, n: int) -> Path | None:
    """
    Вырезает первые n страниц из src_pdf:
      - сохраняет их в отдельный файл (возвращает путь к нему),
      - исходный PDF перезаписывает оставшимися страницами,
        либо удаляет исходник, если страниц не осталось.
    Возвращает путь к файлу с вырезанными страницами, либо None, если n<=0.
    """
    src = Path(src_pdf)
    if not src.exists():
        raise FileNotFoundError(f"Файл не найден: {src}")

    if n <= 0:
        return None

    tmp_dir = (src.parent / "tmp")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # читаем исходник
    with open(src, "rb") as rf:
        reader = PdfReader(rf)
        total = len(reader.pages)
        take = min(int(n), total)
        if take == 0:
            return None

        # 1) "голова" — первые take страниц
        head_writer = PdfWriter()
        for i in range(take):
            head_writer.add_page(reader.pages[i])

        head_out = tmp_dir / f"{src.stem}__head_{take}.pdf"
        with open(head_out, "wb") as f:
            head_writer.write(f)

        # 2) "хвост" — оставшиеся страницы
        remain = total - take
        if remain > 0:
            tail_writer = PdfWriter()
            for i in range(take, total):
                tail_writer.add_page(reader.pages[i])

            tail_tmp = tmp_dir / f"{src.stem}__tail_tmp.pdf"
            with open(tail_tmp, "wb") as f:
                tail_writer.write(f)

    # ВАЖНО: замену исходника делаем уже после закрытия reader
    if remain > 0:
        os.replace(tail_tmp, src)  # атомарная замена исходного файла
    else:
        # ничего не осталось — удаляем исходный файл
        try:
            src.unlink()
        except FileNotFoundError:
            pass

    return head_out


def merge_pdfs(pdf_paths: list[Path | str], output_path: Path | str) -> Path:
    """
    Склеивает список PDF в один файл output_path.
    """
    writer = PdfWriter()
    for p in pdf_paths:
        pth = Path(p)
        if not pth.exists():
            print(f"⚠️ Пропускаю отсутствующий файл при склейке: {pth}")
            continue
        reader = PdfReader(str(pth))
        for page in reader.pages:
            writer.add_page(page)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "wb") as f:
        writer.write(f)

    return out


def build_pdf_from_dataframe(df, output_path: Path | str | None = None) -> Path | None:
    """
    Принимает pandas.DataFrame со столбцами: 'артикул', 'размер', 'количество'.
    Для каждой строки:
      - ищет PDF по (артикул + размер),
      - вырезает первые 'количество' страниц,
      - собирает все вырезанные куски и затем склеивает в один итоговый PDF.
    Возвращает путь к результирующему PDF или None, если ничего не найдено.

    Пример:
        result = build_pdf_from_dataframe(df, PDF_DIR / "result.pdf")
    """
    # Проверки столбцов
    required = {"артикул", "размер", "количество"}
    cols_norm = [str(c).strip().lower() for c in df.columns]
    colset = set(cols_norm)
    if not required.issubset(colset):
        missing = required - colset
        raise ValueError(f"В df нет обязательных колонок: {', '.join(missing)}")

    # Индексы столбцов
    idx_article = cols_norm.index("артикул")
    idx_size = cols_norm.index("размер")
    idx_qty = cols_norm.index("количество")

    # Куда складывать временные вырезанные фрагменты
    cut_parts: list[Path] = []

    for _, row in df.iterrows():
        article = str(row.iloc[idx_article]).strip()
        size = str(row.iloc[idx_size]).strip()

        # количество: стараемся привести к int
        try:
            qty = int(row.iloc[idx_qty])
        except Exception:
            # если не число или NaN — пропускаем строку
            print(f"⚠️ Некорректное количество для {article} / {size}, пропуск.")
            continue

        if qty <= 0:
            print(f"⚠️ Кол-во страниц <= 0 для {article} / {size}, пропуск.")
            continue

        pdf_name = find_pdf_by_article_size(article, size)
        if not pdf_name:
            print(f"⚠️ Не найден PDF для {article} / {size}")
            continue

        src_pdf_path = PDF_DIR / pdf_name
        try:
            part_path = cut_first_n_pages(src_pdf_path, qty)
            # только если реально что-то извлекли
            if Path(part_path).exists() and PdfReader(str(part_path)).pages:
                cut_parts.append(part_path)
            else:
                print(f"⚠️ Пустой фрагмент для {src_pdf_path}")
        except Exception as e:
            print(f"⚠️ Ошибка при вырезании страниц из {src_pdf_path}: {e}")

    if not cut_parts:
        print("⚠️ Нечего склеивать — подходящих фрагментов не найдено.")
        return None

    # Итоговый путь
    if output_path is None:
        output_path = PDF_DIR / "result.pdf"

    result_path = merge_pdfs(cut_parts, output_path)

    for p in cut_parts:
        try:
            Path(p).unlink(missing_ok=True)
        except Exception:
            pass

    return result_path