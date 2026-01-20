from __future__ import annotations

import io
import logging
import re
import csv
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import importlib.util
import requests
import streamlit as st
import xlrd
import xlwt
import shutil
import subprocess
import tempfile


TARGET_HEADERS: List[str] = [
    "Розничная старая",
    "Розничная новая",
    "№",
    "Фото",
    "Код",
    "Артикул",
    "Наименование товаров",
    "Остаток",
    "Корп.",
    "Цена опт.",
    "Закупочная",
    "скидка",
    "акция",
    "наценка",
]
FINAL_HEADERS: List[str] = TARGET_HEADERS + ["Менеджер", "Категория"]

KEYWORDS_FILE = Path("keywords")
KEYWORDS_EXTENSIONS = ("", ".xls", ".csv", ".txt")


def setup_logger() -> Tuple[logging.Logger, List[str]]:
    """Настраивает логирование с сохранением сообщений для вывода в UI."""
    logger = logging.getLogger("xls_merge")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    messages: List[str] = []

    class ListHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            messages.append(self.format(record))

    formatter = logging.Formatter("%(levelname)s: %(message)s")

    list_handler = ListHandler()
    list_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(list_handler)
    logger.addHandler(stream_handler)

    return logger, messages


def normalize_header(value: object) -> str:
    """Нормализует заголовки для сопоставления (нижний регистр, чистка пробелов)."""
    if value is None:
        return ""
    text = str(value).strip().lower()
    text = re.sub(r"[\.:]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text


def is_empty(value: object) -> bool:
    """Проверяет, является ли значение пустым."""
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def select_sheet(book: xlrd.book.Book) -> xlrd.sheet.Sheet:
    """Выбирает лист с максимальным числом заполненных строк."""
    best_sheet = book.sheet_by_index(0)
    best_count = -1
    for sheet in book.sheets():
        filled = 0
        for row_idx in range(sheet.nrows):
            row = sheet.row_values(row_idx)
            if any(not is_empty(cell) for cell in row):
                filled += 1
        if filled > best_count:
            best_count = filled
            best_sheet = sheet
    return best_sheet


def get_cell_value(sheet: xlrd.sheet.Sheet, row_idx: int, col_idx: int) -> object:
    """Возвращает значение ячейки, при необходимости подхватывает ссылку из гиперссылки."""
    value = sheet.cell_value(row_idx, col_idx)
    if not hasattr(sheet, "hyperlink_map"):
        return value
    hyperlink_map = sheet.hyperlink_map or {}
    hyperlink = hyperlink_map.get((row_idx, col_idx))
    if hyperlink and getattr(hyperlink, "url_or_path", ""):
        return hyperlink.url_or_path
    return value


def read_xls_to_rows(
    file_bytes: bytes,
    libreoffice_path: Optional[str],
    logger: logging.Logger,
) -> Tuple[List[List[object]], Dict[int, List[bytes]]]:
    """Читает .xls в список строк и пытается извлечь изображения через конвертацию."""
    if libreoffice_path and importlib.util.find_spec("openpyxl") is not None:
        converted = convert_xls_to_xlsx_libreoffice(file_bytes, libreoffice_path, logger)
        if converted is not None:
            try:
                from openpyxl import load_workbook
                from openpyxl.drawing.image import Image as OpenpyxlImage
                from PIL import Image
            except Exception as exc:
                logger.warning("Не удалось импортировать openpyxl/Pillow: %s", exc)
            else:
                workbook = load_workbook(converted)
                sheet = workbook.active
                rows = [list(row) for row in sheet.iter_rows(values_only=True)]
                images_by_row: Dict[int, List[bytes]] = {}
                for image in getattr(sheet, "_images", []):
                    try:
                        anchor = image.anchor
                        row_idx = anchor._from.row
                        image_bytes = image._data()
                        images_by_row.setdefault(row_idx, []).append(image_bytes)
                    except Exception as exc:
                        logger.warning("Не удалось извлечь изображение из .xlsx: %s", exc)
                return rows, images_by_row

    book = xlrd.open_workbook(file_contents=file_bytes)
    sheet = select_sheet(book)
    if hasattr(sheet, "hyperlink_map") and sheet.hyperlink_map:
        logger.info("Найдены гиперссылки на листе, попробуем перенести их как значения.")
    rows: List[List[object]] = []
    for row_idx in range(sheet.nrows):
        row_values: List[object] = []
        for col_idx in range(sheet.ncols):
            row_values.append(get_cell_value(sheet, row_idx, col_idx))
        rows.append(row_values)
    return rows, {}


def detect_table_header_row(rows: Sequence[Sequence[object]]) -> Optional[int]:
    """Ищет строку шапки таблицы по ключевым заголовкам."""
    key_number = {"№", "n", "no", "номер"}
    for idx, row in enumerate(rows):
        normalized = {normalize_header(cell) for cell in row if not is_empty(cell)}
        if not normalized:
            continue
        has_number = any(cell in key_number for cell in normalized)
        has_name = "наименование товаров" in normalized
        has_code = "код" in normalized
        if has_number and (has_name or has_code):
            return idx
    return None


def extract_document_header(
    rows: Sequence[Sequence[object]], header_row_idx: int
) -> List[List[object]]:
    """Возвращает строки шапки документа (до строки шапки таблицы)."""
    return [list(row) for row in rows[:header_row_idx]]


def build_source_header_map(header_row: Sequence[object]) -> Dict[str, int]:
    """Создает маппинг нормализованных заголовков на индексы колонок."""
    mapping: Dict[str, int] = {}
    for idx, cell in enumerate(header_row):
        key = normalize_header(cell)
        if key and key not in mapping:
            mapping[key] = idx
    return mapping


def find_number_column(header_map: Dict[str, int]) -> Optional[int]:
    """Ищет колонку с номером для фильтрации строк."""
    for key in ["№", "n", "no", "номер"]:
        if key in header_map:
            return header_map[key]
    return None


def map_row_to_target(
    row: Sequence[object],
    source_header_map: Dict[str, int],
    manager_name: str,
    category: str,
) -> List[object]:
    """Преобразует строку исходного файла в строку целевого формата."""
    result: List[object] = []
    result.append(row[0] if len(row) > 0 else "")
    result.append(row[1] if len(row) > 1 else "")

    for header in TARGET_HEADERS[2:]:
        normalized = normalize_header(header)
        source_idx = source_header_map.get(normalized)
        if source_idx is None or source_idx >= len(row):
            result.append("")
        else:
            result.append(row[source_idx])

    result.append(manager_name)
    result.append(category)
    return result


def is_url(value: object) -> bool:
    """Проверяет, что значение похоже на URL."""
    if not value:
        return False
    text = str(value).strip().lower()
    return text.startswith("http://") or text.startswith("https://")


def fetch_image_bytes(url: str, logger: logging.Logger) -> Optional[bytes]:
    """Пытается скачать изображение по URL."""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.content
    except requests.RequestException as exc:
        logger.warning("Не удалось скачать изображение %s: %s", url, exc)
        return None


def convert_xls_to_xlsx_libreoffice(
    xls_bytes: bytes,
    libreoffice_path: Optional[str],
    logger: logging.Logger,
) -> Optional[io.BytesIO]:
    """Конвертирует .xls в .xlsx через LibreOffice в headless-режиме."""
    if not libreoffice_path:
        return None

    with tempfile.TemporaryDirectory() as temp_dir:
        input_path = Path(temp_dir) / "input.xls"
        output_path = Path(temp_dir) / "input.xlsx"
        input_path.write_bytes(xls_bytes)

        result = subprocess.run(
            [
                libreoffice_path,
                "--headless",
                "--convert-to",
                "xlsx",
                "--outdir",
                temp_dir,
                str(input_path),
            ],
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode != 0 or not output_path.exists():
            logger.warning(
                "Не удалось конвертировать .xls → .xlsx через LibreOffice: %s",
                result.stderr.strip() or result.stdout.strip(),
            )
            return None

        output = io.BytesIO(output_path.read_bytes())
        output.seek(0)
        return output


def write_xlsx(
    document_header: Sequence[Sequence[object]],
    rows: Sequence[Sequence[object]],
    images_for_rows: Sequence[List[bytes]],
    logger: logging.Logger,
) -> Optional[io.BytesIO]:
    """Записывает итоговые данные в .xlsx для дополнительной выгрузки."""
    if importlib.util.find_spec("openpyxl") is None:
        logger.warning("openpyxl не установлен, .xlsx файл не будет создан.")
        return None
    if importlib.util.find_spec("PIL") is None:
        logger.warning("pillow не установлен, изображения в .xlsx не будут вставлены.")

    from openpyxl import Workbook
    from openpyxl.drawing.image import Image as OpenpyxlImage
    from PIL import Image

    output = io.BytesIO()
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Общий"

    current_row = 1
    for header_row in document_header:
        for col_idx, value in enumerate(header_row, start=1):
            sheet.cell(row=current_row, column=col_idx, value=value)
        current_row += 1

    for col_idx, header in enumerate(FINAL_HEADERS, start=1):
        sheet.cell(row=current_row, column=col_idx, value=header)
    current_row += 1

    photo_col_index = FINAL_HEADERS.index("Фото") + 1
    for row_index, row in enumerate(rows):
        for col_idx, value in enumerate(row, start=1):
            sheet.cell(row=current_row, column=col_idx, value=value)
        images = images_for_rows[row_index] if row_index < len(images_for_rows) else []
        for image_bytes in images:
            try:
                with Image.open(io.BytesIO(image_bytes)) as img:
                    buffer = io.BytesIO()
                    img.thumbnail((120, 120))
                    img.save(buffer, format="PNG")
                    buffer.seek(0)
                    openpyxl_image = OpenpyxlImage(buffer)
                    openpyxl_image.anchor = sheet.cell(
                        row=current_row, column=photo_col_index
                    ).coordinate
                    sheet.add_image(openpyxl_image)
            except Exception as exc:
                logger.warning("Не удалось обработать изображение: %s", exc)
        photo_value = row[photo_col_index - 1] if len(row) >= photo_col_index else ""
        if (
            not images
            and is_url(photo_value)
            and importlib.util.find_spec("PIL") is not None
        ):
            image_bytes = fetch_image_bytes(str(photo_value), logger)
            if image_bytes:
                try:
                    with Image.open(io.BytesIO(image_bytes)) as img:
                        buffer = io.BytesIO()
                        img.thumbnail((120, 120))
                        img.save(buffer, format="PNG")
                        buffer.seek(0)
                        openpyxl_image = OpenpyxlImage(buffer)
                        openpyxl_image.anchor = sheet.cell(
                            row=current_row, column=photo_col_index
                        ).coordinate
                        sheet.add_image(openpyxl_image)
                except Exception as exc:
                    logger.warning("Не удалось обработать изображение: %s", exc)
        current_row += 1

    workbook.save(output)
    output.seek(0)
    return output


@st.cache_data(show_spinner=False)
def load_keywords_table(file_path: Path) -> List[Tuple[str, str]]:
    """Читает таблицу ключевых слов из файла keywords."""
    if not file_path.exists():
        return []

    if file_path.suffix.lower() == ".xls":
        try:
            book = xlrd.open_workbook(file_contents=file_path.read_bytes())
        except Exception:
            return []
        sheet = select_sheet(book)
        keywords: List[Tuple[str, str]] = []
        for row_idx in range(sheet.nrows):
            keyword = str(sheet.cell_value(row_idx, 0)).strip()
            category = str(sheet.cell_value(row_idx, 1)).strip()
            if keyword:
                keywords.append((keyword.lower(), category))
        return keywords

    content = file_path.read_text(encoding="utf-8").splitlines()
    rows = [line for line in content if line.strip()]
    if not rows:
        return []

    sample = "\n".join(rows[:5])
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";\t,")
    except csv.Error:
        dialect = csv.excel

    keywords: List[Tuple[str, str]] = []
    for row in csv.reader(rows, dialect):
        if len(row) < 2:
            continue
        keyword = row[0].strip()
        category = row[1].strip()
        if keyword:
            keywords.append((keyword.lower(), category))
    return keywords


def resolve_keywords_path() -> Optional[Path]:
    """Ищет файл keywords с поддержкой популярных расширений."""
    for ext in KEYWORDS_EXTENSIONS:
        candidate = KEYWORDS_FILE.with_suffix(ext) if ext else KEYWORDS_FILE
        if candidate.exists():
            return candidate
    return None


def get_category(
    product_name: str,
    keywords: Sequence[Tuple[str, str]],
    logger: logging.Logger,
) -> str:
    """Определяет категорию по ключевым словам из файла keywords."""
    if not product_name:
        return ""
    name_lower = product_name.lower()
    for keyword, category in keywords:
        if keyword and keyword in name_lower:
            logger.info("Категория определена по ключевому слову '%s'.", keyword)
            return category
    return ""


def prepare_source_header_map(header_row: Sequence[object]) -> Dict[str, int]:
    """Создает маппинг заголовков исходного файла."""
    return build_source_header_map(header_row)


def main() -> None:
    st.set_page_config(page_title="Объединение .xls", layout="wide")
    st.title("Объединение .xls файлов")
    st.write(
        "Загрузите несколько файлов Excel 97-2003 (.xls), чтобы получить единый файл "
        "с унифицированными колонками."
    )

    logger, log_messages = setup_logger()

    uploaded_files = st.file_uploader(
        "Загрузите .xls файлы",
        type=["xls"],
        accept_multiple_files=True,
    )
    build_xlsx = True

    st.sidebar.header("Загруженные файлы")
    if uploaded_files:
        for uploaded in uploaded_files:
            st.sidebar.write(f"• {uploaded.name}")
    else:
        st.sidebar.write("Файлы не выбраны.")

    if st.button("Объединить"):
        if not uploaded_files:
            st.warning("Не выбраны файлы.")
            return

        st.write("Общий прогресс по файлам:")
        progress_overall = st.progress(0)
        st.write("Прогресс по строкам текущего файла:")
        progress_rows = st.progress(0)

        keywords_path = resolve_keywords_path()
        keywords = load_keywords_table(keywords_path) if keywords_path else []
        if not keywords:
            logger.warning(
                "Файл keywords не найден или пустой, колонка 'Категория' будет пустой."
            )
        libreoffice_path = shutil.which("libreoffice")
        if not libreoffice_path:
            logger.warning(
                "LibreOffice не найден, конвертация .xls → .xlsx пропущена."
            )
        logger.info(
            "Встроенные изображения в .xls не переносятся, сохраняются только значения и ссылки."
        )

        all_rows: List[List[object]] = []
        all_images: List[List[bytes]] = []
        document_header: List[List[object]] = []
        total_files = len(uploaded_files)
        total_rows_added = 0
        categories_found = 0
        prepared_files: List[Dict[str, object]] = []

        for uploaded in uploaded_files:
            filename = uploaded.name
            try:
                rows, images_by_row = read_xls_to_rows(
                    uploaded.getvalue(),
                    libreoffice_path,
                    logger,
                )
            except Exception as exc:
                logger.error("Не удалось прочитать файл %s: %s", filename, exc)
                continue

            header_row_idx = detect_table_header_row(rows)
            if header_row_idx is None:
                logger.warning("Шапка таблицы не найдена в файле %s", filename)
                continue

            header_row = rows[header_row_idx]
            source_header_map = prepare_source_header_map(header_row)
            number_col_idx = find_number_column(source_header_map)
            if number_col_idx is None:
                logger.warning(
                    "Не удалось определить колонку '№' в файле %s, файл пропущен.",
                    filename,
                )
                continue

            data_rows = rows[header_row_idx + 1 :]
            prepared_files.append(
                {
                    "filename": filename,
                    "rows": rows,
                    "header_row_idx": header_row_idx,
                    "source_header_map": source_header_map,
                    "number_col_idx": number_col_idx,
                    "images_by_row": images_by_row,
                }
            )

        for file_index, file_data in enumerate(prepared_files, start=1):
            filename = file_data["filename"]
            rows = file_data["rows"]
            header_row_idx = file_data["header_row_idx"]
            source_header_map = file_data["source_header_map"]
            number_col_idx = file_data["number_col_idx"]
            manager_name = str(filename).rsplit(".", 1)[0]
            images_by_row = file_data["images_by_row"]
            logger.info("Обработка файла: %s", filename)

            if not document_header:
                document_header = extract_document_header(rows, header_row_idx)

            data_rows = rows[header_row_idx + 1 :]
            rows_count = len(data_rows)
            for row_index, row in enumerate(data_rows, start=1):
                number_value = row[number_col_idx] if number_col_idx < len(row) else ""
                if is_empty(number_value):
                    continue

                first_col_value = row[0] if len(row) > 0 else ""
                if is_empty(first_col_value):
                    continue

                product_name = (
                    row[source_header_map.get("наименование товаров", -1)]
                    if "наименование товаров" in source_header_map
                    and source_header_map["наименование товаров"] < len(row)
                    else ""
                )

                category = get_category(str(product_name), keywords, logger)
                if category:
                    categories_found += 1

                mapped_row = map_row_to_target(
                    row=row,
                    source_header_map=source_header_map,
                    manager_name=manager_name,
                    category=category,
                )
                all_rows.append(mapped_row)
                image_row_index = header_row_idx + row_index
                all_images.append(images_by_row.get(image_row_index, []))
                total_rows_added += 1

                if rows_count:
                    progress_rows.progress(row_index / rows_count)

            progress_overall.progress(file_index / len(prepared_files))

        if not all_rows:
            logger.warning("Нет данных для сохранения.")
            st.text_area("Логи", "\n".join(log_messages), height=200)
            return

        output_xls = io.BytesIO()
        workbook = xlwt.Workbook()
        sheet = workbook.add_sheet("Общий")

        current_row = 0
        for header_row in document_header:
            for col_idx, value in enumerate(header_row):
                sheet.write(current_row, col_idx, value)
            current_row += 1

        for col_idx, header in enumerate(FINAL_HEADERS):
            sheet.write(current_row, col_idx, header)
        current_row += 1

        for row in all_rows:
            for col_idx, value in enumerate(row):
                sheet.write(current_row, col_idx, value)
            current_row += 1

        workbook.save(output_xls)
        output_xls.seek(0)

        output_xlsx = None
        if build_xlsx:
            output_xlsx = convert_xls_to_xlsx_libreoffice(
                output_xls.getvalue(),
                libreoffice_path,
                logger,
            )
            if output_xlsx is None:
                output_xlsx = write_xlsx(document_header, all_rows, all_images, logger)

        st.success("Объединение завершено.")
        st.write(
            f"Файлов обработано: {total_files}. "
            f"Строк собрано: {total_rows_added}. "
            f"Категорий найдено: {categories_found}."
        )

        if output_xlsx is not None:
            st.download_button(
                label="Скачать Акция ОБЩИЙ.xlsx",
                data=output_xlsx,
                file_name="Акция ОБЩИЙ.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        else:
            st.warning("Не удалось сформировать .xlsx файл для скачивания.")

        st.text_area("Логи", "\n".join(log_messages), height=200)


if __name__ == "__main__":
    main()
