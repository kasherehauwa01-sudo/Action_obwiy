from __future__ import annotations

import io
import logging
import re
import csv
from typing import Dict, List, Optional, Sequence, Tuple
from pathlib import Path
import streamlit as st
import xlrd
import xlwt


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


def read_xls_to_rows(file_bytes: bytes, logger: logging.Logger) -> List[List[object]]:
    """Читает .xls в список строк, стараясь сохранить гиперссылки."""
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
    return rows


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


@st.cache_data(show_spinner=False)
def load_keywords_table(file_path: Path) -> List[Tuple[str, str]]:
    """Читает таблицу ключевых слов из файла keywords."""
    if not file_path.exists():
        return []

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

    if st.button("Объединить"):
        if not uploaded_files:
            st.warning("Не выбраны файлы.")
            return

        progress_files = st.progress(0)
        progress_rows = st.progress(0)

        keywords = load_keywords_table(KEYWORDS_FILE)
        if not keywords:
            logger.warning(
                "Файл keywords не найден или пустой, колонка 'Категория' будет пустой."
            )

        all_rows: List[List[object]] = []
        document_header: List[List[object]] = []
        total_files = len(uploaded_files)
        total_rows_processed = 0
        total_rows_added = 0
        categories_found = 0

        for file_index, uploaded in enumerate(uploaded_files, start=1):
            filename = uploaded.name
            manager_name = filename.rsplit(".", 1)[0]
            logger.info("Обработка файла: %s", filename)

            try:
                rows = read_xls_to_rows(uploaded.getvalue(), logger)
            except Exception as exc:
                logger.error("Не удалось прочитать файл %s: %s", filename, exc)
                progress_files.progress(file_index / total_files)
                continue

            header_row_idx = detect_table_header_row(rows)
            if header_row_idx is None:
                logger.warning("Шапка таблицы не найдена в файле %s", filename)
                progress_files.progress(file_index / total_files)
                continue

            if not document_header:
                document_header = extract_document_header(rows, header_row_idx)

            header_row = rows[header_row_idx]
            source_header_map = prepare_source_header_map(header_row)

            number_col_idx = find_number_column(source_header_map)
            if number_col_idx is None:
                logger.warning(
                    "Не удалось определить колонку '№' в файле %s, файл пропущен.",
                    filename,
                )
                progress_files.progress(file_index / total_files)
                continue

            data_rows = rows[header_row_idx + 1 :]
            rows_count = len(data_rows)
            for row_index, row in enumerate(data_rows, start=1):
                total_rows_processed += 1
                number_value = row[number_col_idx] if number_col_idx < len(row) else ""
                if is_empty(number_value):
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
                total_rows_added += 1

                if rows_count:
                    progress_rows.progress(row_index / rows_count)

            progress_files.progress(file_index / total_files)

        if not all_rows:
            logger.warning("Нет данных для сохранения.")
            st.text_area("Логи", "\n".join(log_messages), height=200)
            return

        output = io.BytesIO()
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

        workbook.save(output)
        output.seek(0)

        st.success("Объединение завершено.")
        st.write(
            f"Файлов обработано: {total_files}. "
            f"Строк собрано: {total_rows_added}. "
            f"Категорий найдено: {categories_found}."
        )

        st.download_button(
            label="Скачать Акция ОБЩИЙ.xls",
            data=output,
            file_name="Акция ОБЩИЙ.xls",
            mime="application/vnd.ms-excel",
        )

        st.text_area("Логи", "\n".join(log_messages), height=200)


if __name__ == "__main__":
    main()
