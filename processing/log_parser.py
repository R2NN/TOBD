"""
=============================================================================
processing/log_parser.py - Модуль парсинга и обработки лог-файлов
=============================================================================

Этот модуль отвечает за чтение и парсинг лог-файлов из директории,
извлечение структурированной информации из строк логов и подготовку
данных для ML-анализа.

Формат ожидаемых логов:
    YYYY-MM-DDTHH:MM:SS LEVEL Category: Message
    Пример: 2024-01-15T10:30:45 ERROR Database: Connection timeout

Автор: Команда Atomichack 3.0
=============================================================================
"""

import os
import re
import glob
import pandas as pd

from .knowledge_base import generalize_message


# =============================================================================
# ФУНКЦИИ ПАРСИНГА ЛОГОВ
# =============================================================================

def parse_log_line(line: str) -> tuple[str | None, str, str, str]:
    """
    Парсит строку лога и извлекает из нее структурированную информацию.
    
    Функция использует регулярное выражение для разбора строки на компоненты:
    временная метка, уровень (WARNING/ERROR), категория и сообщение.
    
    Параметры:
        line (str): Строка из лог-файла
    
    Возвращает:
        tuple: Кортеж из 4 элементов (timestamp, level, category, message)
            - timestamp (str | None): Временная метка в формате ISO
            - level (str): Уровень логирования (WARNING, ERROR, или UNKNOWN)
            - category (str): Категория сообщения (или UNKNOWN)
            - message (str): Текст сообщения
    
    Пример:
        >>> parse_log_line("2024-01-15T10:30:45 ERROR Database: Connection timeout")
        ('2024-01-15T10:30:45', 'ERROR', 'Database', 'Connection timeout')
        
        >>> parse_log_line("Invalid log line")
        (None, 'UNKNOWN', 'UNKNOWN', 'Invalid log line')
    
    Формат регулярного выражения:
        - (\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}) - временная метка ISO 8601
        - (\\w+) - уровень логирования (одно слово)
        - ([^:]+) - категория (все до двоеточия)
        - (.*) - остальное сообщение
    """
    # Регулярное выражение для парсинга стандартного формата лога
    regex = r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\s+(\w+)\s+([^:]+):\s+(.*)"
    match = re.match(regex, line)
    
    # Если строка соответствует формату, возвращаем извлеченные компоненты
    if match:
        return match.groups()
    
    # Если не удалось распарсить, возвращаем значения по умолчанию
    return None, 'UNKNOWN', 'UNKNOWN', line


def process_all_logs_for_case(case_directory: str) -> pd.DataFrame:
    """
    Обрабатывает все .txt лог-файлы в указанной директории.
    
    Функция сканирует директорию, читает все TXT-файлы, парсит каждую строку
    и собирает все логи уровня WARNING и ERROR в единый DataFrame.
    Логи уровня INFO игнорируются для фокусировки на проблемах.
    
    Параметры:
        case_directory (str): Путь к директории с лог-файлами
    
    Возвращает:
        pd.DataFrame: DataFrame с обработанными логами, содержащий колонки:
            - Timestamp (datetime): Временная метка (преобразованная в datetime)
            - Level (str): Уровень логирования (WARNING или ERROR)
            - Message (str): Текст сообщения
            - file_name (str): Имя файла, из которого взята запись
            - line_number (int): Номер строки в исходном файле
            - log (str): Исходная строка лога целиком
            - Generalized_Message (str): Обобщенная версия сообщения для ML
        
        Возвращает пустой DataFrame, если не найдено подходящих логов.
    
    Обработка ошибок:
        - Игнорирует файлы с ошибками чтения
        - Пропускает строки с невалидными временными метками
        - Выводит предупреждения в консоль при ошибках
    
    Сортировка:
        Все логи сортируются по временной метке для хронологического анализа.
    """
    all_logs = []
    
    # Находим все .txt файлы в директории
    log_files = glob.glob(os.path.join(case_directory, "*.txt"))
    
    # Обрабатываем каждый файл
    for filepath in log_files:
        filename = os.path.basename(filepath)
        
        try:
            # Открываем файл с обработкой ошибок кодировки
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                for i, line in enumerate(f):
                    line = line.strip()
                    
                    # Пропускаем пустые строки и INFO-логи
                    if not line or ' INFO ' in line:
                        continue
                    
                    # Парсим строку лога
                    timestamp, level, category, message = parse_log_line(line)
                    
                    # Сохраняем только WARNING и ERROR
                    if level in ['WARNING', 'ERROR']:
                        all_logs.append({
                            'Timestamp': timestamp,
                            'Level': level,
                            'Message': message,
                            'file_name': filename,
                            'line_number': i + 1,  # Номера строк с 1
                            'log': line  # Сохраняем оригинальную строку
                        })
        
        except Exception as e:
            # Логируем ошибку, но продолжаем обработку других файлов
            print(f"Ошибка при чтении файла {filename}: {e}")
    
    # Если не нашли ни одной подходящей записи, возвращаем пустой DataFrame
    if not all_logs:
        return pd.DataFrame()
    
    # Создаем DataFrame из собранных логов
    logs_df = pd.DataFrame(all_logs)
    
    # Преобразуем временные метки в datetime объекты
    logs_df['Timestamp'] = pd.to_datetime(logs_df['Timestamp'], errors='coerce')
    
    # Удаляем строки с невалидными временными метками
    logs_df = logs_df.dropna(subset=['Timestamp'])
    
    # Сортируем по времени для хронологического анализа
    logs_df = logs_df.sort_values(by='Timestamp').reset_index(drop=True)
    
    # Добавляем обобщенные версии сообщений для ML-анализа
    logs_df['Generalized_Message'] = logs_df['Message'].apply(generalize_message)
    
    return logs_df


def get_context_snippet(case_directory: str, filename: str, 
                       line_number: int, context_lines: int = 5) -> str:
    """
    Извлекает фрагмент лога с контекстными строками вокруг целевой строки.
    
    Функция читает файл и возвращает указанное количество строк до и после
    целевой строки. Это полезно для понимания контекста возникновения ошибки.
    
    Параметры:
        case_directory (str): Директория с лог-файлами
        filename (str): Имя файла лога
        line_number (int): Номер целевой строки (нумерация с 1)
        context_lines (int): Количество строк контекста до и после (по умолчанию 5)
    
    Возвращает:
        str: Форматированный текстовый фрагмент с номерами строк
             Целевая строка помечается префиксом ">>"
    
    Пример вывода:
           37: Previous log line
           38: Another context line
        >> 39: ERROR: This is the target line
           40: Following context
           41: More context
    
    Обработка ошибок:
        - Возвращает сообщение об ошибке, если файл не найден
        - Возвращает сообщение об ошибке, если не удалось прочитать файл
    """
    filepath = os.path.join(case_directory, filename)
    
    # Проверяем существование файла
    if not os.path.exists(filepath):
        return f"Файл не найден: {filename}"
    
    snippet = []
    
    # Вычисляем диапазон строк для извлечения
    start_line = max(1, line_number - context_lines)
    end_line = line_number + context_lines
    
    try:
        # Читаем файл и извлекаем нужные строки
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for i, line in enumerate(f, 1):
                if start_line <= i <= end_line:
                    # Помечаем целевую строку префиксом ">>"
                    prefix = ">> " if i == line_number else "   "
                    snippet.append(f"{prefix}{i}: {line.strip()}")
                
                # Прекращаем чтение после последней нужной строки
                if i > end_line:
                    break
    
    except Exception as e:
        return f"Ошибка при чтении файла {filename}: {e}"
    
    # Объединяем строки с переносами
    return "\n".join(snippet)

