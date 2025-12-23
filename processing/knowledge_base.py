"""
=============================================================================
processing/knowledge_base.py - Модуль работы с базой знаний
=============================================================================

Этот модуль отвечает за загрузку и подготовку базы знаний из CSV-файла.
База знаний содержит информацию о известных аномалиях (WARNING) и проблемах (ERROR),
а также их взаимосвязях.

Структура базы знаний (CSV):
    anomaly_id - уникальный ID аномалии (WARNING)
    Anomaly_Text - текстовое описание аномалии
    problem_id - уникальный ID проблемы (ERROR)
    Problem_Text - текстовое описание проблемы

Автор: Команда Atomichack 3.0
=============================================================================
"""

import os
import re
import pandas as pd


# =============================================================================
# ФУНКЦИИ ОБРАБОТКИ ТЕКСТА
# =============================================================================

def generalize_message(text: str) -> str:
    """
    Обобщает (генерализует) текст сообщения лога для более точного сопоставления.
    
    Функция заменяет специфичные значения (IP-адреса, числа, пути к файлам)
    на общие метки. Это позволяет сопоставлять логи с одинаковым смыслом,
    но разными конкретными значениями.
    
    Параметры:
        text (str): Исходный текст сообщения лога
    
    Возвращает:
        str: Обобщенный текст с замененными специфичными значениями
    
    Примеры преобразований:
        "Connection from 192.168.1.100 failed" 
        -> "connection from ip address failed"
        
        "Error at line 42 in /var/log/app.log" 
        -> "error at line number in file path"
        
        "Memory address 0xABCD1234" 
        -> "memory address hex value"
    
    Алгоритм:
        1. Приводит текст к нижнему регистру
        2. Заменяет IP-адреса на "ip address"
        3. Заменяет шестнадцатеричные значения на "hex value"
        4. Заменяет пути к файлам на "file path"
        5. Заменяет все числа на "number"
        6. Удаляет знаки пунктуации
        7. Нормализует пробелы
    """
    # Проверка на валидность входных данных
    if not isinstance(text, str):
        return ""
    
    # Приводим к нижнему регистру для единообразия
    text = text.lower()
    
    # Заменяем IP-адреса формата xxx.xxx.xxx.xxx
    text = re.sub(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b', 'ip address', text)
    
    # Заменяем шестнадцатеричные значения (0x...)
    text = re.sub(r'0x[0-9a-f]+', 'hex value', text)
    
    # Заменяем пути к файлам (Unix/Linux стиль)
    text = re.sub(r'(?:/[^/ ]*)+/?', 'file path', text)
    
    # Заменяем все числа на метку "number"
    text = re.sub(r'\b\d+\b', 'number', text)
    
    # Удаляем знаки пунктуации, заменяя их пробелами
    text = re.sub(r'[^\w\s]', ' ', text)
    
    # Нормализуем множественные пробелы в один
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text


# =============================================================================
# ФУНКЦИИ ЗАГРУЗКИ БАЗЫ ЗНАНИЙ
# =============================================================================

def load_knowledge_base(kb_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Загружает и обрабатывает файл с базой знаний (CSV или Excel).
    
    Функция читает файл базы знаний (CSV, XLSX или XLS), содержащий информацию 
    об аномалиях и проблемах, и разделяет его на две таблицы:
    1. Таблицу аномалий (WARNING) - содержит все записи с их связями к проблемам
    2. Таблицу проблем (ERROR) - содержит уникальные проблемы
    
    Обе таблицы дополняются обобщенными версиями текстов для ML-анализа.
    
    Параметры:
        kb_path (str): Путь к файлу с базой знаний (CSV, XLSX или XLS)
    
    Возвращает:
        tuple[pd.DataFrame, pd.DataFrame]: Кортеж из двух DataFrame:
            - anomalies_kb: таблица аномалий с колонками
              [anomaly_id, problem_id, Generalized_Anomaly, Anomaly_Text]
            - problems_kb: таблица проблем с колонками
              [problem_id, Generalized_Problem, Problem_Text]
    
    Исключения:
        FileNotFoundError: Если файл базы знаний не найден по указанному пути
        ValueError: Если формат файла не поддерживается
    
    Поддерживаемые форматы:
        - CSV (.csv) с разделителем ';'
        - Excel (.xlsx, .xls)
    
    Формат входного файла:
        anomaly_id;Anomaly_Text;problem_id;Problem_Text
        1;"Warning text";10;"Problem description"
        ...
    """
    # Проверяем существование файла
    if not os.path.exists(kb_path):
        raise FileNotFoundError(
            f"Файл базы знаний не найден! Проверьте путь: {kb_path}"
        )
    
    # Определяем формат файла по расширению
    file_extension = os.path.splitext(kb_path)[1].lower()
    
    # Загружаем файл в зависимости от формата
    if file_extension == '.csv':
        # Загружаем CSV с явным указанием разделителя и названий колонок
        kb_df = pd.read_csv(
            kb_path, 
            sep=';', 
            names=['anomaly_id', 'Anomaly_Text', 'problem_id', 'Problem_Text'], 
            header=0  # Первая строка содержит заголовки
        )
    elif file_extension in ['.xlsx', '.xls']:
        # Загружаем Excel файл
        kb_df = pd.read_excel(
            kb_path,
            names=['anomaly_id', 'Anomaly_Text', 'problem_id', 'Problem_Text'],
            header=0,  # Первая строка содержит заголовки
            engine='openpyxl' if file_extension == '.xlsx' else None
        )
    else:
        raise ValueError(
            f"Неподдерживаемый формат файла: {file_extension}. "
            f"Поддерживаются форматы: .csv, .xlsx, .xls"
        )
    
    # Создаем обобщенные версии текстов для ML-сопоставления
    kb_df['Generalized_Anomaly'] = kb_df['Anomaly_Text'].apply(generalize_message)
    kb_df['Generalized_Problem'] = kb_df['Problem_Text'].apply(generalize_message)
    
    # Формируем таблицу аномалий (WARNING)
    # Сохраняем все записи, так как одна аномалия может относиться к нескольким проблемам
    anomalies_kb = kb_df[[
        'anomaly_id', 
        'problem_id', 
        'Generalized_Anomaly', 
        'Anomaly_Text'
    ]].reset_index(drop=True)
    
    # Формируем таблицу проблем (ERROR)
    # Убираем дубликаты, так как одна проблема может быть связана с множеством аномалий
    problems_kb = kb_df[[
        'problem_id', 
        'Generalized_Problem', 
        'Problem_Text'
    ]].drop_duplicates().reset_index(drop=True)
    
    return anomalies_kb, problems_kb

