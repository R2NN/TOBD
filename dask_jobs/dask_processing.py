"""
=============================================================================
dask_jobs/dask_processing.py - Модуль параллельной обработки данных с Dask
=============================================================================

Этот модуль реализует распределённую обработку больших объёмов логов
с использованием Dask для параллелизации вычислений.

Возможности:
- Параллельный парсинг множества лог-файлов
- Распределённая генерация эмбеддингов
- Параллельная классификация логов
- Агрегация результатов

Автор: Команда Big Data Project
Дата: 2025
=============================================================================
"""

import os
import re
import glob
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

import dask
from dask import delayed, compute
from dask.distributed import Client, LocalCluster
import dask.dataframe as dd
import dask.bag as db
import pandas as pd
import numpy as np


# =============================================================================
# КОНФИГУРАЦИЯ DASK
# =============================================================================

class DaskConfig:
    """Конфигурация Dask кластера"""
    N_WORKERS = 4  # Количество воркеров
    THREADS_PER_WORKER = 2  # Потоков на воркер
    MEMORY_LIMIT = '4GB'  # Лимит памяти на воркер
    DASHBOARD_ADDRESS = ':8787'  # Адрес Dask Dashboard


# =============================================================================
# ИНИЦИАЛИЗАЦИЯ DASK КЛАСТЕРА
# =============================================================================

def init_dask_cluster(n_workers: int = None, 
                      threads_per_worker: int = None,
                      memory_limit: str = None) -> Client:
    """
    Инициализирует локальный Dask кластер.
    
    Параметры:
        n_workers: Количество воркеров (по умолчанию из конфига)
        threads_per_worker: Потоков на воркер
        memory_limit: Лимит памяти на воркер
    
    Возвращает:
        Client: Dask клиент для отправки задач
    """
    n_workers = n_workers or DaskConfig.N_WORKERS
    threads_per_worker = threads_per_worker or DaskConfig.THREADS_PER_WORKER
    memory_limit = memory_limit or DaskConfig.MEMORY_LIMIT
    
    print(f">>> [DASK] Инициализация кластера...")
    print(f"    - Воркеры: {n_workers}")
    print(f"    - Потоков на воркер: {threads_per_worker}")
    print(f"    - Лимит памяти: {memory_limit}")
    
    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        memory_limit=memory_limit,
        dashboard_address=DaskConfig.DASHBOARD_ADDRESS
    )
    
    client = Client(cluster)
    print(f">>> [DASK] Кластер запущен!")
    print(f"    - Dashboard: http://localhost{DaskConfig.DASHBOARD_ADDRESS}")
    
    return client


def shutdown_dask_cluster(client: Client):
    """Останавливает Dask кластер"""
    if client:
        client.close()
        print(">>> [DASK] Кластер остановлен")


# =============================================================================
# ПАРСИНГ ЛОГОВ С DASK
# =============================================================================

def parse_log_line(line: str) -> Optional[Dict[str, Any]]:
    """
    Парсит одну строку лога.
    
    Параметры:
        line: Строка лога
    
    Возвращает:
        Dict или None если строка не соответствует формату
    """
    regex = r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\s+(\w+)\s+([^:]+):\s+(.*)"
    match = re.match(regex, line.strip())
    
    if match:
        timestamp_str, level, category, message = match.groups()
        if level in ['WARNING', 'ERROR']:
            return {
                'Timestamp': timestamp_str,
                'Level': level,
                'Category': category,
                'Message': message,
                'log': line.strip()
            }
    return None


@delayed
def process_single_log_file(filepath: str) -> List[Dict[str, Any]]:
    """
    Обрабатывает один лог-файл (delayed функция для Dask).
    
    Параметры:
        filepath: Путь к файлу
    
    Возвращает:
        Список распарсенных записей
    """
    filename = os.path.basename(filepath)
    records = []
    
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line_num, line in enumerate(f, 1):
                if ' INFO ' in line:
                    continue
                    
                parsed = parse_log_line(line)
                if parsed:
                    parsed['file_name'] = filename
                    parsed['line_number'] = line_num
                    records.append(parsed)
                    
    except Exception as e:
        print(f">>> [DASK] Ошибка при чтении {filename}: {e}")
    
    return records


def parallel_parse_logs(log_directory: str, pattern: str = "*.txt") -> pd.DataFrame:
    """
    Параллельно парсит все лог-файлы в директории с использованием Dask.
    
    Параметры:
        log_directory: Директория с логами
        pattern: Паттерн для поиска файлов
    
    Возвращает:
        DataFrame со всеми распарсенными логами
    """
    log_files = glob.glob(os.path.join(log_directory, pattern))
    
    if not log_files:
        print(f">>> [DASK] Файлы не найдены в {log_directory}")
        return pd.DataFrame()
    
    print(f">>> [DASK] Найдено {len(log_files)} файлов для обработки")
    
    # Создаем отложенные задачи для каждого файла
    delayed_results = [process_single_log_file(f) for f in log_files]
    
    # Выполняем параллельную обработку
    print(f">>> [DASK] Запуск параллельной обработки...")
    results = compute(*delayed_results)
    
    # Объединяем результаты
    all_records = []
    for file_records in results:
        all_records.extend(file_records)
    
    if not all_records:
        return pd.DataFrame()
    
    # Создаем DataFrame
    df = pd.DataFrame(all_records)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
    df = df.dropna(subset=['Timestamp'])
    df = df.sort_values(by='Timestamp').reset_index(drop=True)
    
    print(f">>> [DASK] Обработано {len(df)} записей из {len(log_files)} файлов")
    
    return df


# =============================================================================
# РАСПРЕДЕЛЁННАЯ ОБРАБОТКА С DASK BAG
# =============================================================================

def process_logs_with_bag(log_directory: str, pattern: str = "*.txt") -> pd.DataFrame:
    """
    Обрабатывает логи с использованием Dask Bag для потоковой обработки.
    
    Параметры:
        log_directory: Директория с логами
        pattern: Паттерн для поиска файлов
    
    Возвращает:
        DataFrame с результатами
    """
    log_files = glob.glob(os.path.join(log_directory, pattern))
    
    if not log_files:
        return pd.DataFrame()
    
    print(f">>> [DASK BAG] Обработка {len(log_files)} файлов...")
    
    def read_and_parse_file(filepath: str) -> List[Dict]:
        """Читает и парсит файл"""
        filename = os.path.basename(filepath)
        records = []
        
        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                for line_num, line in enumerate(f, 1):
                    if ' INFO ' in line:
                        continue
                    parsed = parse_log_line(line)
                    if parsed:
                        parsed['file_name'] = filename
                        parsed['line_number'] = line_num
                        records.append(parsed)
        except Exception as e:
            print(f"Ошибка: {e}")
        
        return records
    
    # Создаем Bag из списка файлов и обрабатываем
    bag = db.from_sequence(log_files)
    results = bag.map(read_and_parse_file).flatten().compute()
    
    if not results:
        return pd.DataFrame()
    
    df = pd.DataFrame(results)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
    df = df.dropna(subset=['Timestamp'])
    df = df.sort_values(by='Timestamp').reset_index(drop=True)
    
    print(f">>> [DASK BAG] Результат: {len(df)} записей")
    
    return df


# =============================================================================
# ПАРАЛЛЕЛЬНАЯ ГЕНЕРАЦИЯ ЭМБЕДДИНГОВ
# =============================================================================

@delayed
def generate_embeddings_batch(texts: List[str], model, batch_id: int) -> Tuple[int, np.ndarray]:
    """
    Генерирует эмбеддинги для батча текстов (delayed функция).
    
    Параметры:
        texts: Список текстов
        model: SentenceTransformer модель
        batch_id: ID батча для отслеживания
    
    Возвращает:
        Кортеж (batch_id, embeddings)
    """
    embeddings = model.encode(texts, show_progress_bar=False)
    return batch_id, embeddings


def parallel_generate_embeddings(texts: List[str], 
                                  model, 
                                  batch_size: int = 100) -> np.ndarray:
    """
    Параллельно генерирует эмбеддинги с использованием Dask.
    
    Параметры:
        texts: Список текстов для эмбеддинга
        model: SentenceTransformer модель
        batch_size: Размер батча
    
    Возвращает:
        NumPy массив эмбеддингов
    """
    if not texts:
        return np.array([])
    
    # Разбиваем на батчи
    batches = [texts[i:i + batch_size] for i in range(0, len(texts), batch_size)]
    
    print(f">>> [DASK] Генерация эмбеддингов: {len(texts)} текстов, {len(batches)} батчей")
    
    # Создаем отложенные задачи
    delayed_results = [
        generate_embeddings_batch(batch, model, idx) 
        for idx, batch in enumerate(batches)
    ]
    
    # Выполняем параллельно
    results = compute(*delayed_results)
    
    # Сортируем по batch_id и объединяем
    results = sorted(results, key=lambda x: x[0])
    all_embeddings = np.vstack([r[1] for r in results])
    
    print(f">>> [DASK] Сгенерировано {len(all_embeddings)} эмбеддингов")
    
    return all_embeddings


# =============================================================================
# DASK DATAFRAME ДЛЯ БОЛЬШИХ ДАННЫХ
# =============================================================================

def load_large_csv_with_dask(filepath: str, 
                              blocksize: str = '64MB') -> dd.DataFrame:
    """
    Загружает большой CSV файл с использованием Dask DataFrame.
    
    Параметры:
        filepath: Путь к файлу или паттерн (например, 'data/*.csv')
        blocksize: Размер блока для чтения
    
    Возвращает:
        Dask DataFrame
    """
    print(f">>> [DASK] Загрузка CSV: {filepath}")
    
    ddf = dd.read_csv(filepath, blocksize=blocksize)
    
    print(f">>> [DASK] Загружено {ddf.npartitions} партиций")
    
    return ddf


def aggregate_with_dask(ddf: dd.DataFrame, 
                        group_by: List[str], 
                        agg_dict: Dict[str, str]) -> pd.DataFrame:
    """
    Выполняет агрегацию данных с использованием Dask.
    
    Параметры:
        ddf: Dask DataFrame
        group_by: Колонки для группировки
        agg_dict: Словарь агрегаций {колонка: функция}
    
    Возвращает:
        Pandas DataFrame с результатами
    """
    print(f">>> [DASK] Агрегация по {group_by}...")
    
    result = ddf.groupby(group_by).agg(agg_dict).compute()
    
    print(f">>> [DASK] Результат агрегации: {len(result)} строк")
    
    return result


# =============================================================================
# ОСНОВНАЯ ФУНКЦИЯ ОБРАБОТКИ
# =============================================================================

def run_dask_pipeline(log_directory: str,
                      output_directory: str = None,
                      use_cluster: bool = True) -> pd.DataFrame:
    """
    Запускает полный Dask пайплайн обработки логов.
    
    Параметры:
        log_directory: Директория с логами
        output_directory: Директория для результатов
        use_cluster: Использовать ли Dask кластер
    
    Возвращает:
        DataFrame с обработанными данными
    """
    client = None
    
    try:
        if use_cluster:
            client = init_dask_cluster()
        
        print("\n" + "=" * 60)
        print("DASK PIPELINE: Параллельная обработка логов")
        print("=" * 60)
        
        # Шаг 1: Параллельный парсинг логов
        print("\n>>> Шаг 1: Парсинг лог-файлов...")
        logs_df = parallel_parse_logs(log_directory)
        
        if logs_df.empty:
            print(">>> Логи не найдены!")
            return pd.DataFrame()
        
        # Шаг 2: Генерация обобщённых сообщений
        print("\n>>> Шаг 2: Обобщение сообщений...")
        
        def generalize_message(text):
            if not isinstance(text, str):
                return ""
            text = text.lower()
            text = re.sub(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b', 'ip_address', text)
            text = re.sub(r'0x[0-9a-f]+', 'hex_value', text)
            text = re.sub(r'\b\d+\b', 'number', text)
            text = re.sub(r'[^\w\s]', ' ', text)
            text = re.sub(r'\s+', ' ', text).strip()
            return text
        
        logs_df['Generalized_Message'] = logs_df['Message'].apply(generalize_message)
        
        # Шаг 3: Статистика
        print("\n>>> Шаг 3: Расчёт статистики...")
        stats = {
            'total_records': len(logs_df),
            'errors': len(logs_df[logs_df['Level'] == 'ERROR']),
            'warnings': len(logs_df[logs_df['Level'] == 'WARNING']),
            'unique_files': logs_df['file_name'].nunique(),
            'time_range': f"{logs_df['Timestamp'].min()} - {logs_df['Timestamp'].max()}"
        }
        
        print(f"    - Всего записей: {stats['total_records']}")
        print(f"    - ERROR: {stats['errors']}")
        print(f"    - WARNING: {stats['warnings']}")
        print(f"    - Уникальных файлов: {stats['unique_files']}")
        print(f"    - Временной диапазон: {stats['time_range']}")
        
        # Сохранение результатов
        if output_directory:
            os.makedirs(output_directory, exist_ok=True)
            output_path = os.path.join(output_directory, 'dask_processed_logs.csv')
            logs_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            print(f"\n>>> Результаты сохранены: {output_path}")
        
        print("\n" + "=" * 60)
        print("DASK PIPELINE: Завершено успешно!")
        print("=" * 60)
        
        return logs_df
        
    finally:
        if client:
            shutdown_dask_cluster(client)


# =============================================================================
# ТОЧКА ВХОДА
# =============================================================================

if __name__ == "__main__":
    # Пример использования
    print("Dask Processing Module")
    print("=" * 40)
    print("Пример запуска:")
    print("  from dask_jobs.dask_processing import run_dask_pipeline")
    print("  df = run_dask_pipeline('./data/logs')")

