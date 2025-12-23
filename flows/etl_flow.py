"""
=============================================================================
flows/etl_flow.py - ETL Pipeline для обработки логов
=============================================================================

Этот модуль реализует ETL (Extract, Transform, Load) пайплайн
для обработки и анализа лог-файлов.

Структура ETL:
1. EXTRACT  - Извлечение данных из источников (ZIP, файлы, директории)
2. TRANSFORM - Трансформация: парсинг, ML-анализ, классификация
3. LOAD     - Загрузка результатов в хранилище (PostgreSQL, файлы)

Автор: Команда Big Data Project
Дата: 2025
=============================================================================
"""

import os
import sys
import glob
import zipfile
import tempfile
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

# Добавляем родительскую директорию для импортов
sys.path.insert(0, str(Path(__file__).parent.parent))

# Импорт Dask для параллельной обработки
try:
    from dask_jobs.dask_processing import (
        parallel_parse_logs,
        init_dask_cluster,
        shutdown_dask_cluster
    )
    DASK_AVAILABLE = True
except ImportError:
    DASK_AVAILABLE = False
    print(">>> [ETL] Dask недоступен, используется последовательная обработка")

# Импорт для работы с PostgreSQL
try:
    import psycopg2
    from psycopg2.extras import execute_values
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False
    print(">>> [ETL] psycopg2 недоступен, результаты будут сохранены в файлы")


# =============================================================================
# КОНФИГУРАЦИЯ
# =============================================================================

class ETLConfig:
    """Конфигурация ETL пайплайна"""
    
    # Директории
    DATA_DIR = Path("data")
    OUTPUT_DIR = Path("storage/etl_results")
    TEMP_DIR = Path("storage/temp")
    
    # PostgreSQL
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_DB = os.getenv("POSTGRES_DB", "log_analytics")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "analytics")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "analytics_password")
    
    # Обработка
    BATCH_SIZE = 1000
    USE_DASK = True
    
    @classmethod
    def get_postgres_conn_string(cls) -> str:
        """Возвращает строку подключения к PostgreSQL"""
        return f"postgresql://{cls.POSTGRES_USER}:{cls.POSTGRES_PASSWORD}@{cls.POSTGRES_HOST}:{cls.POSTGRES_PORT}/{cls.POSTGRES_DB}"


# =============================================================================
# ETL ЗАДАЧИ
# =============================================================================

class ETLTask:
    """Базовый класс ETL задачи"""
    
    def __init__(self, name: str):
        self.name = name
        self.start_time = None
        self.end_time = None
        self.status = "pending"
        self.result = None
        self.error = None
    
    def run(self, *args, **kwargs):
        """Запускает задачу"""
        self.start_time = datetime.now()
        self.status = "running"
        
        try:
            self.result = self.execute(*args, **kwargs)
            self.status = "completed"
        except Exception as e:
            self.status = "failed"
            self.error = str(e)
            raise
        finally:
            self.end_time = datetime.now()
        
        return self.result
    
    def execute(self, *args, **kwargs):
        """Переопределите в подклассе"""
        raise NotImplementedError
    
    @property
    def duration(self) -> float:
        """Длительность выполнения в секундах"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0


# =============================================================================
# EXTRACT - ИЗВЛЕЧЕНИЕ ДАННЫХ
# =============================================================================

class ExtractTask(ETLTask):
    """
    Задача извлечения данных из источников.
    
    Поддерживает:
    - ZIP архивы
    - Директории с лог-файлами
    - Отдельные файлы
    """
    
    def __init__(self):
        super().__init__("extract")
    
    def execute(self, source_path: str) -> Dict[str, Any]:
        """
        Извлекает данные из источника.
        
        Параметры:
            source_path: Путь к источнику данных
        
        Возвращает:
            Dict с извлечёнными данными и метаданными
        """
        print(f"\n{'='*60}")
        print(f"[EXTRACT] Начало извлечения данных")
        print(f"{'='*60}")
        print(f"Источник: {source_path}")
        
        source = Path(source_path)
        
        if source.suffix.lower() == '.zip':
            return self._extract_from_zip(source)
        elif source.is_dir():
            return self._extract_from_directory(source)
        elif source.is_file():
            return self._extract_from_file(source)
        else:
            raise ValueError(f"Неподдерживаемый источник: {source_path}")
    
    def _extract_from_zip(self, zip_path: Path) -> Dict[str, Any]:
        """Извлекает данные из ZIP архива"""
        print(f">>> Извлечение из ZIP: {zip_path.name}")
        
        temp_dir = ETLConfig.TEMP_DIR / f"extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        temp_dir.mkdir(parents=True, exist_ok=True)
        
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(temp_dir)
        
        # Ищем лог-файлы
        log_files = list(temp_dir.rglob("*.txt"))
        
        # Ищем базу знаний
        kb_file = None
        for pattern in ["anomalies_problems.csv", "anomalies_problems.xlsx"]:
            found = list(temp_dir.rglob(pattern))
            if found:
                kb_file = found[0]
                break
        
        result = {
            'source_type': 'zip',
            'source_path': str(zip_path),
            'temp_dir': str(temp_dir),
            'log_files': [str(f) for f in log_files],
            'kb_file': str(kb_file) if kb_file else None,
            'files_count': len(log_files),
            'extracted_at': datetime.now().isoformat()
        }
        
        print(f">>> Извлечено {len(log_files)} лог-файлов")
        if kb_file:
            print(f">>> База знаний: {kb_file.name}")
        
        return result
    
    def _extract_from_directory(self, dir_path: Path) -> Dict[str, Any]:
        """Извлекает данные из директории"""
        print(f">>> Сканирование директории: {dir_path}")
        
        log_files = list(dir_path.rglob("*.txt"))
        
        # Ищем базу знаний
        kb_file = None
        for pattern in ["anomalies_problems.csv", "anomalies_problems.xlsx"]:
            found = list(dir_path.rglob(pattern))
            if found:
                kb_file = found[0]
                break
        
        result = {
            'source_type': 'directory',
            'source_path': str(dir_path),
            'temp_dir': str(dir_path),
            'log_files': [str(f) for f in log_files],
            'kb_file': str(kb_file) if kb_file else None,
            'files_count': len(log_files),
            'extracted_at': datetime.now().isoformat()
        }
        
        print(f">>> Найдено {len(log_files)} лог-файлов")
        
        return result
    
    def _extract_from_file(self, file_path: Path) -> Dict[str, Any]:
        """Извлекает данные из одного файла"""
        print(f">>> Чтение файла: {file_path.name}")
        
        return {
            'source_type': 'file',
            'source_path': str(file_path),
            'temp_dir': str(file_path.parent),
            'log_files': [str(file_path)],
            'kb_file': None,
            'files_count': 1,
            'extracted_at': datetime.now().isoformat()
        }


# =============================================================================
# TRANSFORM - ТРАНСФОРМАЦИЯ ДАННЫХ
# =============================================================================

class TransformTask(ETLTask):
    """
    Задача трансформации данных.
    
    Выполняет:
    - Парсинг лог-файлов
    - Генерализацию сообщений
    - Фильтрацию и очистку
    - Подготовку для ML-анализа
    """
    
    def __init__(self):
        super().__init__("transform")
    
    def execute(self, extract_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Трансформирует извлечённые данные.
        
        Параметры:
            extract_result: Результат задачи Extract
        
        Возвращает:
            Dict с трансформированными данными
        """
        print(f"\n{'='*60}")
        print(f"[TRANSFORM] Начало трансформации данных")
        print(f"{'='*60}")
        
        temp_dir = extract_result['temp_dir']
        log_files = extract_result['log_files']
        
        print(f">>> Обработка {len(log_files)} файлов...")
        
        # Используем Dask если доступен
        if DASK_AVAILABLE and ETLConfig.USE_DASK and len(log_files) > 1:
            print(">>> Использование Dask для параллельной обработки")
            logs_df = parallel_parse_logs(temp_dir)
        else:
            print(">>> Последовательная обработка файлов")
            logs_df = self._sequential_parse(log_files)
        
        if logs_df.empty:
            print(">>> WARNING: Не найдено записей WARNING/ERROR")
            return {
                'logs_df': pd.DataFrame(),
                'stats': {'total': 0, 'errors': 0, 'warnings': 0},
                'transformed_at': datetime.now().isoformat()
            }
        
        # Генерализация сообщений
        print(">>> Генерализация сообщений...")
        logs_df['Generalized_Message'] = logs_df['Message'].apply(self._generalize_message)
        
        # Статистика
        stats = {
            'total': len(logs_df),
            'errors': len(logs_df[logs_df['Level'] == 'ERROR']),
            'warnings': len(logs_df[logs_df['Level'] == 'WARNING']),
            'unique_files': logs_df['file_name'].nunique(),
            'time_range_start': str(logs_df['Timestamp'].min()),
            'time_range_end': str(logs_df['Timestamp'].max())
        }
        
        print(f">>> Обработано записей: {stats['total']}")
        print(f"    - ERROR: {stats['errors']}")
        print(f"    - WARNING: {stats['warnings']}")
        
        return {
            'logs_df': logs_df,
            'stats': stats,
            'extract_result': extract_result,
            'transformed_at': datetime.now().isoformat()
        }
    
    def _sequential_parse(self, log_files: List[str]) -> pd.DataFrame:
        """Последовательный парсинг файлов"""
        import re
        
        all_records = []
        
        for filepath in log_files:
            filename = os.path.basename(filepath)
            
            try:
                with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                    for line_num, line in enumerate(f, 1):
                        if ' INFO ' in line:
                            continue
                        
                        parsed = self._parse_log_line(line.strip())
                        if parsed:
                            parsed['file_name'] = filename
                            parsed['line_number'] = line_num
                            all_records.append(parsed)
                            
            except Exception as e:
                print(f">>> Ошибка при чтении {filename}: {e}")
        
        if not all_records:
            return pd.DataFrame()
        
        df = pd.DataFrame(all_records)
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
        df = df.dropna(subset=['Timestamp'])
        df = df.sort_values(by='Timestamp').reset_index(drop=True)
        
        return df
    
    def _parse_log_line(self, line: str) -> Optional[Dict]:
        """Парсит строку лога"""
        import re
        
        regex = r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\s+(\w+)\s+([^:]+):\s+(.*)"
        match = re.match(regex, line)
        
        if match:
            timestamp_str, level, category, message = match.groups()
            if level in ['WARNING', 'ERROR']:
                return {
                    'Timestamp': timestamp_str,
                    'Level': level,
                    'Category': category,
                    'Message': message,
                    'log': line
                }
        return None
    
    def _generalize_message(self, text: str) -> str:
        """Обобщает сообщение для ML-анализа"""
        import re
        
        if not isinstance(text, str):
            return ""
        
        text = text.lower()
        text = re.sub(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b', 'ip_address', text)
        text = re.sub(r'0x[0-9a-f]+', 'hex_value', text)
        text = re.sub(r'\b\d+\b', 'number', text)
        text = re.sub(r'[^\w\s]', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text


# =============================================================================
# LOAD - ЗАГРУЗКА ДАННЫХ
# =============================================================================

class LoadTask(ETLTask):
    """
    Задача загрузки данных в хранилище.
    
    Поддерживает:
    - PostgreSQL
    - CSV файлы
    - Excel файлы
    """
    
    def __init__(self):
        super().__init__("load")
    
    def execute(self, transform_result: Dict[str, Any], 
                output_format: str = "all") -> Dict[str, Any]:
        """
        Загружает трансформированные данные.
        
        Параметры:
            transform_result: Результат задачи Transform
            output_format: Формат вывода ("postgres", "csv", "excel", "all")
        
        Возвращает:
            Dict с информацией о сохранённых данных
        """
        print(f"\n{'='*60}")
        print(f"[LOAD] Загрузка данных в хранилище")
        print(f"{'='*60}")
        
        logs_df = transform_result['logs_df']
        stats = transform_result['stats']
        
        if logs_df.empty:
            print(">>> WARNING: Нет данных для загрузки")
            return {'status': 'no_data', 'files': []}
        
        # Создаём директорию для результатов
        ETLConfig.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        saved_files = []
        
        # Сохраняем в PostgreSQL
        if output_format in ["postgres", "all"] and POSTGRES_AVAILABLE:
            try:
                self._save_to_postgres(logs_df, stats)
                saved_files.append({'type': 'postgres', 'table': 'log_entries'})
            except Exception as e:
                print(f">>> WARNING: Не удалось сохранить в PostgreSQL: {e}")
        
        # Сохраняем в CSV
        if output_format in ["csv", "all"]:
            csv_path = ETLConfig.OUTPUT_DIR / f"logs_{timestamp}.csv"
            logs_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print(f">>> Сохранено в CSV: {csv_path}")
            saved_files.append({'type': 'csv', 'path': str(csv_path)})
        
        # Сохраняем в Excel
        if output_format in ["excel", "all"]:
            excel_path = ETLConfig.OUTPUT_DIR / f"logs_{timestamp}.xlsx"
            logs_df.to_excel(excel_path, index=False, engine='openpyxl')
            print(f">>> Сохранено в Excel: {excel_path}")
            saved_files.append({'type': 'excel', 'path': str(excel_path)})
        
        # Сохраняем статистику
        stats_path = ETLConfig.OUTPUT_DIR / f"stats_{timestamp}.json"
        import json
        with open(stats_path, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        saved_files.append({'type': 'json', 'path': str(stats_path)})
        
        return {
            'status': 'success',
            'records_loaded': len(logs_df),
            'files': saved_files,
            'loaded_at': datetime.now().isoformat()
        }
    
    def _save_to_postgres(self, df: pd.DataFrame, stats: Dict):
        """Сохраняет данные в PostgreSQL"""
        print(">>> Сохранение в PostgreSQL...")
        
        conn = psycopg2.connect(
            host=ETLConfig.POSTGRES_HOST,
            port=ETLConfig.POSTGRES_PORT,
            database=ETLConfig.POSTGRES_DB,
            user=ETLConfig.POSTGRES_USER,
            password=ETLConfig.POSTGRES_PASSWORD
        )
        
        try:
            cursor = conn.cursor()
            
            # Создаём таблицу если не существует
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS log_entries (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP,
                    level VARCHAR(20),
                    category VARCHAR(100),
                    message TEXT,
                    generalized_message TEXT,
                    file_name VARCHAR(255),
                    line_number INTEGER,
                    log TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Вставляем данные батчами
            records = df.to_dict('records')
            
            insert_query = """
                INSERT INTO log_entries 
                (timestamp, level, category, message, generalized_message, 
                 file_name, line_number, log)
                VALUES %s
            """
            
            values = [
                (
                    r.get('Timestamp'),
                    r.get('Level'),
                    r.get('Category'),
                    r.get('Message'),
                    r.get('Generalized_Message'),
                    r.get('file_name'),
                    r.get('line_number'),
                    r.get('log')
                )
                for r in records
            ]
            
            execute_values(cursor, insert_query, values, page_size=ETLConfig.BATCH_SIZE)
            
            conn.commit()
            print(f">>> Загружено {len(records)} записей в PostgreSQL")
            
        finally:
            conn.close()


# =============================================================================
# ETL PIPELINE
# =============================================================================

class ETLPipeline:
    """
    Главный класс ETL пайплайна.
    
    Координирует выполнение всех задач:
    Extract -> Transform -> Load
    """
    
    def __init__(self):
        self.extract_task = ExtractTask()
        self.transform_task = TransformTask()
        self.load_task = LoadTask()
        self.results = {}
    
    def run(self, source_path: str, output_format: str = "all") -> Dict[str, Any]:
        """
        Запускает полный ETL пайплайн.
        
        Параметры:
            source_path: Путь к источнику данных
            output_format: Формат вывода
        
        Возвращает:
            Dict с результатами всех этапов
        """
        print("\n" + "=" * 70)
        print("ETL PIPELINE: Запуск обработки данных")
        print("=" * 70)
        
        start_time = datetime.now()
        
        try:
            # EXTRACT
            extract_result = self.extract_task.run(source_path)
            self.results['extract'] = extract_result
            
            # TRANSFORM
            transform_result = self.transform_task.run(extract_result)
            self.results['transform'] = transform_result
            
            # LOAD
            load_result = self.load_task.run(transform_result, output_format)
            self.results['load'] = load_result
            
            # Итоговая статистика
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            self.results['pipeline'] = {
                'status': 'success',
                'duration_seconds': duration,
                'started_at': start_time.isoformat(),
                'completed_at': end_time.isoformat()
            }
            
            print("\n" + "=" * 70)
            print(f"ETL PIPELINE: Завершено успешно за {duration:.2f} сек")
            print("=" * 70)
            
        except Exception as e:
            self.results['pipeline'] = {
                'status': 'failed',
                'error': str(e)
            }
            print(f"\n>>> ETL PIPELINE: ОШИБКА - {e}")
            raise
        
        return self.results


# =============================================================================
# ФУНКЦИИ-ОБЕРТКИ ДЛЯ ПРОСТОГО ИСПОЛЬЗОВАНИЯ
# =============================================================================

def run_etl(source_path: str, output_format: str = "all") -> Dict[str, Any]:
    """
    Простая функция для запуска ETL пайплайна.
    
    Параметры:
        source_path: Путь к источнику (ZIP, директория, файл)
        output_format: Формат вывода ("postgres", "csv", "excel", "all")
    
    Возвращает:
        Dict с результатами
    """
    pipeline = ETLPipeline()
    return pipeline.run(source_path, output_format)


def extract(source_path: str) -> Dict[str, Any]:
    """Выполняет только этап Extract"""
    task = ExtractTask()
    return task.run(source_path)


def transform(extract_result: Dict[str, Any]) -> Dict[str, Any]:
    """Выполняет только этап Transform"""
    task = TransformTask()
    return task.run(extract_result)


def load(transform_result: Dict[str, Any], output_format: str = "csv") -> Dict[str, Any]:
    """Выполняет только этап Load"""
    task = LoadTask()
    return task.run(transform_result, output_format)


# =============================================================================
# ТОЧКА ВХОДА
# =============================================================================

if __name__ == "__main__":
    print("ETL Flow Module")
    print("=" * 40)
    print("\nПример использования:")
    print("  from flows.etl_flow import run_etl")
    print("  results = run_etl('./data/logs.zip')")
    print("\nИли отдельные задачи:")
    print("  from flows.etl_flow import extract, transform, load")
    print("  ext = extract('./data/logs.zip')")
    print("  trans = transform(ext)")
    print("  result = load(trans, 'csv')")

