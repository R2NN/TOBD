"""
=============================================================================
example_etl_usage.py - Примеры использования ETL и Dask
=============================================================================

Этот файл демонстрирует использование:
1. ETL Pipeline (Extract → Transform → Load)
2. Dask для параллельной обработки
3. Работу с PostgreSQL

Автор: Команда Big Data Project
Дата: 2025
=============================================================================
"""

import os
import sys

# Добавляем текущую директорию в путь
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def example_full_etl_pipeline():
    """
    Пример 1: Полный ETL Pipeline
    
    Демонстрирует запуск полного цикла обработки данных:
    Extract → Transform → Load
    """
    print("=" * 60)
    print("ПРИМЕР 1: Полный ETL Pipeline")
    print("=" * 60)
    
    from flows.etl_flow import run_etl
    
    # Путь к данным (ZIP-архив или директория)
    source_path = "./data/sample_logs.zip"  # Замените на реальный путь
    
    # Запуск ETL
    results = run_etl(source_path, output_format="csv")
    
    print("\nРезультаты:")
    print(f"  - Статус: {results['pipeline']['status']}")
    print(f"  - Длительность: {results['pipeline'].get('duration_seconds', 0):.2f} сек")
    
    if 'load' in results:
        print(f"  - Записей загружено: {results['load'].get('records_loaded', 0)}")
        print(f"  - Файлы: {results['load'].get('files', [])}")


def example_separate_etl_tasks():
    """
    Пример 2: Раздельные ETL задачи
    
    Демонстрирует выполнение каждой задачи отдельно:
    - Extract: извлечение данных
    - Transform: трансформация
    - Load: загрузка
    """
    print("\n" + "=" * 60)
    print("ПРИМЕР 2: Раздельные ETL задачи")
    print("=" * 60)
    
    from flows.etl_flow import extract, transform, load
    
    # Шаг 1: Extract
    print("\n--- EXTRACT ---")
    source_path = "./data/sample_logs"  # Замените на реальный путь
    extract_result = extract(source_path)
    print(f"Найдено файлов: {extract_result.get('files_count', 0)}")
    
    # Шаг 2: Transform
    print("\n--- TRANSFORM ---")
    transform_result = transform(extract_result)
    stats = transform_result.get('stats', {})
    print(f"Всего записей: {stats.get('total', 0)}")
    print(f"ERROR: {stats.get('errors', 0)}")
    print(f"WARNING: {stats.get('warnings', 0)}")
    
    # Шаг 3: Load
    print("\n--- LOAD ---")
    load_result = load(transform_result, output_format="csv")
    print(f"Записей загружено: {load_result.get('records_loaded', 0)}")


def example_dask_parallel_processing():
    """
    Пример 3: Параллельная обработка с Dask
    
    Демонстрирует использование Dask для параллельного парсинга логов.
    """
    print("\n" + "=" * 60)
    print("ПРИМЕР 3: Параллельная обработка с Dask")
    print("=" * 60)
    
    from dask_jobs.dask_processing import (
        init_dask_cluster,
        parallel_parse_logs,
        shutdown_dask_cluster
    )
    
    client = None
    try:
        # Инициализация Dask кластера
        client = init_dask_cluster(n_workers=4, threads_per_worker=2)
        
        # Параллельный парсинг логов
        log_directory = "./data/logs"  # Замените на реальный путь
        df = parallel_parse_logs(log_directory)
        
        if not df.empty:
            print(f"\nРезультаты:")
            print(f"  - Всего записей: {len(df)}")
            print(f"  - ERROR: {len(df[df['Level'] == 'ERROR'])}")
            print(f"  - WARNING: {len(df[df['Level'] == 'WARNING'])}")
            print(f"  - Уникальных файлов: {df['file_name'].nunique()}")
        else:
            print("Данные не найдены")
            
    finally:
        if client:
            shutdown_dask_cluster(client)


def example_dask_full_pipeline():
    """
    Пример 4: Полный Dask Pipeline
    
    Демонстрирует использование run_dask_pipeline для полной обработки.
    """
    print("\n" + "=" * 60)
    print("ПРИМЕР 4: Полный Dask Pipeline")
    print("=" * 60)
    
    from dask_jobs.dask_processing import run_dask_pipeline
    
    log_directory = "./data/logs"  # Замените на реальный путь
    output_directory = "./storage/dask_results"
    
    df = run_dask_pipeline(
        log_directory=log_directory,
        output_directory=output_directory,
        use_cluster=True
    )
    
    if not df.empty:
        print(f"\nРезультаты сохранены в: {output_directory}")
        print(f"Обработано записей: {len(df)}")


def example_postgresql_storage():
    """
    Пример 5: Работа с PostgreSQL
    
    Демонстрирует сохранение данных в PostgreSQL.
    Требует запущенного контейнера PostgreSQL.
    """
    print("\n" + "=" * 60)
    print("ПРИМЕР 5: PostgreSQL Storage")
    print("=" * 60)
    
    from flows.etl_flow import run_etl
    
    source_path = "./data/sample_logs.zip"
    
    # Запуск ETL с сохранением в PostgreSQL
    results = run_etl(source_path, output_format="postgres")
    
    if results['pipeline']['status'] == 'success':
        print("Данные успешно сохранены в PostgreSQL")
        print("Проверьте таблицы: raw_logs, processed_logs, analysis_results")


def show_help():
    """Показывает справку по примерам"""
    print("""
╔═══════════════════════════════════════════════════════════════╗
║         ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ ETL И DASK                      ║
╠═══════════════════════════════════════════════════════════════╣
║                                                               ║
║  Перед запуском примеров:                                     ║
║  1. Запустите docker-compose up -d                            ║
║  2. Поместите данные в папку ./data/                          ║
║                                                               ║
║  Примеры:                                                     ║
║  - example_full_etl_pipeline()     # Полный ETL               ║
║  - example_separate_etl_tasks()    # Раздельные задачи        ║
║  - example_dask_parallel_processing() # Dask параллелизм      ║
║  - example_dask_full_pipeline()    # Полный Dask pipeline     ║
║  - example_postgresql_storage()    # Работа с PostgreSQL      ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝

Для запуска ETL через командную строку:

    # Полный ETL
    python -c "from flows.etl_flow import run_etl; run_etl('./data/logs.zip')"
    
    # Dask обработка
    python -c "from dask_jobs.dask_processing import run_dask_pipeline; run_dask_pipeline('./data/logs')"
""")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Примеры использования ETL и Dask")
    parser.add_argument('--example', type=int, choices=[1, 2, 3, 4, 5],
                        help="Номер примера (1-5)")
    parser.add_argument('--all', action='store_true',
                        help="Запустить все примеры")
    
    args = parser.parse_args()
    
    if args.all:
        example_full_etl_pipeline()
        example_separate_etl_tasks()
        example_dask_parallel_processing()
        example_dask_full_pipeline()
        example_postgresql_storage()
    elif args.example == 1:
        example_full_etl_pipeline()
    elif args.example == 2:
        example_separate_etl_tasks()
    elif args.example == 3:
        example_dask_parallel_processing()
    elif args.example == 4:
        example_dask_full_pipeline()
    elif args.example == 5:
        example_postgresql_storage()
    else:
        show_help()

