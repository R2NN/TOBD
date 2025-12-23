"""
=============================================================================
dask_jobs/ - Модуль параллельной обработки с Dask
=============================================================================

Этот модуль предоставляет функции для параллельной обработки больших
объёмов данных с использованием Dask.

Основные компоненты:
- dask_processing.py: Функции для параллельного парсинга и обработки логов

Возможности:
- Параллельный парсинг множества лог-файлов
- Распределённая генерация эмбеддингов
- Batch-обработка ML-классификации
- Dask Bag для потоковой обработки
- Dask DataFrame для больших CSV файлов

Использование:
    from dask_jobs.dask_processing import (
        init_dask_cluster,
        parallel_parse_logs,
        run_dask_pipeline
    )
    
    # Инициализация кластера
    client = init_dask_cluster(n_workers=4)
    
    # Параллельный парсинг
    df = parallel_parse_logs('./data/logs/')
    
    # Полный пайплайн
    df = run_dask_pipeline('./data/logs/')

Автор: Команда Big Data Project
Дата: 2025
=============================================================================
"""

from .dask_processing import (
    init_dask_cluster,
    shutdown_dask_cluster,
    parallel_parse_logs,
    process_logs_with_bag,
    parallel_generate_embeddings,
    load_large_csv_with_dask,
    aggregate_with_dask,
    run_dask_pipeline,
    DaskConfig
)

__all__ = [
    'init_dask_cluster',
    'shutdown_dask_cluster',
    'parallel_parse_logs',
    'process_logs_with_bag',
    'parallel_generate_embeddings',
    'load_large_csv_with_dask',
    'aggregate_with_dask',
    'run_dask_pipeline',
    'DaskConfig'
]

