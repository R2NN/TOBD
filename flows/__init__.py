"""
=============================================================================
flows/ - ETL Pipeline модуль
=============================================================================

Этот модуль содержит реализацию ETL (Extract, Transform, Load) пайплайна
для обработки и анализа лог-файлов.

Основные компоненты:
- etl_flow.py: Главный ETL пайплайн с задачами Extract, Transform, Load

Использование:
    from flows.etl_flow import run_etl, extract, transform, load
    
    # Полный ETL
    results = run_etl('./data/logs.zip')
    
    # Отдельные задачи
    ext = extract('./data/logs.zip')
    trans = transform(ext)
    result = load(trans, 'csv')

Автор: Команда Big Data Project
Дата: 2025
=============================================================================
"""

from .etl_flow import (
    run_etl,
    extract,
    transform,
    load,
    ETLPipeline,
    ExtractTask,
    TransformTask,
    LoadTask,
    ETLConfig
)

__all__ = [
    'run_etl',
    'extract',
    'transform',
    'load',
    'ETLPipeline',
    'ExtractTask',
    'TransformTask',
    'LoadTask',
    'ETLConfig'
]

