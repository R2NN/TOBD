"""
=============================================================================
processing/__init__.py - Инициализационный файл пакета обработки
=============================================================================

Этот модуль экспортирует основные функции пакета processing для удобного
импорта в других частях приложения.

Автор: Команда Atomichack 3.0
=============================================================================
"""

from .orchestrator import (
    run_full_analysis_from_zip_bytes,
    process_zip_archive,
    process_single_txt
)

__all__ = [
    'run_full_analysis_from_zip_bytes',
    'process_zip_archive',
    'process_single_txt'
]

