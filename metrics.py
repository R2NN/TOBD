"""
=============================================================================
metrics.py - Модуль кастомных метрик Prometheus для мониторинга
=============================================================================

Этот модуль определяет кастомные метрики для отслеживания производительности
системы анализа логов:

1. log_analysis_duration_seconds - время обработки логов
2. log_records_processed_total - количество обработанных записей
3. ml_model_inference_duration_seconds - время ML инференса
4. memory_usage_bytes - использование памяти
5. anomalies_detected_total - обнаруженные аномалии
6. problems_classified_total - классифицированные проблемы

Автор: Команда Atomichack 3.0
Дата: 2025
=============================================================================
"""

import time
import psutil
from prometheus_client import Counter, Histogram, Gauge
from typing import Optional


# =============================================================================
# ОПРЕДЕЛЕНИЕ МЕТРИК
# =============================================================================

# Счетчик обработанных записей логов
log_records_processed_total = Counter(
    'log_records_processed_total',
    'Общее количество обработанных записей логов',
    ['model_type', 'status']  # light/heavy, success/error
)

# Гистограмма времени обработки логов
log_analysis_duration_seconds = Histogram(
    'log_analysis_duration_seconds',
    'Время выполнения полного анализа логов в секундах',
    ['model_type', 'status'],
    buckets=(1, 5, 10, 30, 60, 120, 300, 600, 1200, float('inf'))
)

# Гистограмма времени ML инференса
ml_model_inference_duration_seconds = Histogram(
    'ml_model_inference_duration_seconds',
    'Время выполнения ML инференса в секундах',
    ['model_type', 'stage'],  # stage: anomaly_classification / problem_classification
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60, float('inf'))
)

# Счетчик обнаруженных аномалий
anomalies_detected_total = Counter(
    'anomalies_detected_total',
    'Общее количество обнаруженных аномалий',
    ['model_type', 'severity']  # severity: high/medium/low
)

# Счетчик классифицированных проблем
problems_classified_total = Counter(
    'problems_classified_total',
    'Общее количество классифицированных проблем',
    ['model_type', 'problem_type']
)

# Gauge использования памяти процессом
memory_usage_bytes = Gauge(
    'memory_usage_bytes',
    'Использование памяти процессом в байтах',
    ['type']  # type: rss / vms / percent
)

# Gauge количества активных WebSocket соединений
active_websocket_connections = Gauge(
    'active_websocket_connections',
    'Количество активных WebSocket соединений'
)

# Счетчик обработанных ZIP архивов
zip_archives_processed_total = Counter(
    'zip_archives_processed_total',
    'Общее количество обработанных ZIP архивов',
    ['model_type', 'status']
)

# Гистограмма времени загрузки ML модели
ml_model_loading_duration_seconds = Histogram(
    'ml_model_loading_duration_seconds',
    'Время загрузки ML модели в секундах',
    ['model_type'],
    buckets=(1, 5, 10, 30, 60, 120, float('inf'))
)

# Гистограмма размера обрабатываемых ZIP архивов
zip_archive_size_bytes = Histogram(
    'zip_archive_size_bytes',
    'Размер обрабатываемых ZIP архивов в байтах',
    ['model_type'],
    buckets=(1024, 10240, 102400, 1048576, 10485760, 104857600, float('inf'))  # 1KB to 100MB
)


# =============================================================================
# HELPER ФУНКЦИИ ДЛЯ ОБНОВЛЕНИЯ МЕТРИК
# =============================================================================

def update_memory_metrics():
    """
    Обновляет метрики использования памяти текущим процессом.
    
    Использует библиотеку psutil для получения информации о памяти:
    - RSS (Resident Set Size) - физическая память
    - VMS (Virtual Memory Size) - виртуальная память
    - Процент от общей памяти системы
    """
    try:
        process = psutil.Process()
        mem_info = process.memory_info()
        
        # Обновляем Gauge метрики
        memory_usage_bytes.labels(type='rss').set(mem_info.rss)
        memory_usage_bytes.labels(type='vms').set(mem_info.vms)
        
        # Процент от общей памяти
        mem_percent = process.memory_percent()
        memory_usage_bytes.labels(type='percent').set(mem_percent)
        
    except Exception as e:
        print(f">>> Ошибка обновления метрик памяти: {e}")


def record_log_analysis(model_type: str, status: str, duration: float, records_count: int):
    """
    Записывает метрики после завершения анализа логов.
    
    Параметры:
        model_type (str): Тип модели ('light' или 'heavy')
        status (str): Статус выполнения ('success' или 'error')
        duration (float): Продолжительность анализа в секундах
        records_count (int): Количество обработанных записей
    """
    # Обновляем счетчик обработанных записей
    log_records_processed_total.labels(model_type=model_type, status=status).inc(records_count)
    
    # Записываем время анализа
    log_analysis_duration_seconds.labels(model_type=model_type, status=status).observe(duration)
    
    # Обновляем метрики памяти
    update_memory_metrics()


def record_ml_inference(model_type: str, stage: str, duration: float):
    """
    Записывает метрики ML инференса.
    
    Параметры:
        model_type (str): Тип модели ('light' или 'heavy')
        stage (str): Этап ('anomaly_classification', 'problem_classification', 'embedding_generation')
        duration (float): Продолжительность в секундах
    """
    ml_model_inference_duration_seconds.labels(model_type=model_type, stage=stage).observe(duration)


def record_anomalies_detected(model_type: str, count: int, severity: str = 'medium'):
    """
    Записывает количество обнаруженных аномалий.
    
    Параметры:
        model_type (str): Тип модели ('light' или 'heavy')
        count (int): Количество аномалий
        severity (str): Уровень важности ('high', 'medium', 'low')
    """
    anomalies_detected_total.labels(model_type=model_type, severity=severity).inc(count)


def record_problems_classified(model_type: str, count: int, problem_type: str = 'generic'):
    """
    Записывает количество классифицированных проблем.
    
    Параметры:
        model_type (str): Тип модели ('light' или 'heavy')
        count (int): Количество проблем
        problem_type (str): Тип проблемы (generic, critical, warning)
    """
    problems_classified_total.labels(model_type=model_type, problem_type=problem_type).inc(count)


def record_zip_processed(model_type: str, status: str, size_bytes: int):
    """
    Записывает метрики обработанного ZIP архива.
    
    Параметры:
        model_type (str): Тип модели ('light' или 'heavy')
        status (str): Статус обработки ('success' или 'error')
        size_bytes (int): Размер архива в байтах
    """
    zip_archives_processed_total.labels(model_type=model_type, status=status).inc()
    zip_archive_size_bytes.labels(model_type=model_type).observe(size_bytes)


def record_model_loading(model_type: str, duration: float):
    """
    Записывает метрики загрузки ML модели.
    
    Параметры:
        model_type (str): Тип модели ('light' или 'heavy')
        duration (float): Время загрузки в секундах
    """
    ml_model_loading_duration_seconds.labels(model_type=model_type).observe(duration)


def update_websocket_count(count: int):
    """
    Обновляет количество активных WebSocket соединений.
    
    Параметры:
        count (int): Текущее количество активных соединений
    """
    active_websocket_connections.set(count)


# =============================================================================
# CONTEXT MANAGER ДЛЯ АВТОМАТИЧЕСКОГО ИЗМЕРЕНИЯ ВРЕМЕНИ
# =============================================================================

class MetricsTimer:
    """
    Контекстный менеджер для автоматического измерения времени выполнения.
    
    Пример использования:
        with MetricsTimer(metrics.record_ml_inference, 'light', 'embedding_generation'):
            # код для измерения
            embeddings = model.encode(texts)
    """
    
    def __init__(self, callback, *args, **kwargs):
        """
        Параметры:
            callback: Функция для записи метрики (принимает duration как последний аргумент)
            *args, **kwargs: Аргументы для передачи в callback
        """
        self.callback = callback
        self.args = args
        self.kwargs = kwargs
        self.start_time = None
        
    def __enter__(self):
        self.start_time = time.time()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        self.callback(*self.args, duration=duration, **self.kwargs)
        return False


# =============================================================================
# ЭКСПОРТ
# =============================================================================

__all__ = [
    'log_records_processed_total',
    'log_analysis_duration_seconds',
    'ml_model_inference_duration_seconds',
    'memory_usage_bytes',
    'anomalies_detected_total',
    'problems_classified_total',
    'active_websocket_connections',
    'zip_archives_processed_total',
    'ml_model_loading_duration_seconds',
    'zip_archive_size_bytes',
    'update_memory_metrics',
    'record_log_analysis',
    'record_ml_inference',
    'record_anomalies_detected',
    'record_problems_classified',
    'record_zip_processed',
    'record_model_loading',
    'update_websocket_count',
    'MetricsTimer',
]


