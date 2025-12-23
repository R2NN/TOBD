"""
=============================================================================
api/v1/tasks.py - Управление задачами и rate limiting
=============================================================================

Этот модуль управляет очередью задач обработки, контролирует количество
одновременно выполняющихся задач и реализует rate limiting.

Автор: Команда Atomichack 3.0
Дата: 2025
=============================================================================
"""

import asyncio
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Callable
import threading

from . import storage


# =============================================================================
# КОНСТАНТЫ
# =============================================================================

# Максимальное количество одновременно обрабатываемых задач
MAX_CONCURRENT_TASKS = 10

# Оценка среднего времени обработки для расчета ETA (в секундах)
AVG_PROCESSING_TIME_LIGHT = 120  # 2 минуты для light модели
AVG_PROCESSING_TIME_HEAVY = 300  # 5 минут для heavy модели


# =============================================================================
# КЛАСС МЕНЕДЖЕРА ЗАДАЧ
# =============================================================================

class TaskManager:
    """
    Менеджер задач для управления очередью обработки и rate limiting.
    Обеспечивает ограничение количества одновременных задач.
    """
    
    def __init__(self):
        """Инициализация менеджера задач"""
        self._lock = threading.Lock()
        self._processing_tasks: Dict[str, Any] = {}  # Текущие обрабатываемые задачи
        self._pending_queue: list = []  # Очередь ожидающих задач
    
    def can_create_task(self) -> bool:
        """
        Проверяет, можно ли создать новую задачу (не превышен лимит).
        
        Returns:
            bool: True если можно создать задачу
        """
        with self._lock:
            # Подсчитываем задачи в статусе processing
            processing_count = storage.count_tasks_by_status('processing')
            pending_count = storage.count_tasks_by_status('pending')
            
            total_active = processing_count + pending_count
            
            return total_active < MAX_CONCURRENT_TASKS
    
    def get_current_load(self) -> Dict[str, int]:
        """
        Возвращает информацию о текущей загрузке.
        
        Returns:
            Dict: Статистика загрузки
        """
        with self._lock:
            processing = storage.count_tasks_by_status('processing')
            pending = storage.count_tasks_by_status('pending')
            
            return {
                'processing': processing,
                'pending': pending,
                'total_active': processing + pending,
                'max_concurrent': MAX_CONCURRENT_TASKS,
                'available_slots': max(0, MAX_CONCURRENT_TASKS - (processing + pending))
            }
    
    def create_task(
        self,
        filename: str,
        model: str = "light",
        file_content: Optional[bytes] = None
    ) -> str:
        """
        Создает новую задачу в системе.
        
        Args:
            filename: Имя файла
            model: Модель для обработки (light или heavy)
            file_content: Содержимое файла (опционально)
            
        Returns:
            str: ID созданной задачи
        """
        task_id = str(uuid.uuid4())
        
        # Вычисляем оценочное время завершения
        avg_time = AVG_PROCESSING_TIME_LIGHT if model == "light" else AVG_PROCESSING_TIME_HEAVY
        estimated_completion = datetime.now() + timedelta(seconds=avg_time)
        
        # Создаем данные задачи
        task_data = {
            'task_id': task_id,
            'filename': filename,
            'model': model,
            'status': 'pending',
            'progress': 0,
            'created_at': datetime.now(),
            'started_at': None,
            'completed_at': None,
            'estimated_completion': estimated_completion,
            'error_message': None
        }
        
        # Сохраняем в storage
        storage.create_task(task_id, task_data)
        
        print(f">>> Создана задача {task_id}: {filename} ({model})")
        
        return task_id
    
    def start_task(self, task_id: str) -> bool:
        """
        Запускает задачу (меняет статус на processing).
        
        Args:
            task_id: ID задачи
            
        Returns:
            bool: True если задача запущена успешно
        """
        with self._lock:
            task = storage.get_task(task_id)
            if not task:
                return False
            
            # Проверяем лимит одновременных задач
            processing_count = storage.count_tasks_by_status('processing')
            if processing_count >= MAX_CONCURRENT_TASKS:
                print(f">>> Невозможно запустить задачу {task_id}: превышен лимит")
                return False
            
            # Обновляем статус
            updates = {
                'status': 'processing',
                'started_at': datetime.now(),
                'progress': 0
            }
            
            storage.update_task(task_id, updates)
            self._processing_tasks[task_id] = task
            
            print(f">>> Задача {task_id} запущена")
            return True
    
    def update_progress(self, task_id: str, progress: int, message: Optional[str] = None) -> bool:
        """
        Обновляет прогресс задачи.
        
        Args:
            task_id: ID задачи
            progress: Прогресс (0-100)
            message: Опциональное сообщение
            
        Returns:
            bool: True если обновление успешно
        """
        updates = {'progress': min(100, max(0, progress))}
        
        if message:
            updates['last_message'] = message
        
        # Пересчитываем ETA на основе текущего прогресса
        task = storage.get_task(task_id)
        if task and task.get('started_at') and progress > 0:
            elapsed = (datetime.now() - task['started_at']).total_seconds()
            estimated_total = (elapsed / progress) * 100
            remaining = estimated_total - elapsed
            updates['estimated_completion'] = datetime.now() + timedelta(seconds=remaining)
        
        return storage.update_task(task_id, updates)
    
    def complete_task(
        self,
        task_id: str,
        success: bool = True,
        error_message: Optional[str] = None
    ) -> bool:
        """
        Завершает задачу.
        
        Args:
            task_id: ID задачи
            success: Успешно ли завершена
            error_message: Сообщение об ошибке (если не успешно)
            
        Returns:
            bool: True если обновление успешно
        """
        with self._lock:
            if task_id in self._processing_tasks:
                del self._processing_tasks[task_id]
            
            updates = {
                'status': 'completed' if success else 'failed',
                'progress': 100 if success else storage.get_task(task_id).get('progress', 0),
                'completed_at': datetime.now(),
                'error_message': error_message
            }
            
            result = storage.update_task(task_id, updates)
            
            status_text = "завершена" if success else "завершена с ошибкой"
            print(f">>> Задача {task_id} {status_text}")
            
            # Проверяем, можем ли запустить следующую задачу из очереди
            self._try_start_next_pending_task()
            
            return result
    
    def _try_start_next_pending_task(self):
        """
        Пытается запустить следующую задачу из очереди pending.
        Вызывается автоматически при завершении задачи.
        """
        # Получаем задачи в статусе pending
        pending_tasks = storage.get_tasks_by_status('pending')
        
        if not pending_tasks:
            return
        
        # Сортируем по времени создания (FIFO)
        pending_tasks.sort(key=lambda x: x.get('created_at', datetime.now()))
        
        # Пытаемся запустить первую задачу
        for task in pending_tasks:
            task_id = task.get('task_id')
            if task_id and self.start_task(task_id):
                print(f">>> Автоматически запущена задача из очереди: {task_id}")
                break
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Получает полную информацию о статусе задачи.
        
        Args:
            task_id: ID задачи
            
        Returns:
            Dict с информацией о задаче или None
        """
        return storage.get_task(task_id)
    
    def delete_task(self, task_id: str) -> bool:
        """
        Удаляет задачу и связанные с ней результаты.
        
        Args:
            task_id: ID задачи
            
        Returns:
            bool: True если удаление успешно
        """
        with self._lock:
            # Удаляем из обрабатываемых
            if task_id in self._processing_tasks:
                del self._processing_tasks[task_id]
            
            # Удаляем результаты
            storage.delete_result(task_id)
            storage.delete_result_zip(task_id)
            
            # Удаляем задачу
            result = storage.delete_task(task_id)
            
            if result:
                print(f">>> Задача {task_id} удалена")
            
            return result
    
    def get_history(
        self,
        skip: int = 0,
        limit: int = 10,
        status: Optional[str] = None,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Получает историю задач с фильтрацией и пагинацией.
        
        Args:
            skip: Количество пропущенных записей
            limit: Максимальное количество записей
            status: Фильтр по статусу
            date_from: Начальная дата
            date_to: Конечная дата
            
        Returns:
            Dict: История задач с метаданными
        """
        all_tasks = storage.get_all_tasks()
        
        # Фильтрация
        filtered_tasks = []
        for task_id, task in all_tasks.items():
            # Фильтр по статусу
            if status and task.get('status') != status:
                continue
            
            # Фильтр по дате
            created_at = task.get('created_at')
            if created_at:
                if date_from and created_at < date_from:
                    continue
                if date_to and created_at > date_to:
                    continue
            
            filtered_tasks.append({**task, 'task_id': task_id})
        
        # Сортировка по времени создания (новые сначала)
        filtered_tasks.sort(
            key=lambda x: x.get('created_at', datetime.min),
            reverse=True
        )
        
        # Пагинация
        total = len(filtered_tasks)
        paginated_tasks = filtered_tasks[skip:skip + limit]
        
        return {
            'items': paginated_tasks,
            'total': total,
            'skip': skip,
            'limit': limit
        }


# =============================================================================
# ГЛОБАЛЬНЫЙ ЭКЗЕМПЛЯР МЕНЕДЖЕРА ЗАДАЧ
# =============================================================================

# Создаем единственный экземпляр менеджера задач
task_manager = TaskManager()


# =============================================================================
# ФУНКЦИЯ ДЛЯ ОБРАБОТКИ ЗАДАЧИ В ФОНЕ
# =============================================================================

async def process_task_background(
    task_id: str,
    file_content: bytes,
    filename: str,
    model: str,
    process_function: Callable
):
    """
    Обрабатывает задачу в фоновом режиме.
    
    Args:
        task_id: ID задачи
        file_content: Содержимое файла
        filename: Имя файла
        model: Модель для обработки
        process_function: Функция обработки (из processing модуля)
    """
    import time
    
    # Запускаем задачу
    task_manager.start_task(task_id)
    
    # Начинаем отсчет времени
    start_time = time.time()
    
    try:
        # Формируем имя выходного файла
        import os
        base_filename = os.path.splitext(filename)[0]
        model_suffix = "Light" if model == "light" else "Heavy"
        output_filename = f"{base_filename}_Results_{model_suffix}.zip"
        
        # Создаем callback для обновления прогресса
        def progress_callback(stage: str, progress: int, message: str):
            task_manager.update_progress(task_id, progress, message)
            print(f">>> [{task_id}] {progress}% - {stage}: {message}")
        
        # Запускаем обработку в executor
        loop = asyncio.get_event_loop()
        success, result_data, metadata = await loop.run_in_executor(
            None,
            process_function,
            file_content,
            filename,
            model,
            progress_callback
        )
        
        if not success:
            # Обработка завершилась с ошибкой
            error_message = result_data.decode('utf-8') if isinstance(result_data, bytes) else str(result_data)
            task_manager.complete_task(task_id, success=False, error_message=error_message)
            return
        
        # Обработка успешна - извлекаем данные для хранения
        import zipfile
        import io
        import pandas as pd
        
        zip_buffer = io.BytesIO(result_data)
        analysis_data = {}
        
        with zipfile.ZipFile(zip_buffer, 'r') as zip_file:
            # Читаем Excel файлы
            for file_in_zip in zip_file.namelist():
                if file_in_zip.endswith('.xlsx'):
                    file_content_inner = zip_file.read(file_in_zip)
                    df = pd.read_excel(io.BytesIO(file_content_inner), engine='openpyxl')
                    # Конвертируем datetime/Timestamp в строки для JSON сериализации
                    for col in df.columns:
                        if pd.api.types.is_datetime64_any_dtype(df[col]):
                            df[col] = df[col].astype(str)
                    analysis_data[file_in_zip] = df.to_dict('records')
                elif file_in_zip.endswith('.csv'):
                    file_content_inner = zip_file.read(file_in_zip).decode('utf-8')
                    # Простой парсинг CSV
                    lines = file_content_inner.split('\n')
                    if len(lines) > 1:
                        headers = lines[0].split(';')
                        data = []
                        for line in lines[1:]:
                            if line.strip():
                                values = line.split(';')
                                row = {
                                    headers[i].strip(): values[i].strip() if i < len(values) else ''
                                    for i in range(len(headers))
                                }
                                data.append(row)
                        analysis_data[file_in_zip] = data
        
        # Подсчитываем статистику из реальных данных
        total_records = sum(len(data) for data in analysis_data.values() if isinstance(data, list))
        processing_time = time.time() - start_time
        
        # Подсчитываем реальное количество аномалий из отчета
        submit_report = analysis_data.get('submit_report.xlsx', [])
        total_anomalies = len(submit_report) if isinstance(submit_report, list) else 0
        
        # Подсчитываем УНИКАЛЬНЫЕ ошибки и предупреждения из строк логов
        unique_errors = set()
        unique_warnings = set()
        for item in submit_report:
            if isinstance(item, dict):
                log_line = item.get('Строка из лога', '')
                if 'ERROR' in log_line:
                    unique_errors.add(log_line)
                elif 'WARNING' in log_line:
                    unique_warnings.add(log_line)
        
        total_errors = len(unique_errors)
        total_warnings = len(unique_warnings)
        
        # Сохраняем результаты
        result_metadata = {
            'task_id': task_id,
            'filename': output_filename,
            'model': model,
            'created_at': storage.get_task(task_id).get('created_at'),
            'completed_at': datetime.now(),
            'total_logs': total_records,
            'total_errors': total_errors,
            'total_warnings': total_warnings,
            'total_anomalies': total_anomalies,
            'processing_time_seconds': processing_time,
            'data': analysis_data
        }
        
        storage.save_result(task_id, result_metadata)
        storage.save_result_zip(task_id, result_data)
        
        # Завершаем задачу успешно
        task_manager.complete_task(task_id, success=True)
        
        print(f">>> [{task_id}] Обработка завершена успешно за {processing_time:.1f}s")
        
    except Exception as e:
        print(f">>> [{task_id}] Ошибка обработки: {e}")
        import traceback
        traceback.print_exc()
        
        task_manager.complete_task(task_id, success=False, error_message=str(e))

