"""
=============================================================================
api/v1/storage.py - Управление хранением задач и результатов
=============================================================================

Этот модуль отвечает за сохранение и загрузку данных о задачах и результатах
анализа в JSON файлы. Обеспечивает персистентность данных между перезапусками.

Автор: Команда Atomichack 3.0
Дата: 2025
=============================================================================
"""

import json
import os
from typing import Dict, List, Optional, Any
from datetime import datetime
from pathlib import Path
import threading


# =============================================================================
# КОНСТАНТЫ
# =============================================================================

STORAGE_DIR = Path("storage")
TASKS_FILE = STORAGE_DIR / "tasks.json"
RESULTS_DIR = STORAGE_DIR / "results"

# Создаем директории если их нет
STORAGE_DIR.mkdir(exist_ok=True)
RESULTS_DIR.mkdir(exist_ok=True)


# =============================================================================
# КЛАСС ДЛЯ ПОТОКОБЕЗОПАСНОГО ХРАНИЛИЩА
# =============================================================================

class StorageManager:
    """
    Менеджер хранилища для задач и результатов.
    Потокобезопасный и обеспечивает консистентность данных.
    """
    
    def __init__(self):
        """Инициализация менеджера хранилища"""
        self._lock = threading.Lock()
        self._tasks: Dict[str, Dict[str, Any]] = {}
        self._load_tasks()
    
    def _load_tasks(self):
        """Загружает задачи из файла при инициализации"""
        if TASKS_FILE.exists():
            try:
                with open(TASKS_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # Преобразуем даты из строк обратно в datetime
                    for task_id, task_data in data.items():
                        self._tasks[task_id] = self._deserialize_task(task_data)
                print(f">>> Загружено {len(self._tasks)} задач из хранилища")
            except Exception as e:
                print(f">>> Ошибка загрузки задач: {e}")
                self._tasks = {}
        else:
            print(">>> Файл задач не найден, создается новый")
            self._tasks = {}
    
    def _save_tasks(self):
        """Сохраняет все задачи в файл"""
        try:
            # Сериализуем задачи для JSON
            serialized = {}
            for task_id, task_data in self._tasks.items():
                serialized[task_id] = self._serialize_task(task_data)
            
            with open(TASKS_FILE, 'w', encoding='utf-8') as f:
                json.dump(serialized, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f">>> Ошибка сохранения задач: {e}")
    
    @staticmethod
    def _serialize_task(task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Сериализует задачу для JSON (конвертирует datetime в строки)"""
        serialized = task_data.copy()
        for key, value in serialized.items():
            if isinstance(value, datetime):
                serialized[key] = value.isoformat()
        return serialized
    
    @staticmethod
    def _deserialize_task(task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Десериализует задачу из JSON (конвертирует строки в datetime)"""
        deserialized = task_data.copy()
        datetime_fields = ['created_at', 'started_at', 'completed_at', 'estimated_completion']
        for field in datetime_fields:
            if field in deserialized and deserialized[field]:
                try:
                    deserialized[field] = datetime.fromisoformat(deserialized[field])
                except (ValueError, TypeError):
                    deserialized[field] = None
        return deserialized
    
    # =========================================================================
    # МЕТОДЫ ДЛЯ РАБОТЫ С ЗАДАЧАМИ
    # =========================================================================
    
    def create_task(self, task_id: str, task_data: Dict[str, Any]) -> bool:
        """
        Создает новую задачу в хранилище.
        
        Args:
            task_id: Уникальный идентификатор задачи
            task_data: Данные задачи
            
        Returns:
            bool: True если задача создана успешно
        """
        with self._lock:
            if task_id in self._tasks:
                print(f">>> Задача {task_id} уже существует")
                return False
            
            self._tasks[task_id] = task_data
            self._save_tasks()
            return True
    
    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Получает данные задачи по ID.
        
        Args:
            task_id: ID задачи
            
        Returns:
            Dict или None если задача не найдена
        """
        with self._lock:
            return self._tasks.get(task_id)
    
    def update_task(self, task_id: str, updates: Dict[str, Any]) -> bool:
        """
        Обновляет данные задачи.
        
        Args:
            task_id: ID задачи
            updates: Словарь с обновлениями
            
        Returns:
            bool: True если обновление успешно
        """
        with self._lock:
            if task_id not in self._tasks:
                return False
            
            self._tasks[task_id].update(updates)
            self._save_tasks()
            return True
    
    def delete_task(self, task_id: str) -> bool:
        """
        Удаляет задачу из хранилища.
        
        Args:
            task_id: ID задачи
            
        Returns:
            bool: True если задача удалена
        """
        with self._lock:
            if task_id not in self._tasks:
                return False
            
            del self._tasks[task_id]
            self._save_tasks()
            return True
    
    def get_all_tasks(self) -> Dict[str, Dict[str, Any]]:
        """
        Возвращает все задачи.
        
        Returns:
            Dict: Словарь всех задач
        """
        with self._lock:
            return self._tasks.copy()
    
    def get_tasks_by_status(self, status: str) -> List[Dict[str, Any]]:
        """
        Возвращает задачи с указанным статусом.
        
        Args:
            status: Статус задачи (pending, processing, completed, failed)
            
        Returns:
            List: Список задач с указанным статусом
        """
        with self._lock:
            return [
                {**task, 'task_id': task_id}
                for task_id, task in self._tasks.items()
                if task.get('status') == status
            ]
    
    def count_tasks_by_status(self, status: str) -> int:
        """
        Подсчитывает количество задач с указанным статусом.
        
        Args:
            status: Статус задачи
            
        Returns:
            int: Количество задач
        """
        with self._lock:
            return sum(1 for task in self._tasks.values() if task.get('status') == status)
    
    # =========================================================================
    # МЕТОДЫ ДЛЯ РАБОТЫ С РЕЗУЛЬТАТАМИ
    # =========================================================================
    
    def save_result(self, task_id: str, result_data: Dict[str, Any]) -> bool:
        """
        Сохраняет результаты анализа в отдельный файл.
        
        Args:
            task_id: ID задачи
            result_data: Данные результата
            
        Returns:
            bool: True если сохранение успешно
        """
        result_file = RESULTS_DIR / f"{task_id}.json"
        try:
            # Сериализуем результаты
            serialized = self._serialize_task(result_data)
            
            with open(result_file, 'w', encoding='utf-8') as f:
                json.dump(serialized, f, indent=2, ensure_ascii=False)
            
            print(f">>> Результаты задачи {task_id} сохранены")
            return True
        except Exception as e:
            print(f">>> Ошибка сохранения результатов {task_id}: {e}")
            return False
    
    def get_result(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Загружает результаты анализа из файла.
        
        Args:
            task_id: ID задачи
            
        Returns:
            Dict или None если результаты не найдены
        """
        result_file = RESULTS_DIR / f"{task_id}.json"
        if not result_file.exists():
            return None
        
        try:
            with open(result_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return self._deserialize_task(data)
        except Exception as e:
            print(f">>> Ошибка загрузки результатов {task_id}: {e}")
            return None
    
    def delete_result(self, task_id: str) -> bool:
        """
        Удаляет файл результатов анализа.
        
        Args:
            task_id: ID задачи
            
        Returns:
            bool: True если удаление успешно
        """
        result_file = RESULTS_DIR / f"{task_id}.json"
        if not result_file.exists():
            return False
        
        try:
            result_file.unlink()
            print(f">>> Результаты задачи {task_id} удалены")
            return True
        except Exception as e:
            print(f">>> Ошибка удаления результатов {task_id}: {e}")
            return False
    
    def save_result_zip(self, task_id: str, zip_data: bytes) -> bool:
        """
        Сохраняет ZIP архив с результатами.
        
        Args:
            task_id: ID задачи
            zip_data: Бинарные данные ZIP файла
            
        Returns:
            bool: True если сохранение успешно
        """
        zip_file = RESULTS_DIR / f"{task_id}.zip"
        try:
            with open(zip_file, 'wb') as f:
                f.write(zip_data)
            print(f">>> ZIP архив задачи {task_id} сохранен")
            return True
        except Exception as e:
            print(f">>> Ошибка сохранения ZIP {task_id}: {e}")
            return False
    
    def get_result_zip(self, task_id: str) -> Optional[bytes]:
        """
        Загружает ZIP архив с результатами.
        
        Args:
            task_id: ID задачи
            
        Returns:
            bytes или None если ZIP не найден
        """
        zip_file = RESULTS_DIR / f"{task_id}.zip"
        if not zip_file.exists():
            return None
        
        try:
            with open(zip_file, 'rb') as f:
                return f.read()
        except Exception as e:
            print(f">>> Ошибка загрузки ZIP {task_id}: {e}")
            return None
    
    def delete_result_zip(self, task_id: str) -> bool:
        """
        Удаляет ZIP архив с результатами.
        
        Args:
            task_id: ID задачи
            
        Returns:
            bool: True если удаление успешно
        """
        zip_file = RESULTS_DIR / f"{task_id}.zip"
        if not zip_file.exists():
            return False
        
        try:
            zip_file.unlink()
            print(f">>> ZIP архив задачи {task_id} удален")
            return True
        except Exception as e:
            print(f">>> Ошибка удаления ZIP {task_id}: {e}")
            return False


# =============================================================================
# ГЛОБАЛЬНЫЙ ЭКЗЕМПЛЯР МЕНЕДЖЕРА
# =============================================================================

# Создаем единственный экземпляр менеджера хранилища
storage_manager = StorageManager()


# =============================================================================
# УДОБНЫЕ ФУНКЦИИ-ОБЕРТКИ
# =============================================================================

def create_task(task_id: str, task_data: Dict[str, Any]) -> bool:
    """Создает новую задачу"""
    return storage_manager.create_task(task_id, task_data)


def get_task(task_id: str) -> Optional[Dict[str, Any]]:
    """Получает данные задачи"""
    return storage_manager.get_task(task_id)


def update_task(task_id: str, updates: Dict[str, Any]) -> bool:
    """Обновляет задачу"""
    return storage_manager.update_task(task_id, updates)


def delete_task(task_id: str) -> bool:
    """Удаляет задачу"""
    return storage_manager.delete_task(task_id)


def get_all_tasks() -> Dict[str, Dict[str, Any]]:
    """Возвращает все задачи"""
    return storage_manager.get_all_tasks()


def get_tasks_by_status(status: str) -> List[Dict[str, Any]]:
    """Возвращает задачи по статусу"""
    return storage_manager.get_tasks_by_status(status)


def count_tasks_by_status(status: str) -> int:
    """Подсчитывает задачи по статусу"""
    return storage_manager.count_tasks_by_status(status)


def save_result(task_id: str, result_data: Dict[str, Any]) -> bool:
    """Сохраняет результаты"""
    return storage_manager.save_result(task_id, result_data)


def get_result(task_id: str) -> Optional[Dict[str, Any]]:
    """Получает результаты"""
    return storage_manager.get_result(task_id)


def delete_result(task_id: str) -> bool:
    """Удаляет результаты"""
    return storage_manager.delete_result(task_id)


def save_result_zip(task_id: str, zip_data: bytes) -> bool:
    """Сохраняет ZIP с результатами"""
    return storage_manager.save_result_zip(task_id, zip_data)


def get_result_zip(task_id: str) -> Optional[bytes]:
    """Получает ZIP с результатами"""
    return storage_manager.get_result_zip(task_id)


def delete_result_zip(task_id: str) -> bool:
    """Удаляет ZIP с результатами"""
    return storage_manager.delete_result_zip(task_id)

