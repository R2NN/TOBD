"""
=============================================================================
api/v1/models.py - Pydantic модели для API v1
=============================================================================

Этот модуль содержит все Pydantic схемы для валидации запросов и ответов
API v1. Обеспечивает строгую типизацию и автоматическую валидацию данных.

Автор: Команда Atomichack 3.0
Дата: 2025
=============================================================================
"""

from typing import List, Optional, Dict, Any, Literal
from pydantic import BaseModel, Field, validator
from datetime import datetime
from enum import Enum


# =============================================================================
# ENUMS
# =============================================================================

class TaskStatus(str, Enum):
    """Статусы задач обработки"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class ExportFormat(str, Enum):
    """Поддерживаемые форматы экспорта"""
    JSON = "json"
    XML = "xml"
    PDF = "pdf"


# =============================================================================
# REQUEST MODELS
# =============================================================================

class BatchProcessRequest(BaseModel):
    """Запрос на batch обработку файлов"""
    model: Literal["light", "heavy"] = Field(
        default="light",
        description="Модель для анализа: light (быстрая) или heavy (точная)"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "model": "light"
            }
        }


class CompareRequest(BaseModel):
    """Запрос на сравнение результатов анализов"""
    analysis_ids: List[str] = Field(
        ...,
        min_length=2,
        description="Список ID анализов для сравнения (минимум 2)"
    )
    
    @validator('analysis_ids')
    def validate_ids_count(cls, v):
        if len(v) < 2:
            raise ValueError('Необходимо минимум 2 analysis_id для сравнения')
        if len(v) > 10:
            raise ValueError('Максимум 10 analysis_id для сравнения')
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "analysis_ids": [
                    "550e8400-e29b-41d4-a716-446655440000",
                    "550e8400-e29b-41d4-a716-446655440001"
                ]
            }
        }


class HistoryFilterRequest(BaseModel):
    """Параметры фильтрации истории"""
    skip: int = Field(default=0, ge=0, description="Количество записей для пропуска")
    limit: int = Field(default=10, ge=1, le=100, description="Максимальное количество записей")
    status: Optional[TaskStatus] = Field(default=None, description="Фильтр по статусу")
    date_from: Optional[datetime] = Field(default=None, description="Дата начала периода")
    date_to: Optional[datetime] = Field(default=None, description="Дата окончания периода")
    
    class Config:
        json_schema_extra = {
            "example": {
                "skip": 0,
                "limit": 10,
                "status": "completed",
                "date_from": "2025-01-01T00:00:00",
                "date_to": "2025-12-31T23:59:59"
            }
        }


# =============================================================================
# RESPONSE MODELS
# =============================================================================

class TaskCreatedResponse(BaseModel):
    """Ответ при создании задачи"""
    task_id: str = Field(..., description="Уникальный идентификатор задачи")
    status: TaskStatus = Field(..., description="Текущий статус задачи")
    message: str = Field(..., description="Информационное сообщение")
    
    class Config:
        json_schema_extra = {
            "example": {
                "task_id": "550e8400-e29b-41d4-a716-446655440000",
                "status": "pending",
                "message": "Задача создана и добавлена в очередь"
            }
        }


class BatchProcessResponse(BaseModel):
    """Ответ на batch обработку"""
    tasks: List[TaskCreatedResponse] = Field(..., description="Список созданных задач")
    total_files: int = Field(..., description="Общее количество файлов")
    queued: int = Field(..., description="Количество задач в очереди")
    
    class Config:
        json_schema_extra = {
            "example": {
                "tasks": [
                    {
                        "task_id": "550e8400-e29b-41d4-a716-446655440000",
                        "status": "pending",
                        "message": "Задача создана"
                    }
                ],
                "total_files": 3,
                "queued": 3
            }
        }


class TaskStatusResponse(BaseModel):
    """Ответ со статусом задачи"""
    task_id: str = Field(..., description="ID задачи")
    status: TaskStatus = Field(..., description="Текущий статус")
    progress: int = Field(..., ge=0, le=100, description="Прогресс выполнения (0-100%)")
    created_at: datetime = Field(..., description="Время создания задачи")
    started_at: Optional[datetime] = Field(None, description="Время начала обработки")
    completed_at: Optional[datetime] = Field(None, description="Время завершения")
    estimated_completion: Optional[datetime] = Field(None, description="Оценочное время завершения")
    filename: Optional[str] = Field(None, description="Имя обрабатываемого файла")
    model: str = Field(..., description="Используемая модель")
    error_message: Optional[str] = Field(None, description="Сообщение об ошибке (если есть)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "task_id": "550e8400-e29b-41d4-a716-446655440000",
                "status": "processing",
                "progress": 45,
                "created_at": "2025-10-23T10:00:00",
                "started_at": "2025-10-23T10:01:00",
                "completed_at": None,
                "estimated_completion": "2025-10-23T10:05:00",
                "filename": "logs_archive.zip",
                "model": "light",
                "error_message": None
            }
        }


class AnalysisMetadata(BaseModel):
    """Метаданные анализа"""
    task_id: str
    filename: str
    model: str
    created_at: datetime
    completed_at: Optional[datetime]
    total_logs: int = 0
    total_errors: int = 0
    total_warnings: int = 0
    total_anomalies: int = 0
    processing_time_seconds: float = 0.0


class ComparisonItem(BaseModel):
    """Элемент сравнения результатов"""
    analysis_id: str
    filename: str
    model: str
    completed_at: datetime
    total_logs: int
    total_errors: int
    total_warnings: int
    total_anomalies: int
    processing_time: float
    
    class Config:
        json_schema_extra = {
            "example": {
                "analysis_id": "550e8400-e29b-41d4-a716-446655440000",
                "filename": "logs_2025_01.zip",
                "model": "light",
                "completed_at": "2025-10-23T10:05:00",
                "total_logs": 1500,
                "total_errors": 45,
                "total_warnings": 120,
                "total_anomalies": 8,
                "processing_time": 125.5
            }
        }


class CompareResponse(BaseModel):
    """Ответ сравнения результатов"""
    comparisons: List[ComparisonItem] = Field(..., description="Список сравниваемых результатов")
    summary: Dict[str, Any] = Field(..., description="Сводная статистика")
    
    class Config:
        json_schema_extra = {
            "example": {
                "comparisons": [
                    {
                        "analysis_id": "id1",
                        "filename": "logs_2025_01.zip",
                        "model": "light",
                        "completed_at": "2025-10-23T10:05:00",
                        "total_logs": 1500,
                        "total_errors": 45,
                        "total_warnings": 120,
                        "total_anomalies": 8,
                        "processing_time": 125.5
                    }
                ],
                "summary": {
                    "max_errors": 45,
                    "min_errors": 30,
                    "avg_processing_time": 115.2
                }
            }
        }


class HistoryItem(BaseModel):
    """Элемент истории анализов"""
    task_id: str
    filename: str
    model: str
    status: TaskStatus
    created_at: datetime
    completed_at: Optional[datetime]
    progress: int
    
    class Config:
        json_schema_extra = {
            "example": {
                "task_id": "550e8400-e29b-41d4-a716-446655440000",
                "filename": "logs.zip",
                "model": "light",
                "status": "completed",
                "created_at": "2025-10-23T10:00:00",
                "completed_at": "2025-10-23T10:05:00",
                "progress": 100
            }
        }


class HistoryResponse(BaseModel):
    """Ответ с историей анализов"""
    items: List[HistoryItem] = Field(..., description="Список анализов")
    total: int = Field(..., description="Общее количество записей")
    skip: int = Field(..., description="Пропущено записей")
    limit: int = Field(..., description="Лимит записей")
    
    class Config:
        json_schema_extra = {
            "example": {
                "items": [
                    {
                        "task_id": "550e8400-e29b-41d4-a716-446655440000",
                        "filename": "logs.zip",
                        "model": "light",
                        "status": "completed",
                        "created_at": "2025-10-23T10:00:00",
                        "completed_at": "2025-10-23T10:05:00",
                        "progress": 100
                    }
                ],
                "total": 25,
                "skip": 0,
                "limit": 10
            }
        }


class ExportResponse(BaseModel):
    """Ответ на экспорт результатов"""
    task_id: str
    format: ExportFormat
    download_url: str
    file_size: int
    expires_at: Optional[datetime]
    
    class Config:
        json_schema_extra = {
            "example": {
                "task_id": "550e8400-e29b-41d4-a716-446655440000",
                "format": "json",
                "download_url": "/api/v1/export/550e8400-e29b-41d4-a716-446655440000/json",
                "file_size": 245678,
                "expires_at": None
            }
        }


class ErrorResponse(BaseModel):
    """Стандартный ответ об ошибке"""
    error: str = Field(..., description="Тип ошибки")
    message: str = Field(..., description="Описание ошибки")
    details: Optional[Dict[str, Any]] = Field(None, description="Дополнительные детали")
    
    class Config:
        json_schema_extra = {
            "example": {
                "error": "rate_limit_exceeded",
                "message": "Превышен лимит одновременных задач (максимум 10)",
                "details": {
                    "current_tasks": 10,
                    "max_tasks": 10
                }
            }
        }


class DeleteResponse(BaseModel):
    """Ответ на удаление результата"""
    task_id: str
    message: str
    deleted: bool
    
    class Config:
        json_schema_extra = {
            "example": {
                "task_id": "550e8400-e29b-41d4-a716-446655440000",
                "message": "Результаты анализа успешно удалены",
                "deleted": True
            }
        }

