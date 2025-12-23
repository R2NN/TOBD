"""
=============================================================================
api/v1/routes.py - Роутеры API v1
=============================================================================

Этот модуль содержит все эндпоинты API v1 для расширенной функциональности:
- Batch processing
- Status tracking
- Results comparison
- Export в разных форматах
- History с фильтрацией
- Удаление результатов

Автор: Команда Atomichack 3.0
Дата: 2025
=============================================================================
"""

import asyncio
import io
import json
from typing import List, Optional
from datetime import datetime

from fastapi import (
    APIRouter, File, UploadFile, HTTPException, status,
    Query, Depends, Header, Response
)
from fastapi.responses import JSONResponse, StreamingResponse

# Импортируем модели
from .models import (
    TaskStatus, ExportFormat,
    BatchProcessRequest, CompareRequest, HistoryFilterRequest,
    BatchProcessResponse, TaskCreatedResponse, TaskStatusResponse,
    CompareResponse, ComparisonItem, HistoryResponse, HistoryItem,
    ExportResponse, ErrorResponse, DeleteResponse, AnalysisMetadata
)

# Импортируем менеджеры
from .tasks import task_manager, process_task_background
from . import storage

# Импортируем функцию обработки из основного модуля
from processing import process_zip_archive


# =============================================================================
# СОЗДАНИЕ РОУТЕРА
# =============================================================================

router = APIRouter(
    prefix="/api/v1",
    tags=["API v1"],
    responses={
        401: {"model": ErrorResponse, "description": "Unauthorized"},
        403: {"model": ErrorResponse, "description": "Forbidden"},
        429: {"model": ErrorResponse, "description": "Too Many Requests"},
    }
)


# =============================================================================
# ЗАВИСИМОСТИ
# =============================================================================

# Простая проверка API ключа
VALID_API_KEYS = {
    "demo-api-key-123",
    "test-api-key-456",
    "dev-api-key-789"
}


async def verify_api_key(x_api_key: Optional[str] = Header(None)) -> str:
    """
    Проверяет API ключ из заголовка X-API-Key.
    
    Args:
        x_api_key: API ключ из заголовка
        
    Returns:
        str: Валидный API ключ
        
    Raises:
        HTTPException: Если ключ отсутствует или невалиден
    """
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key is missing. Provide X-API-Key header."
        )
    
    if x_api_key not in VALID_API_KEYS:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API key"
        )
    
    return x_api_key


# =============================================================================
# ЭНДПОИНТЫ
# =============================================================================

@router.post(
    "/batch-process/",
    response_model=BatchProcessResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Batch обработка нескольких ZIP файлов",
    description="""
    Принимает массив ZIP файлов и создает задачи для асинхронной обработки.
    
    **Ограничения:**
    - Максимум 10 одновременных задач
    - Каждый файл должен быть в формате .zip
    - Возвращает task_id для каждого файла
    
    **Требования:**
    - Заголовок X-API-Key для авторизации
    """
)
async def batch_process(
    files: List[UploadFile] = File(..., description="Список ZIP файлов для обработки"),
    model: str = Query(default="light", regex="^(light|heavy)$", description="Модель: light или heavy"),
    api_key: str = Depends(verify_api_key)
):
    """Обрабатывает несколько ZIP файлов асинхронно"""
    
    # Проверяем лимит задач
    load = task_manager.get_current_load()
    available_slots = load['available_slots']
    
    if available_slots == 0:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail={
                "error": "rate_limit_exceeded",
                "message": f"Превышен лимит одновременных задач (максимум {task_manager.MAX_CONCURRENT_TASKS})",
                "details": load
            }
        )
    
    # Проверяем количество файлов
    if len(files) > available_slots:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail={
                "error": "too_many_files",
                "message": f"Слишком много файлов. Доступно слотов: {available_slots}",
                "details": load
            }
        )
    
    created_tasks = []
    
    # Создаем задачи для каждого файла
    for uploaded_file in files:
        filename = uploaded_file.filename
        
        # Проверяем формат файла
        if not filename.lower().endswith('.zip'):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Неверный формат файла '{filename}'. Требуется .zip"
            )
        
        # Читаем содержимое файла
        file_content = await uploaded_file.read()
        await uploaded_file.close()
        
        # Создаем задачу
        task_id = task_manager.create_task(filename, model, file_content)
        
        # Запускаем обработку в фоне
        asyncio.create_task(
            process_task_background(
                task_id,
                file_content,
                filename,
                model,
                process_zip_archive
            )
        )
        
        created_tasks.append(
            TaskCreatedResponse(
                task_id=task_id,
                status=TaskStatus.PENDING,
                message=f"Задача создана для файла '{filename}'"
            )
        )
    
    # Обновляем информацию о загрузке
    load = task_manager.get_current_load()
    
    return BatchProcessResponse(
        tasks=created_tasks,
        total_files=len(files),
        queued=load['total_active']
    )


@router.get(
    "/status/{task_id}",
    response_model=TaskStatusResponse,
    summary="Получить статус задачи",
    description="""
    Возвращает детальную информацию о статусе обработки задачи.
    
    **Информация включает:**
    - Текущий статус (pending, processing, completed, failed)
    - Прогресс в процентах
    - Время создания, начала и завершения
    - Оценочное время завершения
    - Сообщение об ошибке (если есть)
    """
)
async def get_task_status(
    task_id: str,
    api_key: str = Depends(verify_api_key)
):
    """Получает статус обработки задачи"""
    
    task = task_manager.get_task_status(task_id)
    
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Задача с ID '{task_id}' не найдена"
        )
    
    return TaskStatusResponse(
        task_id=task_id,
        status=task.get('status', 'pending'),
        progress=task.get('progress', 0),
        created_at=task.get('created_at'),
        started_at=task.get('started_at'),
        completed_at=task.get('completed_at'),
        estimated_completion=task.get('estimated_completion'),
        filename=task.get('filename'),
        model=task.get('model', 'light'),
        error_message=task.get('error_message')
    )


@router.post(
    "/compare/",
    response_model=CompareResponse,
    summary="Сравнить результаты нескольких анализов",
    description="""
    Принимает список analysis_ids и возвращает сравнительную таблицу.
    
    **Сравнение включает:**
    - Количество логов, ошибок, предупреждений, аномалий
    - Время обработки
    - Использованную модель
    - Сводную статистику
    
    **Ограничения:**
    - Минимум 2 ID для сравнения
    - Максимум 10 ID
    """
)
async def compare_results(
    request: CompareRequest,
    api_key: str = Depends(verify_api_key)
):
    """Сравнивает результаты нескольких анализов"""
    
    comparisons = []
    
    for analysis_id in request.analysis_ids:
        # Получаем результат
        result = storage.get_result(analysis_id)
        
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Результаты для ID '{analysis_id}' не найдены"
            )
        
        # Проверяем статус задачи
        task = storage.get_task(analysis_id)
        if task and task.get('status') != 'completed':
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Анализ '{analysis_id}' еще не завершен (статус: {task.get('status')})"
            )
        
        comparisons.append(
            ComparisonItem(
                analysis_id=analysis_id,
                filename=result.get('filename', 'Unknown'),
                model=result.get('model', 'light'),
                completed_at=result.get('completed_at', datetime.now()),
                total_logs=result.get('total_logs', 0),
                total_errors=result.get('total_errors', 0),
                total_warnings=result.get('total_warnings', 0),
                total_anomalies=result.get('total_anomalies', 0),
                processing_time=result.get('processing_time_seconds', 0.0)
            )
        )
    
    # Вычисляем сводную статистику
    summary = {
        "total_analyses": len(comparisons),
        "max_errors": max((c.total_errors for c in comparisons), default=0),
        "min_errors": min((c.total_errors for c in comparisons), default=0),
        "avg_errors": sum(c.total_errors for c in comparisons) / len(comparisons) if comparisons else 0,
        "max_warnings": max((c.total_warnings for c in comparisons), default=0),
        "min_warnings": min((c.total_warnings for c in comparisons), default=0),
        "avg_warnings": sum(c.total_warnings for c in comparisons) / len(comparisons) if comparisons else 0,
        "max_anomalies": max((c.total_anomalies for c in comparisons), default=0),
        "min_anomalies": min((c.total_anomalies for c in comparisons), default=0),
        "avg_anomalies": sum(c.total_anomalies for c in comparisons) / len(comparisons) if comparisons else 0,
        "max_processing_time": max((c.processing_time for c in comparisons), default=0),
        "min_processing_time": min((c.processing_time for c in comparisons), default=0),
        "avg_processing_time": sum(c.processing_time for c in comparisons) / len(comparisons) if comparisons else 0,
    }
    
    return CompareResponse(
        comparisons=comparisons,
        summary=summary
    )


@router.get(
    "/export/{task_id}/{format}",
    summary="Экспортировать результаты в указанном формате",
    description="""
    Экспортирует результаты анализа в различных форматах.
    
    **Поддерживаемые форматы:**
    - json - JSON с полными данными
    - xml - XML формат
    - pdf - PDF отчет (в разработке)
    
    **Примечание:** PDF экспорт возвращает заглушку в текущей версии
    """
)
async def export_results(
    task_id: str,
    format: ExportFormat,
    api_key: str = Depends(verify_api_key)
):
    """Экспортирует результаты в указанном формате"""
    
    # Получаем результаты
    result = storage.get_result(task_id)
    
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Результаты для задачи '{task_id}' не найдены"
        )
    
    # Проверяем статус задачи
    task = storage.get_task(task_id)
    if task and task.get('status') != 'completed':
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Задача '{task_id}' еще не завершена (статус: {task.get('status')})"
        )
    
    # Экспорт в JSON
    if format == ExportFormat.JSON:
        # Сериализуем datetime в строки
        serialized_result = {}
        for key, value in result.items():
            if isinstance(value, datetime):
                serialized_result[key] = value.isoformat()
            else:
                serialized_result[key] = value
        
        json_data = json.dumps(serialized_result, indent=2, ensure_ascii=False)
        
        return Response(
            content=json_data,
            media_type="application/json",
            headers={
                "Content-Disposition": f'attachment; filename="{task_id}_results.json"'
            }
        )
    
    # Экспорт в XML
    elif format == ExportFormat.XML:
        xml_content = _convert_to_xml(result, task_id)
        
        return Response(
            content=xml_content,
            media_type="application/xml",
            headers={
                "Content-Disposition": f'attachment; filename="{task_id}_results.xml"'
            }
        )
    
    # Экспорт в PDF
    elif format == ExportFormat.PDF:
        # PDF экспорт - базовая реализация
        pdf_content = _generate_pdf_report(result, task_id)
        
        return Response(
            content=pdf_content,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'attachment; filename="{task_id}_results.pdf"'
            }
        )
    
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Неподдерживаемый формат: {format}"
        )


@router.get(
    "/history",
    response_model=HistoryResponse,
    summary="Получить историю всех анализов",
    description="""
    Возвращает список всех выполненных анализов с фильтрацией и пагинацией.
    
    **Параметры фильтрации:**
    - status - фильтр по статусу (pending, processing, completed, failed)
    - date_from - начальная дата
    - date_to - конечная дата
    
    **Пагинация:**
    - skip - количество пропущенных записей
    - limit - максимальное количество записей (1-100)
    """
)
async def get_history(
    skip: int = Query(default=0, ge=0, description="Пропустить записей"),
    limit: int = Query(default=10, ge=1, le=100, description="Лимит записей"),
    status: Optional[TaskStatus] = Query(default=None, description="Фильтр по статусу"),
    date_from: Optional[datetime] = Query(default=None, description="Начальная дата"),
    date_to: Optional[datetime] = Query(default=None, description="Конечная дата"),
    api_key: str = Depends(verify_api_key)
):
    """Получает историю всех анализов с фильтрацией"""
    
    # Получаем историю через менеджер задач
    history_data = task_manager.get_history(
        skip=skip,
        limit=limit,
        status=status.value if status else None,
        date_from=date_from,
        date_to=date_to
    )
    
    # Формируем ответ
    items = [
        HistoryItem(
            task_id=item.get('task_id'),
            filename=item.get('filename', 'Unknown'),
            model=item.get('model', 'light'),
            status=item.get('status', 'pending'),
            created_at=item.get('created_at'),
            completed_at=item.get('completed_at'),
            progress=item.get('progress', 0)
        )
        for item in history_data['items']
    ]
    
    return HistoryResponse(
        items=items,
        total=history_data['total'],
        skip=history_data['skip'],
        limit=history_data['limit']
    )


@router.delete(
    "/result/{task_id}",
    response_model=DeleteResponse,
    summary="Удалить результаты анализа",
    description="""
    Удаляет результаты анализа и связанные данные.
    
    **Что удаляется:**
    - Метаданные задачи
    - JSON результаты
    - ZIP архив с результатами
    
    **Примечание:** Операция необратима
    """
)
async def delete_result(
    task_id: str,
    api_key: str = Depends(verify_api_key)
):
    """Удаляет результаты анализа"""
    
    # Проверяем существование задачи
    task = storage.get_task(task_id)
    
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Задача с ID '{task_id}' не найдена"
        )
    
    # Удаляем задачу и результаты
    success = task_manager.delete_task(task_id)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Ошибка при удалении результатов"
        )
    
    return DeleteResponse(
        task_id=task_id,
        message="Результаты анализа успешно удалены",
        deleted=True
    )


@router.get(
    "/download/{task_id}",
    summary="Скачать ZIP архив с результатами",
    description="""
    Скачивает ZIP архив с полными результатами анализа.
    
    Архив содержит:
    - Excel файлы с результатами
    - CSV файлы
    - Текстовые отчеты
    """
)
async def download_results(
    task_id: str,
    api_key: str = Depends(verify_api_key)
):
    """Скачивает ZIP архив с результатами"""
    
    # Получаем ZIP архив
    zip_data = storage.get_result_zip(task_id)
    
    if not zip_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"ZIP архив для задачи '{task_id}' не найден"
        )
    
    # Получаем метаданные для имени файла
    result = storage.get_result(task_id)
    filename = result.get('filename', f'{task_id}_results.zip') if result else f'{task_id}_results.zip'
    
    return Response(
        content=zip_data,
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        }
    )


# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================

def _convert_to_xml(data: dict, root_name: str = "result") -> str:
    """
    Конвертирует словарь в XML формат.
    
    Args:
        data: Данные для конвертации
        root_name: Имя корневого элемента
        
    Returns:
        str: XML строка
    """
    def dict_to_xml(d, parent_tag):
        xml_str = f"<{parent_tag}>\n"
        for key, value in d.items():
            if isinstance(value, dict):
                xml_str += dict_to_xml(value, key)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        xml_str += dict_to_xml(item, key)
                    else:
                        xml_str += f"  <{key}>{_escape_xml(str(item))}</{key}>\n"
            elif isinstance(value, datetime):
                xml_str += f"  <{key}>{value.isoformat()}</{key}>\n"
            elif value is not None:
                xml_str += f"  <{key}>{_escape_xml(str(value))}</{key}>\n"
        xml_str += f"</{parent_tag}>\n"
        return xml_str
    
    xml_header = '<?xml version="1.0" encoding="UTF-8"?>\n'
    return xml_header + dict_to_xml(data, root_name)


def _escape_xml(text: str) -> str:
    """Экранирует специальные символы для XML"""
    return (text
            .replace('&', '&amp;')
            .replace('<', '&lt;')
            .replace('>', '&gt;')
            .replace('"', '&quot;')
            .replace("'", '&apos;'))


def _generate_pdf_report(data: dict, task_id: str) -> bytes:
    """
    Генерирует PDF отчет.
    
    Примечание: Это базовая реализация. Для полноценного PDF
    потребуется библиотека типа reportlab или weasyprint.
    
    Args:
        data: Данные для отчета
        task_id: ID задачи
        
    Returns:
        bytes: PDF содержимое
    """
    # Простая заглушка - текстовый файл вместо PDF
    # В production версии здесь должна быть генерация настоящего PDF
    
    report_text = f"""
╔══════════════════════════════════════════════════════════════╗
║          ОТЧЕТ АНАЛИЗА ЛОГОВ - TASK {task_id}               ║
╚══════════════════════════════════════════════════════════════╝

Имя файла: {data.get('filename', 'N/A')}
Модель: {data.get('model', 'N/A')}
Дата создания: {data.get('created_at', 'N/A')}
Дата завершения: {data.get('completed_at', 'N/A')}

СТАТИСТИКА:
───────────────────────────────────────────────────────────────
Всего логов:         {data.get('total_logs', 0)}
Всего ошибок:        {data.get('total_errors', 0)}
Всего предупреждений: {data.get('total_warnings', 0)}
Всего аномалий:      {data.get('total_anomalies', 0)}
Время обработки:     {data.get('processing_time_seconds', 0):.2f} сек

───────────────────────────────────────────────────────────────

ПРИМЕЧАНИЕ: Это текстовая версия отчета.
Для полноценного PDF отчета требуется установка дополнительных
библиотек (reportlab, weasyprint).

Для получения полных результатов используйте:
GET /api/v1/download/{task_id}
"""
    
    return report_text.encode('utf-8')

