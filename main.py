"""
=============================================================================
main.py - Веб-сервер FastAPI для взаимодействия с пользователем
=============================================================================

Этот модуль реализует веб-интерфейс для системы анализа логов.
Предоставляет два эндпоинта:
1. GET /  - главная страница с формой загрузки
2. POST /process/ - обработка загруженного ZIP-архива

Используется Jinja2 для рендеринга HTML-шаблонов.

Автор: Команда Atomichack 3.0
Дата: 2025
=============================================================================
"""

import os
import uuid
import asyncio
from urllib.parse import quote_plus
from typing import Dict

from fastapi import FastAPI, File, UploadFile, Query, Form, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import RedirectResponse, Response, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Импортируем Prometheus для метрик
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

# Импортируем функции обработки из нашего модуля
from processing import process_zip_archive, process_single_txt

# Импортируем модуль метрик
import metrics

# Импортируем API v1 роутер и middleware
from api.v1 import router as api_v1_router
from api.v1.middleware import setup_middleware


# =============================================================================
# ИНИЦИАЛИЗАЦИЯ ПРИЛОЖЕНИЯ
# =============================================================================

# Создаем экземпляр FastAPI приложения
app = FastAPI(
    title="Сервис анализа логов Atomichack 3.0",
    description="ML-система для автоматического анализа логов и выявления проблем",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# =============================================================================
# PROMETHEUS МОНИТОРИНГ
# =============================================================================

# Примечание: Метрики регистрируются в модуле metrics.py
# Этот эндпоинт просто экспонирует их для Prometheus

print(">>> Prometheus мониторинг инициализирован")

# =============================================================================
# ПОДКЛЮЧЕНИЕ API v1 РОУТЕРА
# =============================================================================

# Подключаем API v1 роутер
app.include_router(api_v1_router)
print(">>> API v1 роутер подключен")

# Устанавливаем middleware для API v1
setup_middleware(app)

# =============================================================================

# Подключаем папку 'static' для статических файлов (CSS, изображения)
app.mount("/static", StaticFiles(directory="static"), name="static")

# Настраиваем Jinja2 для рендеринга HTML-шаблонов
templates = Jinja2Templates(directory="templates")

# Словарь для хранения активных WebSocket соединений
active_websockets: Dict[str, WebSocket] = {}

# Словарь для хранения результатов обработки по session_id
session_results: Dict[str, dict] = {}


# =============================================================================
# ЭНДПОИНТЫ
# =============================================================================

@app.get("/dashboard")
async def dashboard_page(request: Request, auto_load: bool = Query(default=False)):
    """
    Отображает страницу с аналитическим дашбордом.
    
    Эндпоинт рендерит интерактивный дашборд для визуализации
    результатов анализа логов. Если auto_load=true, автоматически
    загружает последние результаты анализа.
    
    Параметры запроса:
        auto_load (bool): Автоматически загрузить последние результаты
    
    Возвращает:
        TemplateResponse: Отрендеренный HTML-шаблон dashboard.html
    
    Примеры использования:
        GET /dashboard - открывает пустой дашборд
        GET /dashboard?auto_load=true - открывает с последними результатами
    """
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "auto_load": auto_load
        }
    )


@app.get("/api/latest-results")
async def get_latest_results():
    """
    API эндпоинт для получения последних результатов анализа.
    
    Возвращает JSON с данными последнего анализа для отображения
    на дашборде.
    
    Возвращает:
        dict: Данные анализа или сообщение об отсутствии данных
    """
    global latest_analysis_results
    
    if not latest_analysis_results:
        return {"error": "Нет доступных результатов"}
    
    return {
        "success": True,
        "data": latest_analysis_results.get('data', {}),
        "filename": latest_analysis_results.get('filename', '')
    }


@app.get("/api/download-results")
async def download_results():
    """
    API эндпоинт для скачивания последних результатов в виде ZIP.
    
    Возвращает:
        Response: ZIP-архив с результатами
    """
    global latest_analysis_results
    
    if not latest_analysis_results or 'zip' not in latest_analysis_results:
        return Response(
            content="Нет доступных результатов для скачивания",
            status_code=404
        )
    
    headers = {
        'Content-Disposition': f'attachment; filename="{latest_analysis_results["filename"]}"'
    }
    
    return Response(
        content=latest_analysis_results['zip'],
        media_type='application/zip',
        headers=headers
    )


@app.get("/")
async def main_page(request: Request, error: str | None = Query(default=None)):
    """
    Отображает главную страницу с формой загрузки файла.
    
    Эндпоинт рендерит HTML-шаблон с формой для загрузки ZIP-архива
    и выбора модели машинного обучения (легкая или тяжелая).
    
    Параметры запроса:
        error (str, optional): Сообщение об ошибке для отображения
                              (передается через URL параметр при редиректе)
    
    Возвращает:
        TemplateResponse: Отрендеренный HTML-шаблон index.html
    
    Переменные шаблона:
        - request: объект запроса FastAPI (требуется для Jinja2)
        - error_message: текст ошибки или None
    
    Примеры использования:
        GET / - отображает форму без ошибок
        GET /?error=File+not+found - отображает форму с сообщением об ошибке
    """
    # Если передан параметр error, используем его, иначе None
    error_message = error if error else None
    
    # Рендерим шаблон с передачей переменных
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "error_message": error_message
        }
    )


# Глобальная переменная для хранения последних результатов анализа
latest_analysis_results = {}


@app.websocket("/ws/progress/{session_id}")
async def websocket_progress(websocket: WebSocket, session_id: str):
    """
    WebSocket эндпоинт для отправки прогресса обработки в реальном времени.
    
    Клиент подключается к этому эндпоинту с уникальным session_id
    и получает обновления прогресса в формате JSON.
    
    Параметры:
        websocket (WebSocket): WebSocket соединение
        session_id (str): Уникальный идентификатор сессии обработки
    
    Формат сообщений:
        {
            "type": "progress",
            "stage": "Название этапа",
            "progress": 50,  # 0-100
            "message": "Детальное сообщение"
        }
        
        {
            "type": "error",
            "message": "Описание ошибки"
        }
        
        {
            "type": "complete",
            "message": "Анализ завершен успешно"
        }
    """
    await websocket.accept()
    print(f">>> WebSocket подключен для сессии: {session_id}")
    
    # Регистрируем соединение
    active_websockets[session_id] = websocket
    
    # Обновляем метрику активных WebSocket соединений
    metrics.update_websocket_count(len(active_websockets))
    
    try:
        # Держим соединение открытым и ждем сообщений от клиента
        while True:
            # Получаем данные от клиента (или просто ждем)
            data = await websocket.receive_text()
            
            # Если клиент отправил "ping", отвечаем "pong"
            if data == "ping":
                await websocket.send_json({"type": "pong"})
                
    except WebSocketDisconnect:
        print(f">>> WebSocket отключен для сессии: {session_id}")
        # Удаляем соединение из словаря
        if session_id in active_websockets:
            del active_websockets[session_id]
        # Обновляем метрику
        metrics.update_websocket_count(len(active_websockets))
    except Exception as e:
        print(f">>> Ошибка WebSocket для сессии {session_id}: {e}")
        if session_id in active_websockets:
            del active_websockets[session_id]
        # Обновляем метрику
        metrics.update_websocket_count(len(active_websockets))


async def send_progress(session_id: str, stage: str, progress: int, message: str):
    """
    Отправляет прогресс обработки через WebSocket.
    
    Параметры:
        session_id (str): Идентификатор сессии
        stage (str): Название текущего этапа
        progress (int): Процент выполнения (0-100)
        message (str): Детальное сообщение
    """
    if session_id in active_websockets:
        websocket = active_websockets[session_id]
        try:
            await websocket.send_json({
                "type": "progress",
                "stage": stage,
                "progress": progress,
                "message": message
            })
            print(f">>> WebSocket [{session_id}]: {progress}% - {stage}")
        except Exception as e:
            print(f">>> Ошибка отправки через WebSocket [{session_id}]: {e}")
            # Удаляем неработающее соединение
            if session_id in active_websockets:
                del active_websockets[session_id]


async def send_error(session_id: str, error_message: str):
    """
    Отправляет сообщение об ошибке через WebSocket.
    
    Параметры:
        session_id (str): Идентификатор сессии
        error_message (str): Описание ошибки
    """
    if session_id in active_websockets:
        websocket = active_websockets[session_id]
        try:
            await websocket.send_json({
                "type": "error",
                "message": error_message
            })
            print(f">>> WebSocket [{session_id}]: ОШИБКА - {error_message}")
        except Exception as e:
            print(f">>> Ошибка отправки ошибки через WebSocket [{session_id}]: {e}")


async def send_complete(session_id: str):
    """
    Отправляет сообщение о завершении обработки через WebSocket.
    
    Параметры:
        session_id (str): Идентификатор сессии
    """
    # Ждем подключения WebSocket до 5 секунд
    max_wait = 5
    waited = 0
    while session_id not in active_websockets and waited < max_wait:
        await asyncio.sleep(0.5)
        waited += 0.5
        print(f">>> Ожидание WebSocket подключения для [{session_id}]... ({waited}s)")
    
    if session_id in active_websockets:
        websocket = active_websockets[session_id]
        try:
            await websocket.send_json({
                "type": "complete",
                "message": "Анализ завершен успешно"
            })
            print(f">>> WebSocket [{session_id}]: ЗАВЕРШЕНО - сообщение отправлено клиенту")
            # Не закрываем соединение здесь - клиент сам закроет после получения сообщения
            # Это предотвращает ошибку "websocket.close after websocket.close"
        except Exception as e:
            print(f">>> Ошибка отправки завершения через WebSocket [{session_id}]: {e}")
            if session_id in active_websockets:
                del active_websockets[session_id]
    else:
        print(f">>> WARNING: WebSocket для [{session_id}] не подключен после {max_wait}s ожидания")


@app.post("/process/")
async def handle_file_upload(file: UploadFile = File(...), model: str = Form("light")):
    """
    Обрабатывает загруженный файл с использованием WebSocket для прогресса.
    
    Эндпоинт принимает ZIP-архив с логами и запускает асинхронный анализ.
    Возвращает session_id для подключения к WebSocket и отслеживания прогресса.
    
    Параметры формы:
        file (UploadFile): Загруженный файл (должен быть .zip)
        model (str): Выбор модели - "light" или "heavy"
    
    Возвращает:
        JSONResponse: {"session_id": "uuid"} для подключения к WebSocket
        или
        RedirectResponse: Редирект на главную с ошибкой при неверном формате
    """
    # Получаем имя загруженного файла
    filename = file.filename
    
    # Проверяем формат файла
    if not filename.lower().endswith('.zip'):
        error_message = "Неверный формат. Пожалуйста, выберите файл .zip"
        encoded_error = quote_plus(error_message)
        return RedirectResponse(
            url=f"/?error={encoded_error}", 
            status_code=303
        )
    
    # Читаем содержимое файла в память
    file_content = await file.read()
    
    # Записываем метрику размера ZIP архива
    metrics.record_zip_processed(model, 'received', len(file_content))
    
    # Закрываем файл для освобождения ресурсов
    await file.close()
    
    # Генерируем уникальный session_id
    session_id = str(uuid.uuid4())
    
    print(f">>> Создана сессия: {session_id} для файла: {filename}, модель: {model}")
    
    # Запускаем обработку в фоновом потоке
    asyncio.create_task(process_file_background(
        session_id,
        file_content,
        filename,
        model
    ))
    
    # Немедленно возвращаем session_id клиенту
    return JSONResponse(content={"session_id": session_id})


async def process_file_background(session_id: str, file_content: bytes, filename: str, model: str):
    """
    Обрабатывает файл в фоновом режиме и отправляет прогресс через WebSocket.
    
    Параметры:
        session_id (str): Уникальный идентификатор сессии
        file_content (bytes): Содержимое ZIP-архива
        filename (str): Имя файла
        model (str): Выбор модели ('light' или 'heavy')
    """
    global latest_analysis_results
    
    # Ждем 1 секунду, чтобы клиент успел подключиться к WebSocket
    await asyncio.sleep(1)
    
    # Начинаем отсчет времени для метрик
    import time
    start_time = time.time()
    
    try:
        # Формируем имя для выходного ZIP-архива
        base_filename_without_ext = os.path.splitext(filename)[0]
        model_suffix = "Light" if model == "light" else "Heavy"
        output_filename = f"{base_filename_without_ext}_Results_{model_suffix}.zip"
        
        # Получаем текущий event loop для использования в callback
        main_loop = asyncio.get_event_loop()
        
        # Создаем синхронную обертку для async callback
        def sync_progress_callback(stage: str, progress: int, message: str):
            """
            Синхронная обертка для вызова async send_progress из синхронного кода.
            Использует call_soon_threadsafe для безопасного вызова из executor thread.
            """
            try:
                # Создаем task в основном event loop из executor thread
                asyncio.run_coroutine_threadsafe(
                    send_progress(session_id, stage, progress, message),
                    main_loop
                )
            except Exception as e:
                print(f">>> Ошибка в sync_progress_callback: {e}")
        
        # Запускаем обработку в executor (т.к. process_zip_archive синхронная)
        loop = main_loop
        success, result_data, metadata = await loop.run_in_executor(
            None,
            process_zip_archive,
            file_content,
            filename,
            model,
            sync_progress_callback
        )
        
        if not success:
            # Отправляем ошибку через WebSocket
            error_message = result_data.decode('utf-8') if isinstance(result_data, bytes) else str(result_data)
            await send_error(session_id, error_message or "Произошла неизвестная ошибка")
            
            # Записываем метрики об ошибке
            duration = time.time() - start_time
            metrics.record_log_analysis(model, 'error', duration, 0)
            metrics.record_zip_processed(model, 'error', len(file_content))
            return
        
        # Обработка успешна - сохраняем результаты
        try:
            import zipfile
            import io
            import pandas as pd
            
            print(f">>> [{session_id}] Извлечение данных из ZIP для дашборда...")
            zip_buffer = io.BytesIO(result_data)
            with zipfile.ZipFile(zip_buffer, 'r') as zip_file:
                analysis_data = {}
                
                # Читаем Excel файлы
                for filename_in_zip in zip_file.namelist():
                    if filename_in_zip.endswith('.xlsx'):
                        file_content_inner = zip_file.read(filename_in_zip)
                        df = pd.read_excel(io.BytesIO(file_content_inner), engine='openpyxl')
                        analysis_data[filename_in_zip] = df.to_dict('records')
                    elif filename_in_zip.endswith('.csv'):
                        file_content_inner = zip_file.read(filename_in_zip).decode('utf-8')
                        # Простой парсинг CSV
                        lines = file_content_inner.split('\n')
                        if len(lines) > 1:
                            headers = lines[0].split(';')
                            data = []
                            for line in lines[1:]:
                                if line.strip():
                                    values = line.split(';')
                                    row = {headers[i].strip(): values[i].strip() if i < len(values) else '' 
                                           for i in range(len(headers))}
                                    data.append(row)
                            analysis_data[filename_in_zip] = data
                
                # Сохраняем данные и ZIP
                latest_analysis_results = {
                    'data': analysis_data,
                    'zip': result_data,
                    'filename': output_filename
                }
                
                # Сохраняем результаты также в session_results для доступа через API
                session_results[session_id] = {
                    'success': True,
                    'data': analysis_data,
                    'zip': result_data,
                    'filename': output_filename
                }
                
                print(f">>> [{session_id}] Данные успешно извлечены. Файлов: {len(analysis_data)}")
            
            # Записываем метрики успешной обработки
            duration = time.time() - start_time
            
            # Подсчитываем общее количество записей
            total_records = sum(len(data) for data in analysis_data.values() if isinstance(data, list))
            
            # Записываем основные метрики
            metrics.record_log_analysis(model, 'success', duration, total_records)
            metrics.record_zip_processed(model, 'success', len(file_content))
            
            # Обновляем метрики памяти
            metrics.update_memory_metrics()
            
            # Отправляем сообщение о завершении
            await send_complete(session_id)
            
        except Exception as e:
            print(f">>> [{session_id}] Ошибка при извлечении данных: {e}")
            import traceback
            traceback.print_exc()
            await send_error(session_id, f"Ошибка при обработке результатов: {str(e)}")
            
            # Записываем метрики об ошибке
            duration = time.time() - start_time
            metrics.record_log_analysis(model, 'error', duration, 0)
            
    except Exception as e:
        print(f">>> [{session_id}] Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        await send_error(session_id, f"Критическая ошибка: {str(e)}")
        
        # Записываем метрики об ошибке
        duration = time.time() - start_time
        metrics.record_log_analysis(model, 'error', duration, 0)


# =============================================================================
# ЭНДПОИНТ PROMETHEUS МЕТРИК
# =============================================================================

@app.get("/metrics")
async def metrics_endpoint():
    """
    Эндпоинт для экспорта метрик в формате Prometheus.
    
    Возвращает:
        Response: Метрики в формате Prometheus
    """
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

# =============================================================================
# ТОЧКА ВХОДА ДЛЯ ЗАПУСКА
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    
    # Запускаем сервер для локальной разработки
    # Для продакшена используйте: uvicorn main:app --host 0.0.0.0 --port 8000
    uvicorn.run(
        "main:app",
        host="127.0.0.1",
        port=8001,
        reload=True  # Автоперезагрузка при изменении кода
    )
