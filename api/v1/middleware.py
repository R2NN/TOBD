"""
=============================================================================
api/v1/middleware.py - Middleware для API v1
=============================================================================

Этот модуль содержит middleware для обработки запросов API v1:
- Логирование запросов
- Обработка ошибок
- CORS настройки

Автор: Команда Atomichack 3.0
Дата: 2025
=============================================================================
"""

import time
from typing import Callable
from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware


class APILoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware для логирования всех запросов к API v1.
    Записывает время выполнения и статус ответа.
    """
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        Обрабатывает каждый запрос.
        
        Args:
            request: Входящий запрос
            call_next: Следующий обработчик в цепочке
            
        Returns:
            Response: Ответ сервера
        """
        # Проверяем, относится ли запрос к API v1
        if not request.url.path.startswith("/api/v1"):
            return await call_next(request)
        
        # Засекаем время начала
        start_time = time.time()
        
        # Логируем входящий запрос
        print(f">>> API v1 [{request.method}] {request.url.path}")
        
        # Обрабатываем запрос
        try:
            response = await call_next(request)
            
            # Вычисляем время обработки
            process_time = time.time() - start_time
            
            # Добавляем заголовок с временем обработки
            response.headers["X-Process-Time"] = str(process_time)
            
            # Логируем ответ
            print(f">>> API v1 [{request.method}] {request.url.path} - "
                  f"Status: {response.status_code}, Time: {process_time:.3f}s")
            
            return response
            
        except Exception as e:
            # Логируем ошибку
            process_time = time.time() - start_time
            print(f">>> API v1 ERROR [{request.method}] {request.url.path} - "
                  f"Error: {str(e)}, Time: {process_time:.3f}s")
            
            # Возвращаем JSON ответ с ошибкой
            return JSONResponse(
                status_code=500,
                content={
                    "error": "internal_server_error",
                    "message": "Internal server error occurred",
                    "details": str(e) if __debug__ else None
                }
            )


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Middleware для глобального rate limiting.
    Ограничивает количество запросов от одного IP.
    """
    
    def __init__(self, app, max_requests: int = 100, window_seconds: int = 60):
        """
        Инициализация middleware.
        
        Args:
            app: FastAPI приложение
            max_requests: Максимальное количество запросов
            window_seconds: Временное окно в секундах
        """
        super().__init__(app)
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests = {}  # IP -> [(timestamp, count)]
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        Обрабатывает каждый запрос с проверкой rate limit.
        
        Args:
            request: Входящий запрос
            call_next: Следующий обработчик
            
        Returns:
            Response: Ответ сервера
        """
        # Проверяем только для API v1
        if not request.url.path.startswith("/api/v1"):
            return await call_next(request)
        
        # Получаем IP клиента
        client_ip = request.client.host if request.client else "unknown"
        
        # Текущее время
        current_time = time.time()
        
        # Инициализируем данные для IP если их нет
        if client_ip not in self.requests:
            self.requests[client_ip] = []
        
        # Очищаем старые запросы (за пределами временного окна)
        self.requests[client_ip] = [
            (timestamp, count)
            for timestamp, count in self.requests[client_ip]
            if current_time - timestamp < self.window_seconds
        ]
        
        # Подсчитываем количество запросов в текущем окне
        total_requests = sum(count for _, count in self.requests[client_ip])
        
        # Проверяем лимит
        if total_requests >= self.max_requests:
            return JSONResponse(
                status_code=429,
                content={
                    "error": "rate_limit_exceeded",
                    "message": f"Rate limit exceeded: {self.max_requests} requests per {self.window_seconds} seconds",
                    "details": {
                        "max_requests": self.max_requests,
                        "window_seconds": self.window_seconds,
                        "retry_after": self.window_seconds
                    }
                },
                headers={
                    "Retry-After": str(self.window_seconds),
                    "X-RateLimit-Limit": str(self.max_requests),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(int(current_time + self.window_seconds))
                }
            )
        
        # Добавляем текущий запрос
        self.requests[client_ip].append((current_time, 1))
        
        # Обрабатываем запрос
        response = await call_next(request)
        
        # Добавляем заголовки rate limit
        remaining = self.max_requests - total_requests - 1
        response.headers["X-RateLimit-Limit"] = str(self.max_requests)
        response.headers["X-RateLimit-Remaining"] = str(max(0, remaining))
        response.headers["X-RateLimit-Reset"] = str(int(current_time + self.window_seconds))
        
        return response


# =============================================================================
# ФУНКЦИИ ДЛЯ УСТАНОВКИ MIDDLEWARE
# =============================================================================

def setup_middleware(app):
    """
    Устанавливает все middleware для приложения.
    
    Args:
        app: FastAPI приложение
    """
    # Добавляем middleware для логирования
    app.add_middleware(APILoggingMiddleware)
    print(">>> API v1 Logging Middleware установлен")
    
    # Добавляем middleware для rate limiting
    # 100 запросов в минуту с одного IP
    app.add_middleware(RateLimitMiddleware, max_requests=100, window_seconds=60)
    print(">>> API v1 Rate Limit Middleware установлен (100 req/min)")

