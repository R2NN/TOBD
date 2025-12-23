@echo off
chcp 65001 >nul
REM ============================================================================
REM Скрипт быстрого запуска Docker контейнера для Windows
REM Запускается автоматически в фоновом режиме
REM ============================================================================
color 0A
title Docker Start - FastAPI Log Analyzer

echo.
echo ========================================
echo  FastAPI Log Analyzer - Docker Start
echo ========================================
echo.

REM Проверка установки Docker
echo [1/3] Проверка Docker...
docker --version >nul 2>&1
if %errorlevel% neq 0 (
    color 0C
    echo [ERROR] Docker не установлен!
    echo.
    echo Пожалуйста, установите Docker Desktop:
    echo https://www.docker.com/products/docker-desktop
    echo.
    pause
    exit /b 1
)

echo [OK] Docker установлен
echo.

REM Проверка запущен ли Docker
docker ps >nul 2>&1
if %errorlevel% neq 0 (
    color 0C
    echo [ERROR] Docker не запущен!
    echo.
    echo Пожалуйста, запустите Docker Desktop и повторите попытку.
    echo.
    pause
    exit /b 1
)

echo [OK] Docker запущен
echo.

REM Остановка старых контейнеров
echo [2/3] Остановка старых контейнеров...
docker-compose down >nul 2>&1
echo [OK] Готово
echo.

REM Запуск в фоновом режиме (по умолчанию для двойного клика)
echo [3/3] Запуск контейнеров в фоновом режиме...
echo.
docker-compose up -d --build

if %errorlevel% equ 0 (
    color 0A
    echo.
    echo ========================================
    echo  [SUCCESS] Контейнеры запущены!
    echo ========================================
    echo.
    echo  🌐 Приложение доступно на:
    echo     http://localhost:8001
    echo     http://localhost:8001/docs
    echo.
    echo  📊 Prometheus:
    echo     http://localhost:9090
    echo.
    echo  📈 Grafana:
    echo     http://localhost:3000 (admin/admin)
    echo.
    echo  💡 Полезные команды:
    echo     docker-compose logs -f       - Просмотр логов
    echo     docker-compose ps            - Статус контейнеров
    echo     docker-compose down          - Остановка
    echo.
    echo  🛑 Для остановки запустите: docker-stop.bat
    echo.
    
    REM Открываем браузер
    timeout /t 2 /nobreak >nul
    start http://localhost:8001
    
) else (
    color 0C
    echo.
    echo [ERROR] Ошибка при запуске контейнеров!
    echo.
    echo Попробуйте:
    echo   1. docker-compose down
    echo   2. docker-compose up -d
    echo.
)

pause

