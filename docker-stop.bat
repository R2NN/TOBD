@echo off
chcp 65001 >nul
REM ============================================================================
REM Скрипт остановки Docker контейнера для Windows
REM ============================================================================
color 0E
title Docker Stop - FastAPI Log Analyzer

echo.
echo ========================================
echo  FastAPI Log Analyzer - Docker Stop
echo ========================================
echo.

REM Проверка установки Docker
echo [1/2] Проверка Docker...
docker --version >nul 2>&1
if %errorlevel% neq 0 (
    color 0C
    echo [ERROR] Docker не установлен!
    pause
    exit /b 1
)

echo [OK] Docker найден
echo.

REM Остановка контейнеров
echo [2/2] Остановка всех контейнеров...
echo.

docker-compose down

if %errorlevel% equ 0 (
    color 0A
    echo.
    echo ========================================
    echo  [SUCCESS] Все контейнеры остановлены!
    echo ========================================
    echo.
    echo  📊 Остановленные контейнеры:
    echo     • FastAPI приложение
    echo     • Prometheus
    echo     • Grafana
    echo.
    echo  💡 Для запуска снова используйте:
    echo     🚀_ЗАПУСТИТЬ_ВСЁ.bat
    echo     или
    echo     docker-start.bat
    echo.
) else (
    color 0C
    echo.
    echo [ERROR] Ошибка при остановке контейнеров!
    echo.
    echo 💡 Попробуйте:
    echo    docker-compose down --remove-orphans
    echo.
)

echo 📊 Текущий статус контейнеров:
docker-compose ps

echo.
pause

