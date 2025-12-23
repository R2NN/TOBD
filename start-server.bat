@echo off
chcp 65001 >nul
echo.
echo ╔════════════════════════════════════════════════════════════╗
echo ║                                                            ║
echo ║              🚀 ЗАПУСК FASTAPI СЕРВЕРА                    ║
echo ║                                                            ║
echo ╚════════════════════════════════════════════════════════════╝
echo.

REM Проверка Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python не найден!
    echo.
    echo 📥 Установите Python 3.10+ с https://python.org
    echo.
    pause
    exit /b 1
)

echo ✅ Python найден
python --version
echo.

REM Проверка зависимостей
echo 🔍 Проверка зависимостей...
python -c "import fastapi" >nul 2>&1
if errorlevel 1 (
    echo ⚠️  Зависимости не установлены!
    echo.
    echo 📦 Установка зависимостей...
    pip install -r requirements.txt
    echo.
)

echo ✅ Зависимости установлены
echo.
echo ════════════════════════════════════════════════════════════
echo.
echo 🚀 Запуск сервера...
echo.
echo 📍 Локальный адрес:  http://localhost:8001
echo 📍 Сетевой адрес:   http://127.0.0.1:8001
echo.
echo ⏳ Ожидайте загрузки...
echo.
echo ────────────────────────────────────────────────────────────
echo.
echo 💡 Для публичного доступа (друзья через интернет):
echo    1. Оставьте это окно открытым
echo    2. Откройте НОВОЕ окно PowerShell
echo    3. Запустите: start-ngrok.bat
echo.
echo ════════════════════════════════════════════════════════════
echo.

python main.py

echo.
echo ════════════════════════════════════════════════════════════
echo.
echo ℹ️  Сервер был остановлен
echo.
pause





