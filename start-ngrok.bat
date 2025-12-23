@echo off
chcp 65001 >nul
echo.
echo ╔════════════════════════════════════════════════════════════╗
echo ║                                                            ║
echo ║              🌐 ЗАПУСК NGROK ТУННЕЛЯ                      ║
echo ║                                                            ║
echo ╚════════════════════════════════════════════════════════════╝
echo.

REM Попытка найти ngrok
where ngrok >nul 2>&1
if errorlevel 1 (
    echo ⚠️  ngrok не найден в PATH
    echo.
    echo 📍 Попытка найти в стандартных местах...
    
    if exist "C:\ngrok\ngrok.exe" (
        set NGROK_PATH=C:\ngrok\ngrok.exe
        echo ✅ Найден: C:\ngrok\ngrok.exe
    ) else if exist "D:\ngrok\ngrok.exe" (
        set NGROK_PATH=D:\ngrok\ngrok.exe
        echo ✅ Найден: D:\ngrok\ngrok.exe
    ) else if exist "%USERPROFILE%\ngrok\ngrok.exe" (
        set NGROK_PATH=%USERPROFILE%\ngrok\ngrok.exe
        echo ✅ Найден: %USERPROFILE%\ngrok\ngrok.exe
    ) else if exist "ngrok.exe" (
        set NGROK_PATH=ngrok.exe
        echo ✅ Найден в текущей папке
    ) else (
        echo.
        echo ❌ ngrok НЕ НАЙДЕН!
        echo.
        echo ════════════════════════════════════════════════════════════
        echo 🚀 АВТОМАТИЧЕСКАЯ УСТАНОВКА ДОСТУПНА!
        echo ════════════════════════════════════════════════════════════
        echo.
        echo 💡 Я могу автоматически установить ngrok за вас!
        echo.
        echo    Просто запустите:
        echo    👉 🚀_УСТАНОВИТЬ_NGROK.bat
        echo.
        echo    Это займёт 1-2 минуты и всё настроит автоматически!
        echo.
        echo ════════════════════════════════════════════════════════════
        echo.
        
        if exist "🚀_УСТАНОВИТЬ_NGROK.bat" (
            choice /C YN /M "Запустить автоматическую установку сейчас"
            if not errorlevel 2 (
                echo.
                echo 🚀 Запускаю установщик...
                echo.
                call "🚀_УСТАНОВИТЬ_NGROK.bat"
                echo.
                echo ════════════════════════════════════════════════════════════
                echo.
                echo ✅ Установка завершена! Перезапустите этот скрипт.
                echo.
                pause
                exit /b 0
            )
        )
        
        echo.
        echo 📋 Или установите вручную:
        echo    1. Откройте: https://ngrok.com/download
        echo    2. Скачайте Windows 64-bit
        echo    3. Распакуйте ngrok.exe в папку
        echo    4. Запустите этот скрипт снова
        echo.
        set /p CUSTOM_PATH="Или введите полный путь к ngrok.exe: "
        if "%CUSTOM_PATH%"=="" (
            pause
            exit /b 1
        )
        set NGROK_PATH=%CUSTOM_PATH%
    )
) else (
    set NGROK_PATH=ngrok
    echo ✅ ngrok найден в PATH
)

echo.
echo ────────────────────────────────────────────────────────────
echo.

REM Проверка, что сервер запущен
echo 🔍 Проверка, запущен ли локальный сервер...
powershell -Command "(Invoke-WebRequest -Uri 'http://localhost:8001' -UseBasicParsing -TimeoutSec 2).StatusCode" >nul 2>&1
if errorlevel 1 (
    echo.
    echo ⚠️  ВНИМАНИЕ! Локальный сервер НЕ ОТВЕЧАЕТ на порту 8001
    echo.
    echo 🚀 Перед запуском ngrok нужно запустить сервер:
    echo.
    echo    В ДРУГОМ окне PowerShell выполните:
    echo    cd D:\Downloads\FASTAPIGITPROJECT
    echo    python main.py
    echo.
    set /p CONTINUE="Продолжить запуск ngrok? (Y/N): "
    if /i not "%CONTINUE%"=="Y" (
        echo.
        echo 📋 Шаги для запуска:
        echo    1. Откройте НОВОЕ окно PowerShell
        echo    2. Выполните: cd D:\Downloads\FASTAPIGITPROJECT
        echo    3. Выполните: python main.py
        echo    4. Запустите этот скрипт снова
        echo.
        pause
        exit /b 1
    )
) else (
    echo ✅ Сервер работает на http://localhost:8001
)

echo.
echo ════════════════════════════════════════════════════════════
echo.
echo 🚀 Запуск ngrok туннеля для порта 8001...
echo.
echo ⏳ Пожалуйста, подождите...
echo.

REM Запуск ngrok
"%NGROK_PATH%" http 8001

echo.
echo ════════════════════════════════════════════════════════════
echo.
echo ℹ️  ngrok был остановлен
echo.
echo 💡 Для повторного запуска:
echo    • Запустите этот скрипт снова
echo    • Или выполните: %NGROK_PATH% http 8001
echo.
pause

