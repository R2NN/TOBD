@echo off
chcp 65001 >nul
echo.
echo ╔════════════════════════════════════════════════════════════╗
echo ║                                                            ║
echo ║              🌐 ЗАПУСК NGROK ТУННЕЛЯ                      ║
echo ║                                                            ║
echo ╚════════════════════════════════════════════════════════════╝
echo.
echo 📍 Путь к ngrok: D:\Downloads\ngrok-v3-stable-windows-amd64
echo 🎯 Порт сервера: 8001
echo.
echo ════════════════════════════════════════════════════════════
echo.

REM Проверка наличия ngrok
if not exist "D:\Downloads\ngrok-v3-stable-windows-amd64\ngrok.exe" (
    echo ❌ Ngrok не найден!
    echo.
    echo 📥 Скачайте ngrok с https://ngrok.com/download
    echo 📁 И поместите в: D:\Downloads\ngrok-v3-stable-windows-amd64
    echo.
    pause
    exit /b 1
)

echo ✅ Ngrok найден
echo.

REM Проверка токена
D:\Downloads\ngrok-v3-stable-windows-amd64\ngrok.exe config check >nul 2>&1
if errorlevel 1 (
    echo ⚠️  ВНИМАНИЕ: Токен не настроен!
    echo.
    echo 🔑 Для работы ngrok нужен токен авторизации
    echo.
    echo 📋 КАК ПОЛУЧИТЬ ТОКЕН:
    echo    1. Откройте: https://dashboard.ngrok.com/
    echo    2. Войдите или зарегистрируйтесь
    echo    3. Скопируйте "Your Authtoken"
    echo.
    echo ════════════════════════════════════════════════════════════
    echo.
    set /p NGROK_TOKEN="Вставьте ваш токен и нажмите Enter: "
    echo.
    echo 🔧 Добавляю токен...
    D:\Downloads\ngrok-v3-stable-windows-amd64\ngrok.exe config add-authtoken %NGROK_TOKEN%
    echo.
)

echo ════════════════════════════════════════════════════════════
echo.
echo 🚀 ЗАПУСК NGROK...
echo.
echo 📍 Вы получите публичный URL вида:
echo    https://abc123xyz.ngrok.app
echo.
echo 💡 Эту ссылку можно отправить кому угодно!
echo.
echo ⚠️  НЕ ЗАКРЫВАЙТЕ ЭТО ОКНО!
echo    (иначе публичная ссылка перестанет работать)
echo.
echo ════════════════════════════════════════════════════════════
echo.

REM Запуск ngrok
D:\Downloads\ngrok-v3-stable-windows-amd64\ngrok.exe http 8001

echo.
echo ════════════════════════════════════════════════════════════
echo.
echo ℹ️  Ngrok был остановлен
echo.
pause





