@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

echo.
echo ╔════════════════════════════════════════════════════════════╗
echo ║                                                            ║
echo ║           🚀 АВТОМАТИЧЕСКАЯ УСТАНОВКА NGROK               ║
echo ║                                                            ║
echo ╚════════════════════════════════════════════════════════════╝
echo.
echo 📦 Этот скрипт автоматически:
echo    ✅ Скачает ngrok для Windows
echo    ✅ Распакует в нужную папку
echo    ✅ Добавит ваш authtoken
echo    ✅ Проверит работоспособность
echo.
echo ════════════════════════════════════════════════════════════
echo.

REM Проверка прав администратора (желательно, но не обязательно)
net session >nul 2>&1
if %errorlevel% == 0 (
    echo ✅ Запущено с правами администратора
) else (
    echo ⚠️  Запущено без прав администратора (это OK)
)

echo.
echo ════════════════════════════════════════════════════════════
echo 📂 ШАГ 1: Выбор папки установки
echo ════════════════════════════════════════════════════════════
echo.

REM Определяем папку для установки
set "INSTALL_DIR=%USERPROFILE%\ngrok"
echo 💾 ngrok будет установлен в: %INSTALL_DIR%
echo.

set /p CUSTOM_DIR="Использовать эту папку? (Enter=Да, или введите свой путь): "
if not "%CUSTOM_DIR%"=="" (
    set "INSTALL_DIR=%CUSTOM_DIR%"
)

echo.
echo ✅ Папка установки: %INSTALL_DIR%
echo.

REM Создаём папку если её нет
if not exist "%INSTALL_DIR%" (
    echo 📁 Создаю папку %INSTALL_DIR%...
    mkdir "%INSTALL_DIR%"
)

echo.
echo ════════════════════════════════════════════════════════════
echo 📥 ШАГ 2: Скачивание ngrok
echo ════════════════════════════════════════════════════════════
echo.

REM Проверяем, не установлен ли уже ngrok
if exist "%INSTALL_DIR%\ngrok.exe" (
    echo ℹ️  ngrok.exe уже существует в %INSTALL_DIR%
    echo.
    set /p REDOWNLOAD="Скачать заново? (Y/N): "
    if /i not "!REDOWNLOAD!"=="Y" (
        echo.
        echo ⏭️  Пропускаем скачивание, используем существующий файл
        goto :skip_download
    )
)

echo 🌐 Скачиваю ngrok для Windows (64-bit)...
echo 📍 URL: https://bin.equinox.io/c/bNyj1mQVY4c/ngrok-v3-stable-windows-amd64.zip
echo.
echo ⏳ Пожалуйста, подождите (может занять 30-60 секунд)...
echo.

REM Используем PowerShell для скачивания
powershell -Command "& {[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri 'https://bin.equinox.io/c/bNyj1mQVY4c/ngrok-v3-stable-windows-amd64.zip' -OutFile '%INSTALL_DIR%\ngrok.zip' -UseBasicParsing}" 2>nul

if errorlevel 1 (
    echo.
    echo ❌ ОШИБКА: Не удалось скачать ngrok
    echo.
    echo 📋 Возможные причины:
    echo    • Нет интернет-соединения
    echo    • Блокировка антивирусом/файрволом
    echo    • Проблемы с SSL/TLS
    echo.
    echo 💡 Решение: Скачайте вручную:
    echo    1. Откройте: https://ngrok.com/download
    echo    2. Скачайте Windows 64-bit
    echo    3. Распакуйте ngrok.exe в: %INSTALL_DIR%
    echo    4. Запустите этот скрипт снова
    echo.
    pause
    exit /b 1
)

echo ✅ Скачивание завершено!
echo.

echo ════════════════════════════════════════════════════════════
echo 📦 ШАГ 3: Распаковка архива
echo ════════════════════════════════════════════════════════════
echo.

REM Распаковываем ZIP
echo 📂 Распаковываю ngrok.zip...
powershell -Command "Expand-Archive -Path '%INSTALL_DIR%\ngrok.zip' -DestinationPath '%INSTALL_DIR%' -Force" 2>nul

if errorlevel 1 (
    echo ❌ ОШИБКА: Не удалось распаковать архив
    pause
    exit /b 1
)

echo ✅ Распаковка завершена!
echo.

REM Удаляем ZIP файл
echo 🗑️  Удаляю временный файл ngrok.zip...
del "%INSTALL_DIR%\ngrok.zip" 2>nul
echo.

:skip_download

REM Проверяем наличие ngrok.exe
if not exist "%INSTALL_DIR%\ngrok.exe" (
    echo ❌ ОШИБКА: ngrok.exe не найден в %INSTALL_DIR%
    echo.
    echo 💡 Скачайте ngrok вручную с https://ngrok.com/download
    pause
    exit /b 1
)

echo.
echo ════════════════════════════════════════════════════════════
echo 🔑 ШАГ 4: Добавление Authtoken
echo ════════════════════════════════════════════════════════════
echo.

set "AUTHTOKEN=33xJiXbggbf0VkJQYYS0Q1Rt7IF_4N9bEXBDXbBDvzMjvEPLy"

echo 🔐 Добавляю authtoken в конфигурацию ngrok...
echo.

"%INSTALL_DIR%\ngrok.exe" config add-authtoken %AUTHTOKEN%

if errorlevel 1 (
    echo ❌ ОШИБКА: Не удалось добавить authtoken
    echo.
    echo 💡 Попробуйте вручную:
    echo    "%INSTALL_DIR%\ngrok.exe" config add-authtoken %AUTHTOKEN%
    echo.
    pause
    exit /b 1
)

echo.
echo ✅ Authtoken успешно добавлен!
echo.

echo.
echo ════════════════════════════════════════════════════════════
echo 🔍 ШАГ 5: Проверка установки
echo ════════════════════════════════════════════════════════════
echo.

echo 📋 Проверяю версию ngrok...
"%INSTALL_DIR%\ngrok.exe" version
echo.

if errorlevel 1 (
    echo ⚠️  Предупреждение: ngrok может быть не полностью установлен
) else (
    echo ✅ ngrok работает корректно!
)

echo.
echo ════════════════════════════════════════════════════════════
echo 🎯 ШАГ 6: Добавление в PATH (опционально)
echo ════════════════════════════════════════════════════════════
echo.

echo 💡 Хотите добавить ngrok в PATH для быстрого доступа?
echo    (Тогда можно будет писать просто: ngrok http 8001)
echo.

set /p ADD_PATH="Добавить в PATH? (Y/N): "

if /i "%ADD_PATH%"=="Y" (
    echo.
    echo 📝 Добавляю %INSTALL_DIR% в PATH...
    
    REM Добавляем в PATH текущего пользователя
    powershell -Command "[Environment]::SetEnvironmentVariable('Path', [Environment]::GetEnvironmentVariable('Path', 'User') + ';%INSTALL_DIR%', 'User')" 2>nul
    
    if errorlevel 1 (
        echo ⚠️  Не удалось добавить в PATH автоматически
        echo.
        echo 📋 Добавьте вручную:
        echo    1. Правой кнопкой на "Этот компьютер" → Свойства
        echo    2. Дополнительные параметры системы
        echo    3. Переменные среды
        echo    4. В Path пользователя добавьте: %INSTALL_DIR%
        echo.
    ) else (
        echo ✅ Добавлено в PATH!
        echo.
        echo ⚠️  ВАЖНО: Перезапустите терминал для применения изменений!
    )
) else (
    echo.
    echo ℹ️  Пропускаем добавление в PATH
    echo    Используйте полный путь: "%INSTALL_DIR%\ngrok.exe"
)

echo.
echo.
echo ╔════════════════════════════════════════════════════════════╗
echo ║                                                            ║
echo ║              ✅ УСТАНОВКА ЗАВЕРШЕНА!                      ║
echo ║                                                            ║
echo ╚════════════════════════════════════════════════════════════╝
echo.
echo 📍 ngrok установлен в: %INSTALL_DIR%
echo 🔑 Authtoken добавлен
echo ✅ Готов к использованию!
echo.
echo ════════════════════════════════════════════════════════════
echo 🚀 КАК ИСПОЛЬЗОВАТЬ:
echo ════════════════════════════════════════════════════════════
echo.
echo 1️⃣  Запустите сервер (в этом окне):
echo    python main.py
echo.
echo 2️⃣  Запустите ngrok (в НОВОМ окне):

if /i "%ADD_PATH%"=="Y" (
    echo    ngrok http 8001
    echo    (после перезапуска терминала^)
) else (
    echo    "%INSTALL_DIR%\ngrok.exe" http 8001
)

echo.
echo 3️⃣  Скопируйте публичный URL и отправьте друзьям!
echo.
echo ════════════════════════════════════════════════════════════
echo.
echo 💡 Удобные скрипты для запуска:
echo    • start-server.bat  (запуск сервера)
echo    • start-ngrok.bat   (запуск ngrok)
echo.
echo 📚 Документация:
echo    • 🌐_ПУБЛИЧНЫЙ_ДОСТУП.md  (главная инструкция)
echo    • NGROK_QUICK_START.md    (быстрый старт)
echo.
echo ════════════════════════════════════════════════════════════
echo.

REM Создаём ярлык для удобства (опционально)
set /p CREATE_SHORTCUT="Создать ярлык ngrok на рабочем столе? (Y/N): "

if /i "%CREATE_SHORTCUT%"=="Y" (
    echo.
    echo 🔗 Создаю ярлык...
    
    set "DESKTOP=%USERPROFILE%\Desktop"
    
    powershell -Command "$WshShell = New-Object -ComObject WScript.Shell; $Shortcut = $WshShell.CreateShortcut('%DESKTOP%\ngrok.lnk'); $Shortcut.TargetPath = '%INSTALL_DIR%\ngrok.exe'; $Shortcut.WorkingDirectory = '%CD%'; $Shortcut.Save()"
    
    if errorlevel 1 (
        echo ⚠️  Не удалось создать ярлык
    ) else (
        echo ✅ Ярлык создан на рабочем столе!
    )
)

echo.
echo ════════════════════════════════════════════════════════════
echo.
echo 🎉 Готово! Можете начинать использовать ngrok!
echo.
echo 📝 Сохраните этот путь: %INSTALL_DIR%\ngrok.exe
echo.
pause





