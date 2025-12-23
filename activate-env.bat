@echo off
chcp 65001 >nul
echo.
echo ╔════════════════════════════════════════════════════════════╗
echo ║                                                            ║
echo ║        🐍 АКТИВАЦИЯ ВИРТУАЛЬНОГО ОКРУЖЕНИЯ               ║
echo ║                                                            ║
echo ╚════════════════════════════════════════════════════════════╝
echo.

cd /d D:\Downloads\FASTAPIGITPROJECT
echo 📁 Директория проекта: %CD%
echo.

echo 🔄 Активация окружения 'my_env'...
echo.

call D:\Anaconda3\Scripts\activate.bat my_env

echo.
echo ✅ Окружение активировано!
echo.
echo ════════════════════════════════════════════════════════════
echo.
echo 💡 Доступные команды:
echo    • python main.py          - запустить сервер напрямую
echo    • start-server.bat        - запустить через скрипт
echo    • pip list                - список установленных пакетов
echo    • conda deactivate        - деактивировать окружение
echo.
echo ════════════════════════════════════════════════════════════
echo.
echo 🐍 Версия Python:
python --version
echo.

cmd /k





