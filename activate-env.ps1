# ============================================================================
# Скрипт активации виртуального окружения Anaconda
# ============================================================================

Write-Host ""
Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║                                                            ║" -ForegroundColor Cyan
Write-Host "║        🐍 АКТИВАЦИЯ ВИРТУАЛЬНОГО ОКРУЖЕНИЯ               ║" -ForegroundColor Cyan
Write-Host "║                                                            ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Переход в директорию проекта
$projectPath = "D:\Downloads\FASTAPIGITPROJECT"
Set-Location $projectPath
Write-Host "📁 Директория проекта: $projectPath" -ForegroundColor Green
Write-Host ""

# Активация conda окружения
Write-Host "🔄 Активация окружения 'my_env'..." -ForegroundColor Yellow
Write-Host ""

# Инициализация conda для PowerShell (если еще не инициализирована)
& 'D:\Anaconda3\Scripts\conda.exe' init powershell 2>$null

# Активация окружения
& 'D:\Anaconda3\Scripts\conda.exe' activate my_env

Write-Host ""
Write-Host "✅ Окружение активировано!" -ForegroundColor Green
Write-Host ""
Write-Host "════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""
Write-Host "💡 Доступные команды:" -ForegroundColor Yellow
Write-Host "   • python main.py          - запустить сервер напрямую" -ForegroundColor White
Write-Host "   • .\start-server.bat      - запустить через скрипт" -ForegroundColor White
Write-Host "   • pip list                - список установленных пакетов" -ForegroundColor White
Write-Host "   • conda deactivate        - деактивировать окружение" -ForegroundColor White
Write-Host ""
Write-Host "════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""

# Проверка версии Python
Write-Host "🐍 Версия Python:" -ForegroundColor Cyan
python --version
Write-Host ""





