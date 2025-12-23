# Создание ZIP-архива с файлами ngrok
# Поддержка Unicode имён файлов

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "================================================================"
Write-Host "  Создание ZIP-архива с файлами ngrok"
Write-Host "================================================================"
Write-Host ""

# Список файлов для архивации
$files = @(
    "🚀_УСТАНОВИТЬ_NGROK.bat",
    "start-server.bat",
    "start-ngrok.bat",
    "start-with-ngrok.bat",
    "install-ngrok.bat",
    "⚡_НАЧНИТЕ_ЗДЕСЬ.md",
    "✅_АВТОУСТАНОВКА_ГОТОВА.md",
    "✅_ГОТОВО_К_ЗАПУСКУ.md",
    "🌐_ПУБЛИЧНЫЙ_ДОСТУП.md",
    "NGROK_QUICK_START.md",
    "NGROK_SETUP_GUIDE.md",
    "NGROK_FINAL_INSTRUCTIONS.md"
)

# Имя архива
$zipName = "NGROK_COMPLETE_PACKAGE.zip"

# Удаляем старый архив если есть
if (Test-Path $zipName) {
    Write-Host "🗑️  Удаляю старый архив..."
    Remove-Item $zipName -Force
}

Write-Host "📦 Создаю архив: $zipName"
Write-Host ""

# Проверяем существование файлов и добавляем в архив
$existingFiles = @()
$missingFiles = @()

foreach ($file in $files) {
    if (Test-Path $file) {
        $existingFiles += $file
        Write-Host "  ✅ $file"
    } else {
        $missingFiles += $file
        Write-Host "  ⚠️  Не найден: $file"
    }
}

Write-Host ""

if ($existingFiles.Count -eq 0) {
    Write-Host "❌ ОШИБКА: Нет файлов для архивации!"
    pause
    exit 1
}

Write-Host "📋 Добавляю в архив $($existingFiles.Count) файлов..."
Write-Host ""

try {
    # Создаём архив
    Compress-Archive -Path $existingFiles -DestinationPath $zipName -CompressionLevel Optimal -Force
    
    Write-Host "================================================================"
    Write-Host "  ✅ АРХИВ СОЗДАН УСПЕШНО!"
    Write-Host "================================================================"
    Write-Host ""
    Write-Host "📦 Файл: $zipName"
    
    # Получаем размер архива
    $zipSize = (Get-Item $zipName).Length
    $zipSizeKB = [math]::Round($zipSize / 1KB, 2)
    Write-Host "📊 Размер: $zipSizeKB KB"
    Write-Host "📁 Файлов в архиве: $($existingFiles.Count)"
    
    if ($missingFiles.Count -gt 0) {
        Write-Host ""
        Write-Host "⚠️  Пропущено файлов: $($missingFiles.Count)"
    }
    
    Write-Host ""
    Write-Host "================================================================"
    Write-Host ""
    Write-Host "✅ Готово! Можете отправить архив другим пользователям."
    Write-Host ""
    
} catch {
    Write-Host ""
    Write-Host "❌ ОШИБКА при создании архива:"
    Write-Host $_.Exception.Message
    Write-Host ""
    pause
    exit 1
}

pause





