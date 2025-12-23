import zipfile
import os

# Файлы для архива
files = [
    "🚀_УСТАНОВИТЬ_NGROK.bat",
    "start-server.bat",
    "start-ngrok.bat",
    "start-with-ngrok.bat",
    "install-ngrok.bat",
    "⚡_НАЧНИТЕ_ЗДЕСЬ.md",
    "✅_АВТОУСТАНОВКА_ГОТОВА.md",
    "🌐_ПУБЛИЧНЫЙ_ДОСТУП.md",
    "NGROK_QUICK_START.md",
    "NGROK_SETUP_GUIDE.md",
    "NGROK_FINAL_INSTRUCTIONS.md",
    "📦_КАК_СОЗДАТЬ_АРХИВ.md"
]

zip_name = "NGROK_COMPLETE_PACKAGE.zip"

print("Создаю архив...")
with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
    for file in files:
        if os.path.exists(file):
            zipf.write(file)
            print(f"✓ {file}")

print(f"\n✅ Готово! Создан: {zip_name}")
print(f"Размер: {os.path.getsize(zip_name) / 1024:.1f} KB")





