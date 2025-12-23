"""
=============================================================================
demo_api_curl.py - Генератор cURL команд для демонстрации API
=============================================================================

Этот скрипт генерирует готовые cURL команды для тестирования API.
Идеально подходит для демонстрации на выступлении.

Использование:
    python demo_api_curl.py

Автор: Команда Atomichack 3.0
=============================================================================
"""

import sys
from pathlib import Path

# Устанавливаем UTF-8 для корректного отображения эмодзи в Windows
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass


def print_separator(char="=", length=80):
    """Выводит разделительную линию."""
    print(char * length)


def print_header(text):
    """Выводит красивый заголовок."""
    print_separator()
    print(f"  {text}")
    print_separator()
    print()


def generate_curl_commands(zip_filename="ValidationCases.zip"):
    """
    Генерирует cURL команды для тестирования API.
    
    Параметры:
        zip_filename (str): Имя ZIP-файла
    """
    print_header("🌐 cURL КОМАНДЫ ДЛЯ ТЕСТИРОВАНИЯ API")
    
    print("📝 КОМАНДЫ ДЛЯ КОПИРОВАНИЯ:")
    print()
    
    # Команда для Windows (PowerShell)
    print_separator("-")
    print("💻 WINDOWS (PowerShell):")
    print_separator("-")
    print()
    print("# Быстрая модель (Light)")
    print(f'''curl.exe -X POST "http://127.0.0.1:8001/process/" `
  -F "file=@{zip_filename}" `
  -F "model=light" `
  -o results_light.zip
''')
    print()
    print("# Точная модель (Heavy)")
    print(f'''curl.exe -X POST "http://127.0.0.1:8001/process/" `
  -F "file=@{zip_filename}" `
  -F "model=heavy" `
  -o results_heavy.zip
''')
    print()
    
    # Команда для Linux/macOS
    print_separator("-")
    print("🐧 LINUX / macOS:")
    print_separator("-")
    print()
    print("# Быстрая модель (Light)")
    print(f'''curl -X POST "http://127.0.0.1:8001/process/" \\
  -F "file=@{zip_filename}" \\
  -F "model=light" \\
  -o results_light.zip
''')
    print()
    print("# Точная модель (Heavy)")
    print(f'''curl -X POST "http://127.0.0.1:8001/process/" \\
  -F "file=@{zip_filename}" \\
  -F "model=heavy" \\
  -o results_heavy.zip
''')
    print()
    
    # Команда с подробным выводом
    print_separator("-")
    print("🔍 С ПОДРОБНЫМ ВЫВОДОМ (VERBOSE):")
    print_separator("-")
    print()
    print(f'''curl -X POST "http://127.0.0.1:8001/process/" \\
  -F "file=@{zip_filename}" \\
  -F "model=light" \\
  -o results_light.zip \\
  -v
''')
    print()
    
    # Команда для проверки статуса сервера
    print_separator("-")
    print("✅ ПРОВЕРКА ДОСТУПНОСТИ СЕРВЕРА:")
    print_separator("-")
    print()
    print('curl -X GET "http://127.0.0.1:8001/"')
    print()
    
    print_separator("-")
    print("📊 ПОЛУЧЕНИЕ API ДОКУМЕНТАЦИИ:")
    print_separator("-")
    print()
    print('curl -X GET "http://127.0.0.1:8001/docs"')
    print()
    
    print_separator()


def generate_python_code(zip_filename="ValidationCases.zip"):
    """
    Генерирует Python код для тестирования API.
    
    Параметры:
        zip_filename (str): Имя ZIP-файла
    """
    print_header("🐍 PYTHON КОД ДЛЯ ТЕСТИРОВАНИЯ API")
    
    code = f'''import requests

# Параметры запроса
url = "http://127.0.0.1:8001/process/"
zip_file = "{zip_filename}"
model_choice = "light"  # или "heavy"

# Открываем файл и отправляем
with open(zip_file, 'rb') as f:
    files = {{'file': (zip_file, f, 'application/zip')}}
    data = {{'model': model_choice}}
    
    print(f"📤 Отправка {{zip_file}} на анализ...")
    response = requests.post(url, files=files, data=data, timeout=300)

# Проверяем результат
if response.status_code == 200:
    # Сохраняем результат
    output_file = f"results_{{model_choice}}.zip"
    with open(output_file, 'wb') as f:
        f.write(response.content)
    print(f"✅ Успех! Результаты сохранены в {{output_file}}")
    print(f"📊 Размер: {{len(response.content)}} байт")
else:
    print(f"❌ Ошибка: {{response.status_code}}")
    print(response.text[:500])
'''
    
    print("```python")
    print(code)
    print("```")
    print()
    print_separator()


def generate_postman_collection(zip_filename="ValidationCases.zip"):
    """Генерирует информацию для Postman."""
    print_header("📮 НАСТРОЙКА POSTMAN")
    
    print("1. Создайте новый запрос (Request)")
    print("2. Выберите метод: POST")
    print("3. URL: http://127.0.0.1:8001/process/")
    print()
    print("4. Перейдите на вкладку 'Body'")
    print("5. Выберите 'form-data'")
    print("6. Добавьте поля:")
    print()
    print("   Поле 1:")
    print("   • Key: file")
    print("   • Type: File")
    print(f"   • Value: Выберите файл {zip_filename}")
    print()
    print("   Поле 2:")
    print("   • Key: model")
    print("   • Type: Text")
    print("   • Value: light (или heavy)")
    print()
    print("7. Нажмите Send")
    print("8. Сохраните ответ (ZIP-файл)")
    print()
    print_separator()


def main():
    """Главная функция."""
    # Проверяем наличие ZIP файла
    default_zip = "ValidationCases.zip"
    
    if len(sys.argv) > 1:
        zip_filename = sys.argv[1]
    else:
        zip_filename = default_zip
    
    # Проверяем существование файла
    if not Path(zip_filename).exists():
        print(f"⚠️  ВНИМАНИЕ: Файл '{zip_filename}' не найден!")
        print(f"Команды будут сгенерированы для файла: {zip_filename}")
        print("Замените имя файла на актуальное перед использованием.")
        print()
    
    # Генерируем команды
    generate_curl_commands(zip_filename)
    print()
    generate_python_code(zip_filename)
    print()
    generate_postman_collection(zip_filename)
    
    # Финальные рекомендации
    print_header("💡 РЕКОМЕНДАЦИИ ДЛЯ ДЕМОНСТРАЦИИ")
    print("1. ✅ Убедитесь, что сервер запущен:")
    print("   python main.py")
    print()
    print("2. ✅ Проверьте доступность GPU:")
    print("   python check_gpu.py")
    print()
    print("3. ✅ Подготовьте тестовый ZIP-архив с:")
    print("   • Файлом anomalies_problems.csv")
    print("   • Несколькими .txt файлами с логами")
    print()
    print("4. ✅ Выберите один из способов тестирования:")
    print("   • cURL (быстро, из терминала)")
    print("   • Python скрипт (test_api.py)")
    print("   • Postman (визуально)")
    print("   • Веб-интерфейс (http://127.0.0.1:8001)")
    print()
    print("5. ✅ Для выступления рекомендуем:")
    print("   • Запустить через веб-интерфейс (красиво)")
    print("   • Показать cURL команду (технично)")
    print("   • Открыть дашборд с результатами")
    print()
    print_separator()
    print()
    print("🎤 Готово к презентации!")
    print()


if __name__ == "__main__":
    main()

