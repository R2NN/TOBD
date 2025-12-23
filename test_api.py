"""
=============================================================================
test_api.py - Скрипт для тестирования API анализатора логов
=============================================================================

Этот скрипт демонстрирует работу с API для выступления на хакатоне.
Показывает отправку ZIP-архива через POST-запрос и получение результатов.

Использование:
    python test_api.py [путь_к_архиву.zip] [light|heavy]

Примеры:
    python test_api.py ValidationCases.zip light
    python test_api.py logs.zip heavy

Автор: Команда Atomichack 3.0
=============================================================================
"""

import sys
import requests
import time
from pathlib import Path

# Устанавливаем UTF-8 для корректного отображения эмодзи в Windows
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass


# Настройки API
API_URL = "http://127.0.0.1:8001/process/"
DEFAULT_ZIP = "ValidationCases.zip"  # Замените на ваш тестовый архив


def print_separator(char="=", length=80):
    """Выводит разделительную линию."""
    print(char * length)


def print_header(text):
    """Выводит красивый заголовок."""
    print_separator()
    print(f"  {text}")
    print_separator()
    print()


def format_size(bytes_size):
    """Форматирует размер в байтах в читаемый вид."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} TB"


def test_api_upload(zip_path: str, model: str = 'light'):
    """
    Тестирует загрузку ZIP-архива через API.
    
    Параметры:
        zip_path (str): Путь к ZIP-архиву
        model (str): Модель для использования ('light' или 'heavy')
    """
    print_header("🚀 ТЕСТИРОВАНИЕ API АНАЛИЗАТОРА ЛОГОВ")
    
    # Проверяем существование файла
    file_path = Path(zip_path)
    if not file_path.exists():
        print(f"❌ ОШИБКА: Файл '{zip_path}' не найден!")
        print(f"   Текущая директория: {Path.cwd()}")
        return False
    
    # Информация о файле
    file_size = file_path.stat().st_size
    print(f"📦 Файл для загрузки:")
    print(f"   • Путь: {file_path.absolute()}")
    print(f"   • Размер: {format_size(file_size)}")
    print(f"   • Модель: {'⚡ Быстрая (Light)' if model == 'light' else '🎯 Точная (Heavy)'}")
    print()
    
    # Подготовка данных для отправки
    print("📤 Отправка запроса на API...")
    print(f"   URL: {API_URL}")
    print()
    
    try:
        # Открываем файл и готовим запрос
        with open(file_path, 'rb') as f:
            files = {
                'file': (file_path.name, f, 'application/zip')
            }
            data = {
                'model': model
            }
            
            # Засекаем время
            start_time = time.time()
            
            # Отправляем POST-запрос
            print("⏳ Обработка началась... (это может занять 30-120 секунд)")
            print()
            
            response = requests.post(
                API_URL,
                files=files,
                data=data,
                timeout=300  # Таймаут 5 минут
            )
            
            # Вычисляем время обработки
            elapsed_time = time.time() - start_time
            
            print_separator("-")
            print(f"⏱️  Время обработки: {elapsed_time:.2f} секунд")
            print_separator("-")
            print()
            
            # Проверяем статус ответа
            if response.status_code == 200:
                print_header("✅ УСПЕХ! Анализ завершен")
                
                # Информация о результатах
                result_size = len(response.content)
                print(f"📊 Результаты:")
                print(f"   • Размер архива с результатами: {format_size(result_size)}")
                print(f"   • Content-Type: {response.headers.get('content-type', 'N/A')}")
                print()
                
                # Сохраняем результаты
                output_filename = f"results_{model}_{int(time.time())}.zip"
                with open(output_filename, 'wb') as output_file:
                    output_file.write(response.content)
                
                print(f"💾 Результаты сохранены в: {output_filename}")
                print()
                print_separator("-")
                print("📈 Статистика:")
                print(f"   • Загружено: {format_size(file_size)}")
                print(f"   • Получено: {format_size(result_size)}")
                print(f"   • Скорость: {format_size(file_size / elapsed_time)}/с")
                print(f"   • Модель: {model.upper()}")
                print_separator("-")
                print()
                
                return True
            
            elif response.status_code == 303:
                print_header("⚠️  ПЕРЕНАПРАВЛЕНИЕ")
                print(f"Статус: {response.status_code}")
                print(f"Location: {response.headers.get('location', 'N/A')}")
                print()
                return False
            
            else:
                print_header("❌ ОШИБКА")
                print(f"Статус код: {response.status_code}")
                print(f"Ответ сервера:")
                print(response.text[:500])  # Первые 500 символов
                print()
                return False
    
    except requests.exceptions.Timeout:
        print_header("❌ ОШИБКА: ТАЙМАУТ")
        print("Сервер не ответил в течение 5 минут.")
        print("Возможно, архив слишком большой или сервер перегружен.")
        print()
        return False
    
    except requests.exceptions.ConnectionError:
        print_header("❌ ОШИБКА: НЕТ ПОДКЛЮЧЕНИЯ")
        print(f"Не удалось подключиться к серверу: {API_URL}")
        print()
        print("💡 Убедитесь, что сервер запущен:")
        print("   python main.py")
        print()
        return False
    
    except Exception as e:
        print_header("❌ НЕПРЕДВИДЕННАЯ ОШИБКА")
        print(f"Тип ошибки: {type(e).__name__}")
        print(f"Сообщение: {str(e)}")
        print()
        return False


def main():
    """Главная функция."""
    # Парсим аргументы командной строки
    if len(sys.argv) > 1:
        zip_path = sys.argv[1]
    else:
        zip_path = DEFAULT_ZIP
    
    if len(sys.argv) > 2:
        model = sys.argv[2].lower()
        if model not in ['light', 'heavy']:
            print("⚠️  Неверная модель. Используйте 'light' или 'heavy'. Используется 'light'.")
            model = 'light'
    else:
        model = 'light'
    
    # Запускаем тест
    success = test_api_upload(zip_path, model)
    
    # Финальное сообщение
    print()
    if success:
        print_header("🎉 ТЕСТ УСПЕШНО ЗАВЕРШЕН!")
        print("Вы можете загрузить результаты на дашборд:")
        print("http://127.0.0.1:8001/dashboard")
        print()
    else:
        print_header("❌ ТЕСТ ЗАВЕРШИЛСЯ С ОШИБКОЙ")
        print("Проверьте логи выше для деталей.")
        print()
    
    print_separator()


if __name__ == "__main__":
    main()

