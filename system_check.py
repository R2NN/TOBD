"""
=============================================================================
system_check.py - Проверка готовности системы к демонстрации
=============================================================================

Этот скрипт проверяет все компоненты системы перед демонстрацией.

Использование:
    python system_check.py

Автор: Команда Atomichack 3.0
=============================================================================
"""

import sys
import os
from pathlib import Path
import importlib.util

# Устанавливаем UTF-8 для корректного отображения эмодзи в Windows
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass


def check_mark(condition):
    """Возвращает галочку или крестик."""
    return "✅" if condition else "❌"


def print_separator(char="=", length=80):
    """Выводит разделительную линию."""
    print(char * length)


def print_header(text):
    """Выводит красивый заголовок."""
    print_separator()
    print(f"  {text}")
    print_separator()
    print()


def check_python_version():
    """Проверяет версию Python."""
    version = sys.version_info
    required = (3, 10)
    is_ok = version >= required
    
    print(f"{check_mark(is_ok)} Python версия: {version.major}.{version.minor}.{version.micro}")
    if not is_ok:
        print(f"   ⚠️  Требуется Python {required[0]}.{required[1]} или выше")
    return is_ok


def check_module(module_name, display_name=None):
    """Проверяет наличие модуля."""
    if display_name is None:
        display_name = module_name
    
    spec = importlib.util.find_spec(module_name)
    is_installed = spec is not None
    
    print(f"{check_mark(is_installed)} {display_name}")
    if not is_installed:
        print(f"   ⚠️  Установите: pip install {module_name}")
    
    return is_installed


def check_dependencies():
    """Проверяет все зависимости."""
    print_header("📦 ПРОВЕРКА ЗАВИСИМОСТЕЙ")
    
    modules = [
        ("fastapi", "FastAPI"),
        ("uvicorn", "Uvicorn"),
        ("pandas", "Pandas"),
        ("sentence_transformers", "Sentence Transformers"),
        ("sklearn", "Scikit-learn"),
        ("openpyxl", "OpenPyXL"),
        ("torch", "PyTorch"),
    ]
    
    results = []
    for module_name, display_name in modules:
        results.append(check_module(module_name, display_name))
    
    print()
    return all(results)


def check_gpu():
    """Проверяет доступность GPU."""
    print_header("🎮 ПРОВЕРКА GPU")
    
    try:
        import torch
        
        if torch.cuda.is_available():
            gpu_name = torch.cuda.get_device_name(0)
            gpu_memory = torch.cuda.get_device_properties(0).total_memory / 1024**3
            print(f"✅ GPU доступен: {gpu_name}")
            print(f"   Память: {gpu_memory:.2f} GB")
            print(f"   CUDA версия: {torch.version.cuda}")
            print()
            return True, "gpu"
        else:
            print("⚠️  GPU не обнаружен")
            print("   Система будет использовать CPU")
            print("   Для ускорения установите PyTorch с CUDA:")
            print("   pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118")
            print()
            return True, "cpu"
    except ImportError:
        print("❌ PyTorch не установлен")
        print("   Установите: pip install torch")
        print()
        return False, None


def check_project_structure():
    """Проверяет структуру проекта."""
    print_header("📁 ПРОВЕРКА СТРУКТУРЫ ПРОЕКТА")
    
    required_files = [
        "main.py",
        "config.py",
        "requirements.txt",
        "processing/__init__.py",
        "processing/orchestrator.py",
        "processing/ml_analysis.py",
        "processing/log_parser.py",
        "processing/knowledge_base.py",
        "processing/report_generator.py",
        "processing/playbooks.py",
        "templates/index.html",
        "templates/dashboard.html",
    ]
    
    all_present = True
    for file_path in required_files:
        exists = Path(file_path).exists()
        print(f"{check_mark(exists)} {file_path}")
        if not exists:
            all_present = False
    
    print()
    return all_present


def check_test_files():
    """Проверяет наличие тестовых файлов."""
    print_header("🧪 ПРОВЕРКА ТЕСТОВЫХ ФАЙЛОВ")
    
    test_files = [
        "check_gpu.py",
        "test_api.py",
        "demo_api_curl.py",
        "system_check.py",
        "DEMO_API_GUIDE.md",
    ]
    
    for file_path in test_files:
        exists = Path(file_path).exists()
        print(f"{check_mark(exists)} {file_path}")
    
    print()


def check_server_running():
    """Проверяет, запущен ли сервер."""
    print_header("🌐 ПРОВЕРКА СЕРВЕРА")
    
    try:
        import requests
        response = requests.get("http://127.0.0.1:8001/", timeout=2)
        print("✅ Сервер запущен и отвечает")
        print(f"   Статус код: {response.status_code}")
        print()
        return True
    except ImportError:
        print("⚠️  Модуль requests не установлен")
        print("   Установите: pip install requests")
        print()
        return False
    except requests.exceptions.ConnectionError:
        print("❌ Сервер не запущен")
        print("   Запустите: python main.py")
        print()
        return False
    except Exception as e:
        print(f"⚠️  Ошибка при проверке сервера: {e}")
        print()
        return False


def generate_summary(checks):
    """Генерирует итоговую сводку."""
    print_header("📊 ИТОГОВАЯ СВОДКА")
    
    total = len(checks)
    passed = sum(1 for c in checks.values() if c)
    
    print(f"Всего проверок: {total}")
    print(f"Успешно: {passed}")
    print(f"Не пройдено: {total - passed}")
    print()
    
    if passed == total:
        print_separator("=")
        print("🎉 ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ!")
        print("   Система готова к демонстрации!")
        print_separator("=")
        return True
    else:
        print_separator("=")
        print("⚠️  ЕСТЬ ПРОБЛЕМЫ")
        print("   Исправьте ошибки перед демонстрацией")
        print_separator("=")
        return False


def generate_recommendations(checks, device_type):
    """Генерирует рекомендации."""
    print()
    print_header("💡 РЕКОМЕНДАЦИИ")
    
    if not checks['dependencies']:
        print("1. Установите зависимости:")
        print("   pip install -r requirements.txt")
        print()
    
    if device_type == "cpu":
        print("2. Для ускорения установите PyTorch с CUDA:")
        print("   pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118")
        print()
    
    if not checks['server']:
        print("3. Запустите сервер перед демонстрацией:")
        print("   python main.py")
        print()
    
    print("4. Для проверки GPU запустите:")
    print("   python check_gpu.py")
    print()
    
    print("5. Для тестирования API:")
    print("   python test_api.py [путь_к_архиву.zip] [light|heavy]")
    print()
    
    print("6. Для получения cURL команд:")
    print("   python demo_api_curl.py")
    print()
    
    print("7. Прочитайте руководство по демонстрации:")
    print("   DEMO_API_GUIDE.md")
    print()


def main():
    """Главная функция."""
    print()
    print_header("🔍 ПРОВЕРКА СИСТЕМЫ ПЕРЕД ДЕМОНСТРАЦИЕЙ")
    
    # Словарь результатов проверок
    checks = {}
    
    # Проверка Python
    print_header("🐍 ПРОВЕРКА PYTHON")
    checks['python'] = check_python_version()
    print()
    
    # Проверка зависимостей
    checks['dependencies'] = check_dependencies()
    
    # Проверка GPU
    gpu_ok, device_type = check_gpu()
    checks['gpu'] = gpu_ok
    
    # Проверка структуры проекта
    checks['structure'] = check_project_structure()
    
    # Проверка тестовых файлов
    check_test_files()
    
    # Проверка сервера
    checks['server'] = check_server_running()
    
    # Итоговая сводка
    all_ok = generate_summary(checks)
    
    # Рекомендации
    generate_recommendations(checks, device_type)
    
    print_separator()
    print()
    
    # Возвращаем код выхода
    return 0 if all_ok else 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)

