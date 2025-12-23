"""
=============================================================================
demo_showcase.py - Эффектная демонстрация API v1
=============================================================================

Скрипт для визуальной демонстрации всех возможностей API v1.
Идеально подходит для записи скринкаста!

Запуск:
    python demo_showcase.py

Требования:
    - Сервер должен быть запущен: python main.py
    - requests: pip install requests

Автор: Команда Atomichack 3.0
Дата: 2025
=============================================================================
"""

import requests
import time
import json
import zipfile
import io
import os
from datetime import datetime
from typing import Optional

# =============================================================================
# КОНФИГУРАЦИЯ
# =============================================================================

API_URL = "http://localhost:8002/api/v1"
API_KEY = "demo-api-key-123"
HEADERS = {"X-API-Key": API_KEY}

# Настройки демонстрации
DEMO_SPEED = "normal"  # "fast", "normal", "slow"
PAUSE_TIMES = {
    "fast": 0.5,
    "normal": 2.0,
    "slow": 4.0
}


# =============================================================================
# ЦВЕТА ДЛЯ ТЕРМИНАЛА (ANSI)
# =============================================================================

class Colors:
    """ANSI коды для цветного вывода"""
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    
    # Дополнительные цвета
    YELLOW = '\033[33m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    GRAY = '\033[90m'


# =============================================================================
# ФУНКЦИИ ДЛЯ КРАСИВОГО ВЫВОДА
# =============================================================================

def print_header(text: str, char: str = "="):
    """Печатает большой заголовок"""
    width = 75
    print("\n" + Colors.HEADER + Colors.BOLD)
    print(char * width)
    print(f"  {text}".center(width))
    print(char * width)
    print(Colors.ENDC)


def print_section(number: int, title: str):
    """Печатает заголовок секции"""
    print("\n" + Colors.OKCYAN + Colors.BOLD)
    print("─" * 75)
    print(f"  🔷 ШАГ {number}: {title}")
    print("─" * 75)
    print(Colors.ENDC)


def print_success(text: str):
    """Печатает сообщение об успехе"""
    print(Colors.OKGREEN + "✅ " + text + Colors.ENDC)


def print_info(text: str):
    """Печатает информационное сообщение"""
    print(Colors.OKBLUE + "ℹ️  " + text + Colors.ENDC)


def print_warning(text: str):
    """Печатает предупреждение"""
    print(Colors.WARNING + "⚠️  " + text + Colors.ENDC)


def print_error(text: str):
    """Печатает ошибку"""
    print(Colors.FAIL + "❌ " + text + Colors.ENDC)


def print_json(data: dict, indent: int = 2):
    """Печатает JSON с подсветкой"""
    json_str = json.dumps(data, indent=indent, ensure_ascii=False)
    print(Colors.GRAY + json_str + Colors.ENDC)


def print_progress_bar(progress: int, width: int = 50):
    """Рисует прогресс-бар"""
    filled = int(width * progress / 100)
    bar = "█" * filled + "░" * (width - filled)
    print(f"\r{Colors.CYAN}[{bar}] {progress}%{Colors.ENDC}", end="", flush=True)


def pause(message: str = ""):
    """Пауза между шагами"""
    if message:
        print(Colors.GRAY + f"\n{message}" + Colors.ENDC)
    time.sleep(PAUSE_TIMES[DEMO_SPEED])


def separator():
    """Разделитель"""
    print(Colors.GRAY + "─" * 75 + Colors.ENDC)


# =============================================================================
# ФУНКЦИИ ДЛЯ РАБОТЫ С API
# =============================================================================

def check_server():
    """Проверяет доступность сервера"""
    try:
        response = requests.get("http://localhost:8002/docs", timeout=5)
        return response.status_code == 200
    except Exception:
        return False


def create_test_zip(filename: str, kb_format: str = 'csv') -> bytes:
    """
    Создает тестовый ZIP файл с логами.
    
    Args:
        filename: Имя создаваемого архива
        kb_format: Формат базы знаний ('csv', 'xlsx', 'xls')
        
    Returns:
        bytes: Содержимое ZIP файла
    """
    zip_buffer = io.BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        # База знаний - создаем в зависимости от запрошенного формата
        if kb_format == 'csv':
            kb_content = """Severity;Description;Recommendation
ERROR;Database connection failed;Check database configuration and network connectivity
ERROR;Out of memory error;Increase heap size or optimize memory usage
ERROR;Authentication failed;Verify credentials and access permissions
ERROR;Timeout exception;Increase timeout value or check network
WARNING;High CPU usage detected;Monitor system resources and optimize processes
WARNING;Slow response time;Check network latency and server load
WARNING;Disk space low;Clean up unnecessary files
WARNING;Memory usage high;Check for memory leaks"""
            
            zip_file.writestr('anomalies_problems.csv', kb_content)
        
        elif kb_format in ['xlsx', 'xls']:
            # Создаем Excel файл с той же структурой
            import pandas as pd
            
            kb_data = {
                'Severity': ['ERROR', 'ERROR', 'ERROR', 'ERROR', 'WARNING', 'WARNING', 'WARNING', 'WARNING'],
                'Description': [
                    'Database connection failed',
                    'Out of memory error',
                    'Authentication failed',
                    'Timeout exception',
                    'High CPU usage detected',
                    'Slow response time',
                    'Disk space low',
                    'Memory usage high'
                ],
                'Recommendation': [
                    'Check database configuration and network connectivity',
                    'Increase heap size or optimize memory usage',
                    'Verify credentials and access permissions',
                    'Increase timeout value or check network',
                    'Monitor system resources and optimize processes',
                    'Check network latency and server load',
                    'Clean up unnecessary files',
                    'Check for memory leaks'
                ]
            }
            
            df = pd.DataFrame(kb_data)
            excel_buffer = io.BytesIO()
            df.to_excel(excel_buffer, index=False, engine='openpyxl')
            excel_buffer.seek(0)
            
            kb_filename = f'anomalies_problems.{kb_format}'
            zip_file.writestr(kb_filename, excel_buffer.getvalue())
        
        # Лог файл с различными проблемами
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_content = f"""{timestamp} [INFO] Application started successfully
{timestamp} [INFO] Loading configuration from config.yml
{timestamp} [INFO] Connecting to database: localhost:5432
{timestamp} [ERROR] Database connection failed: Connection refused
{timestamp} [WARNING] Retrying connection (attempt 1/3)
{timestamp} [ERROR] Database connection failed: Connection refused
{timestamp} [WARNING] Retrying connection (attempt 2/3)
{timestamp} [ERROR] Database connection failed: Connection refused
{timestamp} [ERROR] Maximum retry attempts reached
{timestamp} [WARNING] High CPU usage detected: 87%
{timestamp} [INFO] Attempting to start in offline mode
{timestamp} [ERROR] Authentication failed for user 'admin'
{timestamp} [WARNING] Multiple failed login attempts detected
{timestamp} [ERROR] Out of memory error: heap space exhausted
{timestamp} [INFO] Initiating graceful shutdown
{timestamp} [WARNING] Slow response time: 5234ms
{timestamp} [ERROR] Timeout exception: Request timeout after 10s
{timestamp} [INFO] Cleanup completed
{timestamp} [INFO] Application stopped"""
        
        zip_file.writestr(f'logs_{filename}.txt', log_content)
    
    zip_buffer.seek(0)
    return zip_buffer.getvalue()


# =============================================================================
# ОСНОВНАЯ ДЕМОНСТРАЦИЯ
# =============================================================================

def demo_introduction():
    """Вступление"""
    print_header("🎬 ДЕМОНСТРАЦИЯ API V1", "═")
    
    print(Colors.BOLD + "\n📋 Что будет продемонстрировано:" + Colors.ENDC)
    print("  1️⃣  Batch обработка нескольких файлов")
    print("  2️⃣  Отслеживание статуса в реальном времени")
    print("  3️⃣  Сравнение результатов анализов")
    print("  4️⃣  Экспорт в различных форматах (JSON, XML, PDF)")
    print("  5️⃣  Просмотр истории анализов")
    print("  6️⃣  Скачивание результатов")
    print("  7️⃣  Удаление результатов")
    
    print(Colors.BOLD + "\n🔧 Конфигурация:" + Colors.ENDC)
    print(f"  • API URL: {Colors.CYAN}{API_URL}{Colors.ENDC}")
    print(f"  • API Key: {Colors.CYAN}{API_KEY}{Colors.ENDC}")
    print(f"  • Скорость: {Colors.CYAN}{DEMO_SPEED}{Colors.ENDC}")
    
    pause("\n⏳ Проверка доступности сервера...")


def demo_step1_batch_process() -> list:
    """Шаг 1: Batch обработка"""
    print_section(1, "BATCH ОБРАБОТКА ФАЙЛОВ")
    
    # Используем реальный рабочий файл
    real_zip_path = r"D:\Downloads\ValidationCase 13.zip"
    
    print_info(f"Использование реального файла: {real_zip_path}")
    
    # Проверяем существование файла
    import os
    if not os.path.exists(real_zip_path):
        print_error(f"Файл не найден: {real_zip_path}")
        print_info("Пожалуйста, убедитесь, что файл существует")
        return []
    
    # Читаем файл
    with open(real_zip_path, 'rb') as f:
        zip_data = f.read()
    
    file_size = len(zip_data)
    print_success(f"Файл загружен: {file_size:,} байт")
    
    # Отправляем один и тот же файл 3 раза (для демонстрации batch processing)
    test_files = []
    filenames = ['ValidationCase_13_Test1', 'ValidationCase_13_Test2', 'ValidationCase_13_Test3']
    
    for name in filenames:
        test_files.append(('files', (f'{name}.zip', zip_data, 'application/zip')))
        print_success(f"Подготовлен: {name}.zip ({file_size:,} байт)")
    
    pause("\n📤 Отправка файлов на сервер...")
    separator()
    
    try:
        response = requests.post(
            f"{API_URL}/batch-process/",
            headers=HEADERS,
            params={"model": "light"},
            files=test_files,
            timeout=30
        )
        
        if response.status_code == 201:
            data = response.json()
            
            print_success(f"Batch запрос успешно обработан!")
            print_info(f"Создано задач: {data['total_files']}")
            print_info(f"В очереди: {data['queued']}")
            
            print("\n" + Colors.BOLD + "📋 Созданные задачи:" + Colors.ENDC)
            task_ids = []
            for i, task in enumerate(data['tasks'], 1):
                task_id = task['task_id']
                task_ids.append(task_id)
                print(f"  {i}. {Colors.YELLOW}{task_id}{Colors.ENDC}")
                print(f"     Статус: {Colors.CYAN}{task['status']}{Colors.ENDC}")
                print(f"     {task['message']}")
            
            return task_ids
        else:
            print_error(f"Ошибка: {response.status_code}")
            print_json(response.json())
            return []
            
    except Exception as e:
        print_error(f"Исключение: {e}")
        return []


def show_task_results(task_id: str, status_data: dict):
    """Показывает детальные результаты завершенной задачи"""
    try:
        # Получаем детальные результаты
        response = requests.get(
            f"{API_URL}/export/{task_id}/json",
            headers=HEADERS,
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            
            print(f"\n   {Colors.CYAN}📊 РЕЗУЛЬТАТЫ АНАЛИЗА:{Colors.ENDC}")
            print(f"   {'─' * 45}")
            
            # Базовая статистика
            total_logs = result.get('total_logs', 0)
            total_errors = result.get('total_errors', 0)
            total_warnings = result.get('total_warnings', 0)
            proc_time = result.get('processing_time_seconds', 0)
            
            print(f"   📝 Обработано логов: {Colors.BOLD}{total_logs}{Colors.ENDC}")
            print(f"   🔴 Найдено ошибок: {Colors.FAIL}{total_errors}{Colors.ENDC}")
            print(f"   ⚠️  Предупреждений: {Colors.WARNING}{total_warnings}{Colors.ENDC}")
            print(f"   ⏱️  Время обработки: {Colors.GRAY}{proc_time:.2f}s{Colors.ENDC}")
            
            # Показываем найденные аномалии из отчета
            data = result.get('data', {})
            submit_report = data.get('submit_report.xlsx', [])
            
            if submit_report and len(submit_report) > 0:
                anomalies_count = len(submit_report)
                print(f"\n   🔍 Обнаружено аномалий: {Colors.FAIL}{Colors.BOLD}{anomalies_count}{Colors.ENDC}")
                
                # Показываем первые 3 аномалии как примеры
                print(f"\n   {Colors.BOLD}Примеры найденных проблем:{Colors.ENDC}")
                for i, anomaly in enumerate(submit_report[:3], 1):
                    scenario_id = anomaly.get('ID сценария', '?')
                    anomaly_id = anomaly.get('ID аномалии', '?')
                    problem_id = anomaly.get('ID проблемы', '?')
                    log_line = anomaly.get('Строка из лога', '')[:60]
                    
                    print(f"   {Colors.YELLOW}{i}.{Colors.ENDC} Аномалия #{anomaly_id} → Проблема #{problem_id}")
                    print(f"      {Colors.GRAY}{log_line}...{Colors.ENDC}")
                
                if anomalies_count > 3:
                    print(f"   {Colors.GRAY}   ... и еще {anomalies_count - 3} аномалий{Colors.ENDC}")
            
            # Предсказательные алерты
            predictive = data.get('predictive_alerts.xlsx', [])
            if predictive and len(predictive) > 0:
                print(f"\n   🔮 Предсказательных алертов: {Colors.CYAN}{len(predictive)}{Colors.ENDC}")
            
            print(f"   {'─' * 45}\n")
            
    except Exception as e:
        # Тихо игнорируем ошибки - это не критично для демо
        pass


def demo_step2_track_status(task_ids: list) -> list:
    """Шаг 2: Отслеживание статуса"""
    print_section(2, "ОТСЛЕЖИВАНИЕ СТАТУСА ОБРАБОТКИ")
    
    if not task_ids:
        print_error("Нет задач для отслеживания")
        return []
    
    print_info(f"Отслеживание {len(task_ids)} задач...")
    print_info("Нажмите Ctrl+C для пропуска ожидания\n")
    
    completed_tasks = []
    max_iterations = 60  # Максимум 2 минуты ожидания
    iteration = 0
    
    try:
        while task_ids and iteration < max_iterations:
            iteration += 1
            
            for task_id in task_ids[:]:
                try:
                    response = requests.get(
                        f"{API_URL}/status/{task_id}",
                        headers=HEADERS,
                        timeout=10
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        status = data['status']
                        progress = data['progress']
                        filename = data.get('filename', 'Unknown')
                        
                        # Красивый вывод статуса
                        status_color = {
                            'pending': Colors.YELLOW,
                            'processing': Colors.CYAN,
                            'completed': Colors.OKGREEN,
                            'failed': Colors.FAIL
                        }.get(status, Colors.WHITE)
                        
                        print(f"\n{Colors.BOLD}📁 {filename[:30]}{Colors.ENDC}")
                        print(f"   ID: {Colors.GRAY}{task_id[:8]}...{Colors.ENDC}")
                        print(f"   Статус: {status_color}{status.upper()}{Colors.ENDC}")
                        
                        # Прогресс-бар
                        if status == 'processing':
                            print(f"   Прогресс: ", end="")
                            print_progress_bar(progress, width=40)
                            print()
                            
                            if data.get('estimated_completion'):
                                eta = data['estimated_completion']
                                print(f"   ETA: {Colors.GRAY}{eta}{Colors.ENDC}")
                        else:
                            print(f"   Прогресс: {progress}%")
                        
                        # Проверяем завершение
                        if status == 'completed':
                            print_success(f"   ✨ Задача завершена!")
                            
                            # ПОКАЗЫВАЕМ РЕЗУЛЬТАТЫ АНАЛИЗА!
                            show_task_results(task_id, data)
                            
                            completed_tasks.append(task_id)
                            task_ids.remove(task_id)
                        elif status == 'failed':
                            print_error(f"   ❌ Ошибка: {data.get('error_message', 'Unknown')}")
                            task_ids.remove(task_id)
                            
                except Exception as e:
                    print_error(f"Ошибка проверки {task_id[:8]}: {e}")
            
            if task_ids:
                time.sleep(2)
        
        separator()
        print_success(f"\n✨ Завершено задач: {len(completed_tasks)}")
        
        return completed_tasks
        
    except KeyboardInterrupt:
        print_warning("\n\n⏭️  Ожидание прервано пользователем")
        return completed_tasks + task_ids  # Возвращаем все задачи


def demo_step3_compare(task_ids: list):
    """Шаг 3: Сравнение результатов"""
    print_section(3, "СРАВНЕНИЕ РЕЗУЛЬТАТОВ АНАЛИЗОВ")
    
    if len(task_ids) < 2:
        print_warning("Недостаточно завершенных задач для сравнения (минимум 2)")
        print_info(f"Доступно задач: {len(task_ids)}")
        pause()
        return
    
    # Берем первые 3 задачи для сравнения
    compare_ids = task_ids[:3]
    
    print_info(f"Сравнение {len(compare_ids)} результатов...")
    print("Задачи для сравнения:")
    for i, task_id in enumerate(compare_ids, 1):
        print(f"  {i}. {Colors.YELLOW}{task_id}{Colors.ENDC}")
    
    pause("\n🔄 Выполнение сравнения...")
    separator()
    
    try:
        response = requests.post(
            f"{API_URL}/compare/",
            headers=HEADERS,
            json={"analysis_ids": compare_ids},
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            
            print_success("Сравнение выполнено успешно!\n")
            
            # Таблица результатов
            print(Colors.BOLD + "📊 СРАВНИТЕЛЬНАЯ ТАБЛИЦА:" + Colors.ENDC)
            print("─" * 85)
            print(f"{'Файл':<30} {'Логов':<8} {'Аномалий':<10} {'Ошибок':<8} {'Время (s)':<10}")
            print("─" * 85)
            
            for item in data['comparisons']:
                filename = item['filename'][:29]
                logs = item['total_logs']
                
                # Подсчитываем аномалии из детального отчета
                anomalies = 0
                try:
                    task_id_for_detail = item.get('task_id', '')
                    if task_id_for_detail:
                        detail_resp = requests.get(f"{API_URL}/export/{task_id_for_detail}/json", headers=HEADERS, timeout=5)
                        if detail_resp.status_code == 200:
                            detail_data = detail_resp.json()
                            submit_report = detail_data.get('data', {}).get('submit_report.xlsx', [])
                            anomalies = len(submit_report)
                except:
                    pass
                
                errors = item['total_errors']
                warnings = item['total_warnings']
                proc_time = item['processing_time']
                
                anomaly_color = Colors.FAIL if anomalies > 0 else Colors.OKGREEN
                print(f"{filename:<30} {logs:<8} {anomaly_color}{anomalies:<10}{Colors.ENDC} {errors:<8} {proc_time:<10.2f}")
            
            print("─" * 85)
            
            # Сводная статистика
            summary = data['summary']
            print("\n" + Colors.BOLD + "📈 СВОДНАЯ СТАТИСТИКА:" + Colors.ENDC)
            print(f"  • Всего анализов: {Colors.CYAN}{summary['total_analyses']}{Colors.ENDC}")
            print(f"  • Макс. ошибок: {Colors.FAIL}{summary['max_errors']}{Colors.ENDC}")
            print(f"  • Мин. ошибок: {Colors.OKGREEN}{summary['min_errors']}{Colors.ENDC}")
            print(f"  • Сред. ошибок: {Colors.YELLOW}{summary['avg_errors']:.1f}{Colors.ENDC}")
            print(f"  • Сред. время обработки: {Colors.CYAN}{summary['avg_processing_time']:.2f}s{Colors.ENDC}")
            
        else:
            print_error(f"Ошибка сравнения: {response.status_code}")
            
    except Exception as e:
        print_error(f"Исключение: {e}")
    
    pause()


def demo_step4_export(task_id: str):
    """Шаг 4: Экспорт в разных форматах"""
    print_section(4, "ЭКСПОРТ РЕЗУЛЬТАТОВ")
    
    if not task_id:
        print_error("Нет задачи для экспорта")
        return
    
    print_info(f"Экспорт результатов задачи: {Colors.YELLOW}{task_id[:16]}...{Colors.ENDC}")
    
    formats = [
        ('json', 'JSON', 'application/json'),
        ('xml', 'XML', 'application/xml'),
        ('pdf', 'PDF', 'application/pdf')
    ]
    
    exported_files = []
    
    for fmt, name, content_type in formats:
        print(f"\n📄 Экспорт в формат {Colors.BOLD}{name}{Colors.ENDC}...")
        
        try:
            response = requests.get(
                f"{API_URL}/export/{task_id}/{fmt}",
                headers=HEADERS,
                timeout=30
            )
            
            if response.status_code == 200:
                filename = f"demo_export_{task_id[:8]}.{fmt}"
                
                with open(filename, 'wb') as f:
                    f.write(response.content)
                
                file_size = len(response.content)
                print_success(f"Экспортировано: {filename} ({file_size:,} байт)")
                exported_files.append(filename)
            else:
                print_error(f"Ошибка экспорта {name}: {response.status_code}")
                
        except Exception as e:
            print_error(f"Исключение при экспорте {name}: {e}")
    
    if exported_files:
        separator()
        print("\n" + Colors.BOLD + "📦 Экспортированные файлы:" + Colors.ENDC)
        for filename in exported_files:
            print(f"  • {Colors.CYAN}{filename}{Colors.ENDC}")
    
    pause()


def demo_step5_history():
    """Шаг 5: История анализов"""
    print_section(5, "ИСТОРИЯ АНАЛИЗОВ")
    
    print_info("Получение истории всех анализов...")
    pause()
    
    try:
        response = requests.get(
            f"{API_URL}/history",
            headers=HEADERS,
            params={"limit": 10},
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            
            print_success(f"Найдено записей: {data['total']}")
            print_info(f"Показано: {len(data['items'])}\n")
            
            separator()
            
            for i, item in enumerate(data['items'], 1):
                status_icons = {
                    'completed': '✅',
                    'processing': '⏳',
                    'pending': '⏸️',
                    'failed': '❌'
                }
                
                status_colors = {
                    'completed': Colors.OKGREEN,
                    'processing': Colors.CYAN,
                    'pending': Colors.YELLOW,
                    'failed': Colors.FAIL
                }
                
                icon = status_icons.get(item['status'], '❓')
                color = status_colors.get(item['status'], Colors.WHITE)
                
                print(f"{Colors.BOLD}{i}. {icon} {item['filename']}{Colors.ENDC}")
                print(f"   ID: {Colors.GRAY}{item['task_id']}{Colors.ENDC}")
                print(f"   Статус: {color}{item['status']}{Colors.ENDC}")
                print(f"   Модель: {item['model']}")
                print(f"   Создано: {item['created_at']}")
                
                if item.get('completed_at'):
                    print(f"   Завершено: {item['completed_at']}")
                
                print()
            
        else:
            print_error(f"Ошибка получения истории: {response.status_code}")
            
    except Exception as e:
        print_error(f"Исключение: {e}")
    
    pause()


def demo_step6_download(task_id: str):
    """Шаг 6: Скачивание ZIP архива"""
    print_section(6, "СКАЧИВАНИЕ РЕЗУЛЬТАТОВ")
    
    if not task_id:
        print_error("Нет задачи для скачивания")
        return
    
    print_info(f"Скачивание ZIP архива: {Colors.YELLOW}{task_id[:16]}...{Colors.ENDC}")
    pause()
    
    try:
        response = requests.get(
            f"{API_URL}/download/{task_id}",
            headers=HEADERS,
            timeout=30
        )
        
        if response.status_code == 200:
            filename = f"demo_results_{task_id[:8]}.zip"
            
            with open(filename, 'wb') as f:
                f.write(response.content)
            
            file_size = len(response.content)
            
            separator()
            print_success(f"ZIP архив скачан!")
            print_info(f"Файл: {Colors.CYAN}{filename}{Colors.ENDC}")
            print_info(f"Размер: {Colors.CYAN}{file_size:,}{Colors.ENDC} байт")
            
            # Показываем содержимое ZIP
            print("\n" + Colors.BOLD + "📦 Содержимое архива:" + Colors.ENDC)
            
            with zipfile.ZipFile(filename, 'r') as zip_file:
                for file_info in zip_file.filelist:
                    print(f"  • {file_info.filename} ({file_info.file_size:,} байт)")
            
        else:
            print_error(f"Ошибка скачивания: {response.status_code}")
            
    except Exception as e:
        print_error(f"Исключение: {e}")
    
    pause()


def demo_step7_delete(task_id: str):
    """Шаг 7: Удаление результата (опционально)"""
    print_section(7, "УДАЛЕНИЕ РЕЗУЛЬТАТОВ (ОПЦИОНАЛЬНО)")
    
    if not task_id:
        print_error("Нет задачи для удаления")
        return
    
    print_warning("⚠️  ВНИМАНИЕ: Операция необратима!")
    print_info(f"Будет удалена задача: {Colors.YELLOW}{task_id[:16]}...{Colors.ENDC}")
    
    # В демо режиме не удаляем, просто показываем
    print_warning("\n🔒 Удаление пропущено в демо-режиме")
    print_info("Для удаления раскомментируйте код в demo_step7_delete()")
    
    # Раскомментируйте для реального удаления:
    """
    pause("\n🗑️  Выполнение удаления...")
    
    try:
        response = requests.delete(
            f"{API_URL}/result/{task_id}",
            headers=HEADERS,
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            print_success(f"Результаты удалены: {data['message']}")
        else:
            print_error(f"Ошибка удаления: {response.status_code}")
            
    except Exception as e:
        print_error(f"Исключение: {e}")
    """
    
    pause()


def demo_conclusion():
    """Заключение"""
    print_header("🎉 ДЕМОНСТРАЦИЯ ЗАВЕРШЕНА", "═")
    
    print(Colors.BOLD + "\n✅ Продемонстрировано:" + Colors.ENDC)
    print("  ✅ Batch обработка нескольких файлов")
    print("  ✅ Отслеживание статуса в реальном времени")
    print("  ✅ Сравнение результатов анализов")
    print("  ✅ Экспорт в JSON, XML, PDF форматах")
    print("  ✅ Просмотр истории анализов")
    print("  ✅ Скачивание ZIP архивов")
    print("  ✅ Управление результатами")
    
    print(Colors.BOLD + "\n📚 Дополнительная информация:" + Colors.ENDC)
    print("  📄 API_V1_GUIDE.md - полное руководство")
    print("  📄 API_V1_QUICKSTART.md - быстрый старт")
    print("  🌐 http://localhost:8001/docs - интерактивная документация")
    
    print(Colors.BOLD + "\n🔧 API v1 готов к использованию!" + Colors.ENDC)
    print("  • 7 эндпоинтов")
    print("  • Async обработка")
    print("  • Rate limiting")
    print("  • API key авторизация")
    
    print("\n" + Colors.HEADER + "═" * 75 + Colors.ENDC)
    print(Colors.BOLD + "  Спасибо за внимание! 🚀".center(75) + Colors.ENDC)
    print(Colors.HEADER + "═" * 75 + Colors.ENDC + "\n")


# =============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# =============================================================================

def main():
    """Главная функция демонстрации"""
    
    # Вступление
    demo_introduction()
    
    # Проверка сервера
    if not check_server():
        print_error("❌ Сервер недоступен!")
        print_info("\nЗапустите сервер командой:")
        print(f"  {Colors.CYAN}python main.py{Colors.ENDC}\n")
        return
    
    print_success("✅ Сервер доступен")
    pause("\n🚀 Начинаем демонстрацию...\n")
    
    # Шаг 1: Batch обработка
    task_ids = demo_step1_batch_process()
    
    if not task_ids:
        print_error("Не удалось создать задачи. Демонстрация прервана.")
        return
    
    pause()
    
    # Шаг 2: Отслеживание статуса
    completed_tasks = demo_step2_track_status(task_ids)
    
    pause()
    
    # Шаг 3: Сравнение (если есть завершенные задачи)
    if len(completed_tasks) >= 2:
        demo_step3_compare(completed_tasks)
    
    # Шаг 4: Экспорт (первая задача)
    if completed_tasks:
        demo_step4_export(completed_tasks[0])
    
    # Шаг 5: История
    demo_step5_history()
    
    # Шаг 6: Скачивание (первая задача)
    if completed_tasks:
        demo_step6_download(completed_tasks[0])
    
    # Шаг 7: Удаление (последняя задача, опционально)
    if len(completed_tasks) > 1:
        demo_step7_delete(completed_tasks[-1])
    
    # Заключение
    demo_conclusion()


# =============================================================================
# ТОЧКА ВХОДА
# =============================================================================

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n" + Colors.WARNING + "⏹️  Демонстрация прервана пользователем" + Colors.ENDC)
        print("До свидания! 👋\n")
    except Exception as e:
        print_error(f"\n❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()

