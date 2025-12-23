"""
=============================================================================
test_api_v1.py - Тестирование API v1
=============================================================================

Скрипт для тестирования всех эндпоинтов API v1.
Проверяет функциональность batch processing, статусы, сравнение и экспорт.

Запуск:
    python test_api_v1.py

Требования:
    - Сервер должен быть запущен на http://localhost:8001
    - Должны быть тестовые ZIP файлы или скрипт создаст заглушки

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


# =============================================================================
# КОНФИГУРАЦИЯ
# =============================================================================

BASE_URL = "http://localhost:8001/api/v1"
API_KEY = "demo-api-key-123"
HEADERS = {"X-API-Key": API_KEY}


# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================

def print_section(title):
    """Печатает заголовок секции"""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70 + "\n")


def print_success(message):
    """Печатает успешное сообщение"""
    print(f"✅ {message}")


def print_error(message):
    """Печатает сообщение об ошибке"""
    print(f"❌ {message}")


def print_info(message):
    """Печатает информационное сообщение"""
    print(f"ℹ️  {message}")


def create_test_zip(filename, kb_format='csv'):
    """
    Создает тестовый ZIP файл для проверки.
    
    Args:
        filename: Имя ZIP файла
        kb_format: Формат базы знаний ('csv', 'xlsx', 'xls')
        
    Returns:
        bytes: Содержимое ZIP файла
    """
    # Создаем ZIP в памяти
    zip_buffer = io.BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        # Добавляем тестовый файл с базой знаний в нужном формате
        if kb_format == 'csv':
            kb_content = """Severity;Description;Recommendation
ERROR;Database connection failed;Check database configuration and network connectivity
ERROR;Out of memory error;Increase heap size or optimize memory usage
WARNING;High CPU usage detected;Monitor system resources and optimize processes
WARNING;Slow response time;Check network latency and server load"""
            
            zip_file.writestr('anomalies_problems.csv', kb_content)
        
        elif kb_format in ['xlsx', 'xls']:
            # Создаем Excel файл
            import pandas as pd
            
            kb_data = {
                'Severity': ['ERROR', 'ERROR', 'WARNING', 'WARNING'],
                'Description': [
                    'Database connection failed',
                    'Out of memory error',
                    'High CPU usage detected',
                    'Slow response time'
                ],
                'Recommendation': [
                    'Check database configuration and network connectivity',
                    'Increase heap size or optimize memory usage',
                    'Monitor system resources and optimize processes',
                    'Check network latency and server load'
                ]
            }
            
            df = pd.DataFrame(kb_data)
            excel_buffer = io.BytesIO()
            df.to_excel(excel_buffer, index=False, engine='openpyxl')
            excel_buffer.seek(0)
            
            kb_filename = f'anomalies_problems.{kb_format}'
            zip_file.writestr(kb_filename, excel_buffer.getvalue())
        
        # Добавляем тестовый лог файл
        log_content = """2025-10-23 10:00:00 [INFO] Application started
2025-10-23 10:00:01 [INFO] Loading configuration
2025-10-23 10:00:02 [ERROR] Database connection failed
2025-10-23 10:00:03 [WARNING] High CPU usage detected
2025-10-23 10:00:04 [INFO] Retrying connection
2025-10-23 10:00:05 [ERROR] Out of memory error
2025-10-23 10:00:06 [WARNING] Slow response time
2025-10-23 10:00:07 [INFO] Service recovered"""
        
        zip_file.writestr('test_logs.txt', log_content)
    
    zip_buffer.seek(0)
    return zip_buffer.getvalue()


# =============================================================================
# ТЕСТЫ
# =============================================================================

def test_1_batch_process():
    """Тест 1: Batch обработка файлов"""
    print_section("ТЕСТ 1: Batch Processing")
    
    # Создаем тестовые файлы
    print_info("Создание тестовых ZIP файлов...")
    test_files = {
        'test_file_1.zip': create_test_zip('test_file_1.zip'),
        'test_file_2.zip': create_test_zip('test_file_2.zip'),
    }
    
    # Отправляем запрос
    print_info("Отправка batch запроса...")
    
    files = [
        ('files', (name, content, 'application/zip'))
        for name, content in test_files.items()
    ]
    
    try:
        response = requests.post(
            f"{BASE_URL}/batch-process/",
            headers=HEADERS,
            params={"model": "light"},
            files=files,
            timeout=30
        )
        
        if response.status_code == 201:
            data = response.json()
            task_ids = [task['task_id'] for task in data['tasks']]
            
            print_success(f"Создано задач: {len(task_ids)}")
            print_info(f"Task IDs: {task_ids}")
            
            return task_ids
        else:
            print_error(f"Ошибка: {response.status_code}")
            print_error(f"Ответ: {response.text}")
            return []
            
    except Exception as e:
        print_error(f"Исключение: {e}")
        return []


def test_2_status_tracking(task_ids):
    """Тест 2: Отслеживание статуса задач"""
    print_section("ТЕСТ 2: Status Tracking")
    
    if not task_ids:
        print_error("Нет task_ids для проверки")
        return []
    
    completed_tasks = []
    max_wait_time = 300  # 5 минут максимум
    start_time = time.time()
    
    print_info(f"Ожидание завершения {len(task_ids)} задач...")
    
    while task_ids and (time.time() - start_time) < max_wait_time:
        for task_id in task_ids[:]:
            try:
                response = requests.get(
                    f"{BASE_URL}/status/{task_id}",
                    headers=HEADERS,
                    timeout=10
                )
                
                if response.status_code == 200:
                    data = response.json()
                    status = data['status']
                    progress = data['progress']
                    
                    print_info(f"Task {task_id[:8]}... - {status} ({progress}%)")
                    
                    if status == 'completed':
                        print_success(f"Задача {task_id[:8]}... завершена!")
                        completed_tasks.append(task_id)
                        task_ids.remove(task_id)
                    elif status == 'failed':
                        print_error(f"Задача {task_id[:8]}... завершилась с ошибкой!")
                        print_error(f"Ошибка: {data.get('error_message', 'Unknown')}")
                        task_ids.remove(task_id)
                        
            except Exception as e:
                print_error(f"Ошибка проверки статуса: {e}")
        
        if task_ids:
            time.sleep(5)
    
    if task_ids:
        print_error(f"Превышено время ожидания. Незавершенных задач: {len(task_ids)}")
    
    return completed_tasks


def test_3_compare_results(task_ids):
    """Тест 3: Сравнение результатов"""
    print_section("ТЕСТ 3: Compare Results")
    
    if len(task_ids) < 2:
        print_error("Недостаточно завершенных задач для сравнения (минимум 2)")
        return False
    
    print_info(f"Сравнение {len(task_ids)} результатов...")
    
    try:
        response = requests.post(
            f"{BASE_URL}/compare/",
            headers=HEADERS,
            json={"analysis_ids": task_ids[:10]},  # Максимум 10
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            
            print_success("Сравнение выполнено успешно!")
            print_info(f"Результатов в сравнении: {len(data['comparisons'])}")
            
            # Выводим сводку
            summary = data['summary']
            print_info("\nСводная статистика:")
            print(f"  Макс. ошибок: {summary['max_errors']}")
            print(f"  Мин. ошибок: {summary['min_errors']}")
            print(f"  Сред. ошибок: {summary['avg_errors']:.2f}")
            print(f"  Сред. время обработки: {summary['avg_processing_time']:.2f}s")
            
            return True
        else:
            print_error(f"Ошибка: {response.status_code}")
            print_error(f"Ответ: {response.text}")
            return False
            
    except Exception as e:
        print_error(f"Исключение: {e}")
        return False


def test_4_export_formats(task_id):
    """Тест 4: Экспорт в разных форматах"""
    print_section("ТЕСТ 4: Export Formats")
    
    if not task_id:
        print_error("Нет task_id для экспорта")
        return False
    
    formats = ['json', 'xml', 'pdf']
    results = {}
    
    for fmt in formats:
        print_info(f"Экспорт в формат {fmt.upper()}...")
        
        try:
            response = requests.get(
                f"{BASE_URL}/export/{task_id}/{fmt}",
                headers=HEADERS,
                timeout=30
            )
            
            if response.status_code == 200:
                # Сохраняем файл
                filename = f"test_export_{task_id[:8]}.{fmt}"
                with open(filename, 'wb') as f:
                    f.write(response.content)
                
                file_size = len(response.content)
                print_success(f"Экспорт {fmt.upper()} успешен! Размер: {file_size} байт")
                print_info(f"Сохранено в: {filename}")
                
                results[fmt] = True
            else:
                print_error(f"Ошибка экспорта {fmt.upper()}: {response.status_code}")
                results[fmt] = False
                
        except Exception as e:
            print_error(f"Исключение при экспорте {fmt.upper()}: {e}")
            results[fmt] = False
    
    return all(results.values())


def test_5_history():
    """Тест 5: История анализов"""
    print_section("ТЕСТ 5: History")
    
    print_info("Запрос истории анализов...")
    
    try:
        # Тест с различными параметрами
        tests = [
            {"params": {"limit": 10}, "name": "Последние 10"},
            {"params": {"limit": 5, "status": "completed"}, "name": "5 завершенных"},
            {"params": {"skip": 0, "limit": 20}, "name": "20 с начала"},
        ]
        
        for test in tests:
            response = requests.get(
                f"{BASE_URL}/history",
                headers=HEADERS,
                params=test['params'],
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                print_success(f"{test['name']}: найдено {len(data['items'])} из {data['total']}")
            else:
                print_error(f"{test['name']}: Ошибка {response.status_code}")
        
        return True
        
    except Exception as e:
        print_error(f"Исключение: {e}")
        return False


def test_6_download_zip(task_id):
    """Тест 6: Скачивание ZIP архива"""
    print_section("ТЕСТ 6: Download ZIP")
    
    if not task_id:
        print_error("Нет task_id для скачивания")
        return False
    
    print_info("Скачивание ZIP архива...")
    
    try:
        response = requests.get(
            f"{BASE_URL}/download/{task_id}",
            headers=HEADERS,
            timeout=30
        )
        
        if response.status_code == 200:
            # Сохраняем архив
            filename = f"test_download_{task_id[:8]}.zip"
            with open(filename, 'wb') as f:
                f.write(response.content)
            
            file_size = len(response.content)
            print_success(f"ZIP архив скачан! Размер: {file_size} байт")
            print_info(f"Сохранено в: {filename}")
            
            return True
        else:
            print_error(f"Ошибка: {response.status_code}")
            return False
            
    except Exception as e:
        print_error(f"Исключение: {e}")
        return False


def test_7_delete_result(task_id):
    """Тест 7: Удаление результата"""
    print_section("ТЕСТ 7: Delete Result")
    
    if not task_id:
        print_error("Нет task_id для удаления")
        return False
    
    print_info(f"Удаление результата {task_id[:8]}...")
    
    try:
        response = requests.delete(
            f"{BASE_URL}/result/{task_id}",
            headers=HEADERS,
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            print_success(f"Результат удален: {data['message']}")
            return True
        else:
            print_error(f"Ошибка: {response.status_code}")
            return False
            
    except Exception as e:
        print_error(f"Исключение: {e}")
        return False


def test_8_rate_limiting():
    """Тест 8: Rate Limiting"""
    print_section("ТЕСТ 8: Rate Limiting")
    
    print_info("Проверка rate limiting (отправка 15 запросов)...")
    
    rate_limited = False
    
    for i in range(15):
        try:
            response = requests.get(
                f"{BASE_URL}/history",
                headers=HEADERS,
                params={"limit": 1},
                timeout=10
            )
            
            if response.status_code == 429:
                print_success(f"Rate limiting сработал на запросе #{i+1}")
                rate_limited = True
                
                # Проверяем заголовки
                if 'Retry-After' in response.headers:
                    print_info(f"Retry-After: {response.headers['Retry-After']}s")
                
                break
            
        except Exception as e:
            print_error(f"Исключение: {e}")
            break
    
    if not rate_limited:
        print_info("Rate limiting не сработал (возможно лимит выше)")
    
    return True


def test_9_api_key_validation():
    """Тест 9: Валидация API ключа"""
    print_section("ТЕСТ 9: API Key Validation")
    
    # Тест без API ключа
    print_info("Запрос без API ключа...")
    try:
        response = requests.get(f"{BASE_URL}/history", timeout=10)
        
        if response.status_code == 401:
            print_success("Запрос без ключа корректно отклонен (401)")
        else:
            print_error(f"Неожиданный код: {response.status_code}")
    except Exception as e:
        print_error(f"Исключение: {e}")
    
    # Тест с неверным ключом
    print_info("Запрос с неверным API ключом...")
    try:
        response = requests.get(
            f"{BASE_URL}/history",
            headers={"X-API-Key": "invalid-key"},
            timeout=10
        )
        
        if response.status_code == 403:
            print_success("Неверный ключ корректно отклонен (403)")
        else:
            print_error(f"Неожиданный код: {response.status_code}")
    except Exception as e:
        print_error(f"Исключение: {e}")
    
    return True


# =============================================================================
# ДОПОЛНИТЕЛЬНЫЕ ТЕСТЫ
# =============================================================================

def test_10_excel_knowledge_base():
    """Тест 10: Поддержка Excel базы знаний"""
    print_section("ТЕСТ 10: Excel Knowledge Base Support")
    
    # Создаем тестовые файлы с разными форматами базы знаний
    print_info("Создание тестовых ZIP файлов с разными форматами базы знаний...")
    test_files = {
        'test_csv_kb.zip': create_test_zip('test_csv_kb.zip', kb_format='csv'),
        'test_xlsx_kb.zip': create_test_zip('test_xlsx_kb.zip', kb_format='xlsx'),
    }
    
    task_ids = []
    
    for filename, zip_data in test_files.items():
        print_info(f"Отправка файла: {filename}")
        
        files = {'files': (filename, zip_data, 'application/zip')}
        data = {'model_choice': 'light'}
        
        try:
            response = requests.post(
                f"{BASE_URL}/batch",
                files=files,
                data=data,
                headers=HEADERS,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                task_id = result[0]['task_id']
                kb_format = 'CSV' if 'csv' in filename else 'XLSX'
                print_success(f"Файл с {kb_format} базой знаний принят. Task ID: {task_id}")
                task_ids.append(task_id)
            else:
                print_error(f"Ошибка для {filename}: {response.status_code}")
                print(response.text)
        
        except Exception as e:
            print_error(f"Исключение при отправке {filename}: {e}")
    
    # Ждем завершения обработки
    if task_ids:
        print_info("Ожидание завершения обработки...")
        time.sleep(5)
        
        # Проверяем статус
        for task_id in task_ids:
            try:
                response = requests.get(
                    f"{BASE_URL}/status/{task_id}",
                    headers=HEADERS,
                    timeout=10
                )
                
                if response.status_code == 200:
                    result = response.json()
                    status = result.get('status', 'unknown')
                    if status == 'completed':
                        print_success(f"Task {task_id}: успешно обработан")
                    else:
                        print_info(f"Task {task_id}: {status}")
                else:
                    print_error(f"Не удалось получить статус для {task_id}")
            
            except Exception as e:
                print_error(f"Ошибка при проверке статуса {task_id}: {e}")
    
    print_success(f"Тест завершен! Обработано {len(task_ids)} файлов с разными форматами базы знаний")
    return task_ids


# =============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# =============================================================================

def main():
    """Запускает все тесты"""
    print("\n" + "=" * 70)
    print("  🧪 ТЕСТИРОВАНИЕ API v1")
    print("  " + "=" * 68)
    print(f"  URL: {BASE_URL}")
    print(f"  API Key: {API_KEY}")
    print("=" * 70)
    
    # Проверяем доступность сервера
    try:
        response = requests.get("http://localhost:8001/docs", timeout=5)
        if response.status_code == 200:
            print_success("Сервер доступен")
        else:
            print_error("Сервер недоступен")
            return
    except Exception as e:
        print_error(f"Не удается подключиться к серверу: {e}")
        print_info("Запустите сервер: python main.py")
        return
    
    # Запускаем тесты
    task_ids = test_1_batch_process()
    
    if task_ids:
        completed_tasks = test_2_status_tracking(task_ids)
        
        if completed_tasks:
            test_3_compare_results(completed_tasks)
            
            if len(completed_tasks) > 0:
                test_4_export_formats(completed_tasks[0])
                test_6_download_zip(completed_tasks[0])
                
                # Удаляем только последнюю задачу
                if len(completed_tasks) > 1:
                    test_7_delete_result(completed_tasks[-1])
    
    test_5_history()
    test_8_rate_limiting()
    test_9_api_key_validation()
    test_10_excel_knowledge_base()
    
    # Итоговая сводка
    print_section("ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
    print_success("Все основные тесты выполнены!")
    print_info("\nПроверьте логи сервера для дополнительной информации")
    print_info("Интерактивная документация: http://localhost:8001/docs")


if __name__ == "__main__":
    main()

