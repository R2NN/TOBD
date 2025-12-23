"""
Детальная проверка обработки ValidationCase 13.zip
"""
import requests
import time
import pandas as pd
import io
import os

API_URL = "http://localhost:8001"

def check_processing():
    print("=" * 75)
    print("  ДЕТАЛЬНАЯ ПРОВЕРКА ValidationCase 13.zip")
    print("=" * 75)
    
    # 1. Загрузка файла
    print("\n📤 Загрузка ValidationCase 13.zip...")
    
    zip_path = r"D:\Downloads\ValidationCase 13.zip"
    
    with open(zip_path, 'rb') as f:
        files = {'file': ('ValidationCase 13.zip', f, 'application/zip')}
        response = requests.post(
            f"{API_URL}/process/",
            files=files,
            data={'model': 'light'}
        )
    
    if response.status_code != 200:
        print(f"❌ Ошибка загрузки: {response.status_code}")
        print(f"   Ответ: {response.text}")
        return
    
    result = response.json()
    session_id = result.get('session_id')
    print(f"✅ Загружено! Session ID: {session_id}")
    
    # 2. Ожидание с проверкой каждые 5 секунд (макс 120 сек)
    print(f"\n⏳ Ожидание обработки (макс 120 сек)...")
    
    max_wait = 120
    check_interval = 5
    
    for i in range(0, max_wait, check_interval):
        time.sleep(check_interval)
        
        results_response = requests.get(f"{API_URL}/api/latest-results")
        
        if results_response.status_code == 200:
            results = results_response.json()
            
            if 'error' not in results:
                data = results.get('data', {})
                submit_count = len(data.get('submit', []))
                
                print(f"   [{i+check_interval}s] Проверка... Submit записей: {submit_count}")
                
                if submit_count > 0:
                    print(f"✅ Обработка завершена! Найдено {submit_count} записей")
                    break
        
        if i + check_interval >= max_wait:
            print("⚠️  Превышено время ожидания!")
    
    # 3. Проверка файлов в storage
    print(f"\n📁 Проверка файлов в storage/results...")
    storage_path = "storage/results"
    
    if os.path.exists(storage_path):
        cases = [d for d in os.listdir(storage_path) if os.path.isdir(os.path.join(storage_path, d))]
        print(f"   Найдено кейсов: {len(cases)}")
        
        if cases:
            # Берем последний кейс
            latest_case = sorted(cases)[-1]
            case_path = os.path.join(storage_path, latest_case)
            
            print(f"\n   Последний кейс: {latest_case}")
            print(f"   Путь: {case_path}")
            
            files = os.listdir(case_path)
            print(f"\n   Файлы в директории:")
            for file in files:
                file_path = os.path.join(case_path, file)
                size = os.path.getsize(file_path)
                print(f"      - {file} ({size} bytes)")
            
            # Проверяем submit_report.xlsx
            submit_file = os.path.join(case_path, "submit_report.xlsx")
            if os.path.exists(submit_file):
                print(f"\n✅ submit_report.xlsx найден!")
                
                df = pd.read_excel(submit_file)
                
                print(f"\n📋 Структура submit_report.xlsx:")
                print(f"   Строк: {len(df)}")
                print(f"   Колонок: {len(df.columns)}")
                print(f"\n   Названия колонок:")
                for i, col in enumerate(df.columns, 1):
                    print(f"      {i}. {col}")
                
                # Проверяем обязательные колонки
                required_cols = [
                    'ID сценария',
                    'ID аномалии',
                    'ID проблемы',
                    'Файл с проблемой',
                    '№ строки проблемы',
                    'Строка лога проблемы',
                    'Файл с аномалией',
                    '№ строки аномалии',
                    'Строка лога аномалии'
                ]
                
                print(f"\n🔍 Проверка обязательных колонок:")
                missing = []
                for col in required_cols:
                    if col in df.columns:
                        print(f"   ✅ {col}")
                    else:
                        print(f"   ❌ {col} - ОТСУТСТВУЕТ!")
                        missing.append(col)
                
                if missing:
                    print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА!")
                    print(f"   Отсутствуют колонки: {missing}")
                    print(f"\n   Это значит, что изменения в orchestrator.py не применились!")
                    print(f"   Решение: Перезапустите сервер (Ctrl+C и python main.py)")
                else:
                    print(f"\n✅ ВСЕ ОБЯЗАТЕЛЬНЫЕ КОЛОНКИ ПРИСУТСТВУЮТ!")
                
                # Статистика
                if 'ID проблемы' in df.columns:
                    print(f"\n📊 Статистика:")
                    unique_errors = df['ID проблемы'].nunique()
                    total_warnings = len(df)
                    print(f"   Уникальных проблем (ERROR): {unique_errors}")
                    print(f"   Всего аномалий (WARNING): {total_warnings}")
                    
                    if 'ID аномалии' in df.columns:
                        unique_warnings = df['ID аномалии'].nunique()
                        print(f"   Уникальных аномалий: {unique_warnings}")
                    
                    # Проверяем дубликаты ERROR
                    if 'Строка лога проблемы' in df.columns:
                        unique_error_logs = df['Строка лога проблемы'].nunique()
                        print(f"\n   🔍 Проверка дубликатов ERROR:")
                        print(f"      Уникальных строк ERROR логов: {unique_error_logs}")
                        print(f"      Всего строк в файле: {total_warnings}")
                        
                        if unique_error_logs < total_warnings:
                            print(f"      ✅ Есть дубликаты ERROR (это нормально)")
                            print(f"      ⚠️  Дашборд должен показывать: {unique_error_logs} ERROR, {total_warnings} WARNING")
                        else:
                            print(f"      ℹ️  Каждый ERROR уникален")
                
                # Показываем первые 2 строки
                print(f"\n📝 Первые 2 строки для проверки:")
                print(df.head(2).to_string())
            else:
                print(f"\n❌ submit_report.xlsx НЕ НАЙДЕН!")
    else:
        print(f"   ❌ Директория storage/results не существует!")
    
    # 4. Финальные ссылки
    print("\n" + "=" * 75)
    print("  ССЫЛКИ ДЛЯ ПРОВЕРКИ")
    print("=" * 75)
    print(f"\n🌐 Дашборд: http://localhost:8001/dashboard?auto_load=true")
    print(f"💡 Откройте Chrome DevTools (F12) → Console")
    print(f"   Проверьте сообщение: 'Timeline chart rendered: X ERROR, Y WARNING'")

if __name__ == "__main__":
    try:
        check_processing()
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

