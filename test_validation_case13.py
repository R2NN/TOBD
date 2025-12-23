"""
Тестирование ValidationCase 13.zip
"""
import requests
import time
import json

API_URL = "http://localhost:8002"

def upload_and_process():
    print("=" * 75)
    print("  ТЕСТИРОВАНИЕ ValidationCase 13.zip")
    print("=" * 75)
    
    # 1. Загрузка файла
    print("\n📤 Загрузка ValidationCase 13.zip...")
    
    zip_path = r"D:\Downloads\ValidationCase 13.zip"
    
    with open(zip_path, 'rb') as f:
        files = {'file': ('ValidationCase 13.zip', f, 'application/zip')}
        response = requests.post(
            f"{API_URL}/process/",
            files=files,
            data={
                'model': 'ollama_llama'
            }
        )
    
    if response.status_code != 200:
        print(f"❌ Ошибка загрузки: {response.status_code}")
        print(f"   Ответ: {response.text}")
        return None
    
    result = response.json()
    print(f"✅ Загружено! ID кейса: {result['case_id']}")
    
    # 2. Ожидание обработки
    case_id = result['case_id']
    print(f"\n⏳ Ожидание обработки (case_id: {case_id})...")
    
    max_attempts = 60
    for i in range(max_attempts):
        time.sleep(2)
        
        status_response = requests.get(
            f"{API_URL}/status/{case_id}",
            headers=HEADERS
        )
        
        if status_response.status_code == 200:
            status_data = status_response.json()
            current_status = status_data.get('status', 'unknown')
            progress = status_data.get('progress', 0)
            
            print(f"   [{i+1}/{max_attempts}] Статус: {current_status}, Прогресс: {progress}%")
            
            if current_status == 'completed':
                print("✅ Обработка завершена!")
                break
            elif current_status == 'failed':
                print(f"❌ Ошибка обработки: {status_data.get('message', 'Unknown error')}")
                return None
    else:
        print("❌ Timeout: обработка не завершилась за отведенное время")
        return None
    
    # 3. Получение результатов
    print(f"\n📊 Получение результатов...")
    
    results_response = requests.get(
        f"{API_URL}/results/{case_id}",
        headers=HEADERS
    )
    
    if results_response.status_code != 200:
        print(f"❌ Ошибка получения результатов: {results_response.status_code}")
        return None
    
    results = results_response.json()
    print("✅ Результаты получены!")
    
    # 4. Проверка структуры данных
    print("\n" + "=" * 75)
    print("  ПРОВЕРКА ДАННЫХ")
    print("=" * 75)
    
    reports = results.get('reports', {})
    
    # Проверяем submit_report.xlsx
    if 'submit_report.xlsx' in reports:
        print("\n✅ submit_report.xlsx найден")
        # Скачиваем и проверяем
        import io
        import pandas as pd
        
        excel_bytes = requests.get(
            f"{API_URL}/download/{case_id}/submit_report.xlsx",
            headers=HEADERS
        ).content
        
        df = pd.read_excel(io.BytesIO(excel_bytes))
        
        print(f"\n📋 Структура submit_report.xlsx:")
        print(f"   Строк: {len(df)}")
        print(f"   Колонок: {len(df.columns)}")
        print(f"\n   Названия колонок:")
        for col in df.columns:
            print(f"      - {col}")
        
        print(f"\n   Первые 3 строки:")
        print(df.head(3).to_string())
        
        # Проверяем наличие новых колонок
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
            print(f"\n⚠️  ВНИМАНИЕ: Отсутствуют колонки: {missing}")
            print("   Дашборд может не работать корректно!")
        else:
            print(f"\n✅ Все обязательные колонки присутствуют!")
        
        # Статистика
        print(f"\n📊 Статистика:")
        print(f"   Уникальных проблем (ERROR): {df['ID проблемы'].nunique()}")
        print(f"   Аномалий (WARNING): {len(df)}")
        print(f"   Уникальных аномалий: {df['ID аномалии'].nunique()}")
        
    else:
        print("❌ submit_report.xlsx не найден в результатах!")
    
    # Проверяем predictive_alerts.xlsx
    if 'predictive_alerts.xlsx' in reports:
        print("\n✅ predictive_alerts.xlsx найден")
    else:
        print("\n⚠️  predictive_alerts.xlsx не найден")
    
    # 5. URL дашборда
    print("\n" + "=" * 75)
    print("  ССЫЛКИ")
    print("=" * 75)
    print(f"\n🌐 Дашборд: http://localhost:8002/results/{case_id}")
    print(f"📊 API результаты: {API_URL}/results/{case_id}")
    
    return case_id

if __name__ == "__main__":
    try:
        case_id = upload_and_process()
        if case_id:
            print("\n" + "=" * 75)
            print("  ✅ ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
            print("=" * 75)
            print(f"\nОткройте дашборд: http://localhost:8002/results/{case_id}")
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

