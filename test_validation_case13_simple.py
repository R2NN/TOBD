"""
Простой тест ValidationCase 13.zip
"""
import requests
import time
import pandas as pd
import io

API_URL = "http://localhost:8001"

def test_case13():
    print("=" * 75)
    print("  ТЕСТИРОВАНИЕ ValidationCase 13.zip")
    print("=" * 75)
    
    # 1. Загрузка файла
    print("\n📤 Загрузка ValidationCase 13.zip через /process/...")
    
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
    
    # 2. Ожидание обработки (30 секунд)
    print(f"\n⏳ Ожидание обработки (30 сек)...")
    time.sleep(30)
    
    # 3. Получение последних результатов
    print(f"\n📊 Получение результатов через /api/latest-results...")
    
    results_response = requests.get(f"{API_URL}/api/latest-results")
    
    if results_response.status_code != 200:
        print(f"❌ Ошибка получения результатов: {results_response.status_code}")
        return
    
    results = results_response.json()
    
    if 'error' in results:
        print(f"❌ {results['error']}")
        return
    
    print("✅ Результаты получены!")
    
    # 4. Анализ данных
    print("\n" + "=" * 75)
    print("  АНАЛИЗ ДАННЫХ")
    print("=" * 75)
    
    data = results.get('data', {})
    
    # Проверяем submit данные
    submit_data = data.get('submit', [])
    predictions_data = data.get('predictions', [])
    novel_data = data.get('novel', [])
    
    print(f"\n📊 Количество записей:")
    print(f"   Submit (WARNING): {len(submit_data)}")
    print(f"   Predictions: {len(predictions_data)}")
    print(f"   Novel anomalies: {len(novel_data)}")
    
    if submit_data:
        # Преобразуем в DataFrame для анализа
        df = pd.DataFrame(submit_data)
        
        print(f"\n📋 Структура submit данных:")
        print(f"   Колонок: {len(df.columns)}")
        print(f"\n   Названия колонок:")
        for col in df.columns:
            print(f"      - {col}")
        
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
            print("   Это может быть причиной проблем на дашборде!")
            print("\n   Возможные причины:")
            print("   1. Данные созданы до обновления orchestrator.py")
            print("   2. Обработка еще не завершена")
            print("   3. Ошибка при обработке")
        else:
            print(f"\n✅ Все обязательные колонки присутствуют!")
        
        # Статистика
        if 'ID проблемы' in df.columns:
            print(f"\n📊 Статистика:")
            print(f"   Уникальных проблем (ERROR): {df['ID проблемы'].nunique()}")
            print(f"   Аномалий (WARNING): {len(df)}")
            if 'ID аномалии' in df.columns:
                print(f"   Уникальных аномалий: {df['ID аномалии'].nunique()}")
        
        # Показываем первую строку для отладки
        print(f"\n📝 Первая строка данных:")
        if len(df) > 0:
            first_row = df.iloc[0]
            for col in df.columns:
                value = first_row[col]
                if isinstance(value, str) and len(value) > 60:
                    value = value[:60] + "..."
                print(f"   {col}: {value}")
    else:
        print("\n⚠️  Submit данных нет!")
    
    print("\n" + "=" * 75)
    print("  ДОСТУП К ДАШБОРДУ")
    print("=" * 75)
    print(f"\n🌐 Откройте: http://localhost:8001/dashboard?auto_load=true")
    print(f"\n💡 Нажмите F12 в браузере и посмотрите Console на наличие ошибок JavaScript")

if __name__ == "__main__":
    try:
        test_case13()
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

