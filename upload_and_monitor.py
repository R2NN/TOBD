"""
Загрузка ValidationCase 13.zip с мониторингом прогресса
"""
import requests
import time
import pandas as pd
import zipfile
import io
import os

API_URL = "http://localhost:8001"

def upload_and_monitor():
    print("=" * 75)
    print("  ЗАГРУЗКА И МОНИТОРИНГ ValidationCase 13.zip")
    print("=" * 75)
    
    # 1. Загрузка
    print("\n📤 Загрузка файла...")
    zip_path = r"D:\Downloads\ValidationCase 13.zip"
    
    with open(zip_path, 'rb') as f:
        files = {'file': ('ValidationCase 13.zip', f, 'application/zip')}
        response = requests.post(
            f"{API_URL}/process/",
            files=files,
            data={'model': 'light'}
        )
    
    if response.status_code != 200:
        print(f"❌ Ошибка: {response.status_code} - {response.text}")
        return False
    
    result = response.json()
    session_id = result.get('session_id')
    print(f"✅ Загружено! Session ID: {session_id}")
    
    # 2. Мониторинг обработки
    print(f"\n⏳ Мониторинг обработки...")
    print(f"   (Проверяем появление файлов в storage/results)")
    
    max_wait = 180  # 3 минуты
    check_interval = 5
    
    for i in range(0, max_wait, check_interval):
        time.sleep(check_interval)
        
        # Проверяем появление файлов
        results_dir = "storage/results"
        if os.path.exists(results_dir):
            files = [f for f in os.listdir(results_dir) if f.endswith('.zip')]
            
            if files:
                latest_zip = files[0]  # Берем первый (должен быть только один)
                result_id = latest_zip[:-4]
                
                print(f"\n✅ Обработка завершена! (найден файл: {latest_zip})")
                
                # 3. Проверяем структуру
                return check_result_structure(result_id)
        
        print(f"   [{i+check_interval}s] Ожидание...")
    
    print(f"\n⚠️  Превышено время ожидания ({max_wait}с)")
    return False

def check_result_structure(result_id):
    """Проверяет структуру созданного результата"""
    print("\n" + "=" * 75)
    print(f"  ПРОВЕРКА СТРУКТУРЫ РЕЗУЛЬТАТА")
    print("=" * 75)
    
    zip_path = f"storage/results/{result_id}.zip"
    
    with zipfile.ZipFile(zip_path, 'r') as zf:
        if 'submit_report.xlsx' not in zf.namelist():
            print(f"❌ submit_report.xlsx не найден!")
            return False
        
        excel_data = zf.read('submit_report.xlsx')
        df = pd.read_excel(io.BytesIO(excel_data))
        
        print(f"\n📋 Структура submit_report.xlsx:")
        print(f"   Строк: {len(df)}")
        print(f"   Колонок: {len(df.columns)}")
        
        print(f"\n   Колонки:")
        for i, col in enumerate(df.columns, 1):
            print(f"      {i}. {col}")
        
        # Проверка формата
        required_cols_new = [
            'ID сценария', 'ID аномалии', 'ID проблемы',
            'Файл с проблемой', '№ строки проблемы', 'Строка лога проблемы',
            'Файл с аномалией', '№ строки аномалии', 'Строка лога аномалии'
        ]
        
        print(f"\n🔍 Проверка нового формата (9 колонок):")
        missing = []
        for col in required_cols_new:
            status = "✅" if col in df.columns else "❌"
            print(f"   {status} {col}")
            if col not in df.columns:
                missing.append(col)
        
        if missing:
            print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА!")
            print(f"   Отсутствуют колонки: {missing}")
            print(f"\n   Возможные причины:")
            print(f"   1. Изменения в orchestrator.py не применились")
            print(f"   2. Нужен перезапуск сервера (Ctrl+C, python main.py)")
            return False
        
        print(f"\n✅ ФОРМАТ КОРРЕКТНЫЙ (9 колонок)!")
        
        # Статистика
        if len(df) > 0:
            print(f"\n📊 Статистика:")
            unique_errors = df['ID проблемы'].nunique()
            unique_error_logs = df['Строка лога проблемы'].nunique()
            total_warnings = len(df)
            unique_warnings = df['ID аномалии'].nunique()
            
            print(f"   ERROR:")
            print(f"      Уникальных ID проблем: {unique_errors}")
            print(f"      Уникальных строк логов: {unique_error_logs}")
            
            print(f"   WARNING:")
            print(f"      Всего аномалий: {total_warnings}")
            print(f"      Уникальных аномалий: {unique_warnings}")
            
            print(f"\n   🎯 ОЖИДАЕМОЕ ОТОБРАЖЕНИЕ НА ДАШБОРДЕ:")
            print(f"      График должен показать:")
            print(f"      🔴 ERROR: {unique_error_logs} событий (дедуплицировано)")
            print(f"      🟠 WARNING: {total_warnings} событий")
            
            if unique_error_logs < total_warnings:
                print(f"\n      ✅ Есть дубликаты ERROR (это нормально)")
                print(f"         Дашборд должен показать МЕНЬШЕ ERROR чем WARNING")
            
            # Показываем первую строку
            print(f"\n📝 Первая строка (для проверки):")
            first_row = df.iloc[0]
            for col in df.columns:
                value = str(first_row[col])
                if len(value) > 60:
                    value = value[:60] + "..."
                print(f"   {col}: {value}")
        
        return True

# Запуск
if __name__ == "__main__":
    try:
        success = upload_and_monitor()
        
        if success:
            print("\n" + "=" * 75)
            print("  ✅ ВСЁ ГОТОВО!")
            print("=" * 75)
            print(f"\n🌐 Откройте дашборд:")
            print(f"   http://localhost:8001/dashboard?auto_load=true")
            print(f"\n💡 Откройте DevTools (F12) → Console")
            print(f"   Должны увидеть: 'Timeline chart rendered: X ERROR, Y WARNING'")
            print(f"\n🔍 Проверьте график Timeline:")
            print(f"   - 🔴 Красная линия (ERROR) - уникальные ошибки")
            print(f"   - 🟠 Оранжевая линия (WARNING) - все аномалии")
            print(f"   - 💛 Желтая пунктирная (ПРОГНОЗЫ)")
        else:
            print("\n" + "=" * 75)
            print("  ❌ ОБРАБОТКА НЕ ЗАВЕРШИЛАСЬ")
            print("=" * 75)
            print(f"\n  Попробуйте:")
            print(f"  1. Перезапустить сервер (Ctrl+C, python main.py)")
            print(f"  2. Загрузить через веб-интерфейс: http://localhost:8001/")
    
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

