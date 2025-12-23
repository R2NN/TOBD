"""
Проверка существующих результатов
"""
import pandas as pd
import zipfile
import io
import json
import os

def check_result(result_id):
    print("=" * 75)
    print(f"  ПРОВЕРКА РЕЗУЛЬТАТА: {result_id}")
    print("=" * 75)
    
    zip_path = f"storage/results/{result_id}.zip"
    json_path = f"storage/results/{result_id}.json"
    
    if not os.path.exists(zip_path):
        print(f"❌ ZIP файл не найден: {zip_path}")
        return False
    
    # Читаем метаданные
    if os.path.exists(json_path):
        with open(json_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
            print(f"\n📝 Метаданные:")
            print(f"   ID: {metadata.get('id', 'N/A')}")
            print(f"   Filename: {metadata.get('filename', 'N/A')}")
            print(f"   Timestamp: {metadata.get('timestamp', 'N/A')}")
            print(f"   Model: {metadata.get('model', 'N/A')}")
    
    # Читаем ZIP
    with zipfile.ZipFile(zip_path, 'r') as zf:
        files = zf.namelist()
        print(f"\n📦 Файлы в ZIP:")
        for file in files:
            print(f"   - {file}")
        
        # Проверяем submit_report.xlsx
        if 'submit_report.xlsx' in files:
            print(f"\n✅ submit_report.xlsx найден!")
            
            excel_data = zf.read('submit_report.xlsx')
            df = pd.read_excel(io.BytesIO(excel_data))
            
            print(f"\n📋 Структура submit_report.xlsx:")
            print(f"   Строк: {len(df)}")
            print(f"   Колонок: {len(df.columns)}")
            print(f"\n   Названия колонок:")
            for i, col in enumerate(df.columns, 1):
                print(f"      {i}. {col}")
            
            # Проверяем новые колонки
            new_cols = ['Файл с аномалией', '№ строки аномалии', 'Строка лога аномалии']
            renamed_cols = ['№ строки проблемы', 'Строка лога проблемы']
            
            print(f"\n🔍 Проверка изменений:")
            
            has_new_cols = all(col in df.columns for col in new_cols)
            has_renamed_cols = all(col in df.columns for col in renamed_cols)
            
            if has_new_cols and has_renamed_cols:
                print(f"   ✅ НОВЫЙ ФОРМАТ (9 колонок) - изменения применены!")
                return True
            else:
                print(f"   ❌ СТАРЫЙ ФОРМАТ (6 колонок) - изменения НЕ применены!")
                print(f"\n   Отсутствующие новые колонки:")
                for col in new_cols:
                    if col not in df.columns:
                        print(f"      - {col}")
                print(f"\n   Отсутствующие переименованные колонки:")
                for col in renamed_cols:
                    if col not in df.columns:
                        print(f"      - {col}")
                return False
        else:
            print(f"\n❌ submit_report.xlsx НЕ найден в ZIP!")
            return False

# Проверяем все результаты
results_dir = "storage/results"
result_ids = []

for file in os.listdir(results_dir):
    if file.endswith('.zip'):
        result_id = file[:-4]  # убираем .zip
        result_ids.append(result_id)

print(f"Найдено результатов: {len(result_ids)}")
print()

all_new_format = True
for result_id in result_ids:
    is_new = check_result(result_id)
    if not is_new:
        all_new_format = False
    print()

print("=" * 75)
if all_new_format and result_ids:
    print("✅ ВСЕ РЕЗУЛЬТАТЫ В НОВОМ ФОРМАТЕ!")
    print("\nМожно открыть дашборд и проверить отображение:")
    print(f"http://localhost:8001/dashboard?auto_load=true")
else:
    print("❌ НАЙДЕНЫ РЕЗУЛЬТАТЫ В СТАРОМ ФОРМАТЕ!")
    print("\nРешение:")
    print("1. Удалите старые результаты: rm storage/results/*")
    print("2. Перезапустите сервер: Ctrl+C, затем python main.py")
    print("3. Загрузите файл заново через http://localhost:8001/")
print("=" * 75)

