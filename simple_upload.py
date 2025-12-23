"""
Простая загрузка без ожидания - просто загружаем файл
"""
import requests

print("Загрузка ValidationCase 13.zip...")

with open(r"D:\Downloads\ValidationCase 13.zip", 'rb') as f:
    files = {'file': ('ValidationCase 13.zip', f, 'application/zip')}
    response = requests.post(
        "http://localhost:8001/process/",
        files=files,
        data={'model': 'light'}
    )

if response.status_code == 200:
    result = response.json()
    session_id = result.get('session_id')
    print(f"✅ Загружено! Session ID: {session_id}")
    print(f"\nТеперь смотрите в терминал сервера!")
    print(f"Должны появиться сообщения:")
    print(f"  >>> [ОТЧЕТ] Создан submit_report.xlsx: ...")
    print(f"\nПодождите 30-60 секунд, затем запустите:")
    print(f"  python check_existing_results.py")
else:
    print(f"❌ Ошибка: {response.status_code} - {response.text}")

