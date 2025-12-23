import requests

try:
    response = requests.get('http://localhost:8001/openapi.json', timeout=5)
    data = response.json()
    
    print("Доступные эндпоинты:")
    print("=" * 70)
    for path in sorted(data['paths'].keys()):
        methods = list(data['paths'][path].keys())
        print(f"{path:50} {methods}")
    
    # Проверяем наличие API v1
    api_v1_paths = [p for p in data['paths'].keys() if p.startswith('/api/v1')]
    print(f"\nНайдено API v1 эндпоинтов: {len(api_v1_paths)}")
    
except Exception as e:
    print(f"Ошибка: {e}")

