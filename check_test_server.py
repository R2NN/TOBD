import requests

print("Проверка тестового сервера на порту 8002...")

try:
    response = requests.get('http://localhost:8002/openapi.json', timeout=5)
    data = response.json()
    
    print("\nДоступные эндпоинты:")
    print("=" * 70)
    for path in sorted(data['paths'].keys()):
        methods = list(data['paths'][path].keys())
        print(f"{path:50} {methods}")
    
    # Проверяем наличие API v1
    api_v1_paths = [p for p in data['paths'].keys() if p.startswith('/api/v1')]
    print(f"\n✅ Найдено API v1 эндпоинтов: {len(api_v1_paths)}")
    
    if api_v1_paths:
        print("\nAPI v1 эндпоинты:")
        for path in api_v1_paths:
            print(f"  • {path}")
    
except Exception as e:
    print(f"❌ Ошибка: {e}")

