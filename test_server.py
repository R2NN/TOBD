"""Тестовый сервер для проверки API v1"""
from fastapi import FastAPI

app = FastAPI(title="Test API v1")

# Пробуем импортировать и подключить роутер
try:
    print(">>> Импорт API v1 router...")
    from api.v1 import router as api_v1_router
    print(f">>> Router импортирован: {api_v1_router}")
    print(f">>> Prefix: {api_v1_router.prefix}")
    print(f">>> Routes count: {len(api_v1_router.routes)}")
    
    print(">>> Подключение router...")
    app.include_router(api_v1_router)
    print(">>> Router подключен успешно!")
    
    # Проверяем зарегистрированные routes
    print("\n>>> Зарегистрированные пути:")
    for route in app.routes:
        if hasattr(route, 'path'):
            print(f"    {route.path}")
    
except Exception as e:
    print(f">>> ОШИБКА: {e}")
    import traceback
    traceback.print_exc()

if __name__ == "__main__":
    import uvicorn
    print("\n>>> Запуск тестового сервера на порту 8002...")
    uvicorn.run(app, host="127.0.0.1", port=8002)

