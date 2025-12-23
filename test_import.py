print("Попытка импорта API v1...")

try:
    print("1. Импорт api.v1.models...")
    from api.v1 import models
    print("   ✅ models импортирован")
    
    print("2. Импорт api.v1.storage...")
    from api.v1 import storage
    print("   ✅ storage импортирован")
    
    print("3. Импорт api.v1.tasks...")
    from api.v1 import tasks
    print("   ✅ tasks импортирован")
    
    print("4. Импорт api.v1.middleware...")
    from api.v1 import middleware
    print("   ✅ middleware импортирован")
    
    print("5. Импорт api.v1.routes...")
    from api.v1 import routes
    print("   ✅ routes импортирован")
    
    print("6. Импорт router...")
    from api.v1 import router
    print(f"   ✅ router импортирован: {router}")
    
    print("\n✅ ВСЕ МОДУЛИ ИМПОРТИРОВАНЫ УСПЕШНО!")
    
except Exception as e:
    print(f"\n❌ ОШИБКА ПРИ ИМПОРТЕ: {e}")
    import traceback
    traceback.print_exc()

