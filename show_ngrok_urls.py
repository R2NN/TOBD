#!/usr/bin/env python3
"""
Скрипт для получения и отображения активных ngrok туннелей
Автоматически получает публичные URL и сохраняет их в файл для судей
"""
import requests
import json
from datetime import datetime
import sys

def get_ngrok_tunnels():
    """Получить список активных туннелей из ngrok API"""
    try:
        response = requests.get('http://127.0.0.1:4040/api/tunnels', timeout=5)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.ConnectionError:
        print("❌ Ошибка: Не удалось подключиться к ngrok API")
        print("\n⚠️  Убедитесь, что ngrok запущен!")
        print("   Запустите: .\\🚀_ЗАПУСТИТЬ_NGROK_ВСЁ.bat")
        return None
    except requests.exceptions.Timeout:
        print("❌ Ошибка: Превышено время ожидания ответа от ngrok API")
        return None
    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка подключения к ngrok API: {e}")
        return None

def format_tunnels(data):
    """Красиво отформатировать информацию о туннелях"""
    if not data or 'tunnels' not in data:
        return "❌ Нет активных туннелей"
    
    tunnels = data['tunnels']
    if not tunnels:
        return "❌ Нет активных туннелей"
    
    output = []
    output.append("=" * 80)
    output.append("🌐 АКТИВНЫЕ ПУБЛИЧНЫЕ ССЫЛКИ NGROK - ATOMICHACK 3.0")
    output.append("=" * 80)
    output.append(f"📅 Дата и время генерации: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    output.append("")
    
    # Группируем туннели по именам
    tunnel_map = {
        'loganalyzer': {
            'emoji': '🌐',
            'title': 'FastAPI приложение - Анализ логов',
            'description': 'Основной сервис: загрузка ZIP, ML-анализ, экспорт отчетов',
            'tips': [
                'Загрузите ZIP архив с .txt логами',
                'Получите JSON/PDF/XML отчет с анализом',
                'Используйте WebSocket для мониторинга в реальном времени'
            ]
        },
        'grafana': {
            'emoji': '📈',
            'title': 'Grafana - Визуализация метрик',
            'description': 'Дашборды и графики системы мониторинга',
            'tips': [
                'Логин: admin, Пароль: admin',
                'Откройте Dashboard "Log Analysis Overview"',
                'Посмотрите графики обработки логов в реальном времени'
            ]
        },
        'prometheus': {
            'emoji': '📊',
            'title': 'Prometheus - Сбор метрик',
            'description': 'Raw метрики и PromQL запросы',
            'tips': [
                'Откройте раздел "Graph" для визуализации',
                'Используйте запрос: log_analyzer_total_logs_processed',
                'Проверьте Targets для статуса сервисов'
            ]
        }
    }
    
    # Словарь для хранения https URL (приоритет)
    tunnel_urls = {}
    
    for tunnel in tunnels:
        name = tunnel.get('name', 'unknown')
        public_url = tunnel.get('public_url', 'N/A')
        proto = tunnel.get('proto', 'N/A')
        
        if name in tunnel_map:
            # Сохраняем https версию (приоритет) или http если https нет
            if proto == 'https':
                tunnel_urls[name] = public_url
            elif name not in tunnel_urls and proto == 'http':
                tunnel_urls[name] = public_url
    
    # Выводим информацию о туннелях в определенном порядке
    for name in ['loganalyzer', 'grafana', 'prometheus']:
        if name in tunnel_urls and name in tunnel_map:
            info = tunnel_map[name]
            output.append("-" * 80)
            output.append(f"{info['emoji']} {info['title']}")
            output.append("-" * 80)
            output.append(f"📝 Описание: {info['description']}")
            output.append(f"🔗 URL: {tunnel_urls[name]}")
            output.append("")
            output.append("💡 Полезные советы:")
            for tip in info['tips']:
                output.append(f"   • {tip}")
            output.append("")
    
    output.append("=" * 80)
    output.append("📋 КРАТКАЯ ИНСТРУКЦИЯ ДЛЯ СУДЕЙ")
    output.append("=" * 80)
    output.append("")
    output.append("1️⃣  Откройте FastAPI ссылку:")
    if 'loganalyzer' in tunnel_urls:
        output.append(f"    {tunnel_urls['loganalyzer']}")
    output.append("    → Загрузите демо архив с логами или используйте свой")
    output.append("")
    output.append("2️⃣  Откройте Grafana ссылку:")
    if 'grafana' in tunnel_urls:
        output.append(f"    {tunnel_urls['grafana']}")
    output.append("    → Логин: admin, Пароль: admin")
    output.append("    → Посмотрите дашборды с метриками")
    output.append("")
    output.append("3️⃣  Откройте Prometheus ссылку:")
    if 'prometheus' in tunnel_urls:
        output.append(f"    {tunnel_urls['prometheus']}")
    output.append("    → Проверьте raw метрики и targets")
    output.append("")
    output.append("=" * 80)
    output.append("🎯 ОСНОВНЫЕ ВОЗМОЖНОСТИ СИСТЕМЫ")
    output.append("=" * 80)
    output.append("✅ ML-анализ логов (классификация, аномалии, дубликаты)")
    output.append("✅ Экспорт в JSON, PDF, XML форматы")
    output.append("✅ WebSocket для мониторинга в реальном времени")
    output.append("✅ Prometheus + Grafana для метрик и визуализации")
    output.append("✅ Docker контейнеризация для легкого развертывания")
    output.append("=" * 80)
    output.append("")
    output.append("📞 Atomichack 3.0 Team | Октябрь 2025")
    output.append("")
    
    return "\n".join(output)

def save_to_file(content, filename='📋_ПУБЛИЧНЫЕ_ССЫЛКИ.txt'):
    """Сохранить ссылки в файл"""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"\n✅ Публичные ссылки сохранены в файл: {filename}")
        print(f"   Отправьте этот файл судьям!")
        return True
    except Exception as e:
        print(f"\n⚠️  Не удалось сохранить в файл: {e}")
        return False

def save_raw_json(data, filename='ngrok_tunnels_raw.json'):
    """Сохранить raw JSON для отладки"""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"✅ Raw данные сохранены в: {filename}")
        return True
    except Exception as e:
        print(f"⚠️  Не удалось сохранить raw JSON: {e}")
        return False

def main():
    print("\n" + "=" * 80)
    print("🔍 ПОЛУЧЕНИЕ ИНФОРМАЦИИ О NGROK ТУННЕЛЯХ")
    print("=" * 80)
    print()
    
    data = get_ngrok_tunnels()
    if not data:
        print("\n❌ Не удалось получить данные о туннелях")
        print("\nПопробуйте:")
        print("  1. Убедитесь, что ngrok запущен: .\\🚀_ЗАПУСТИТЬ_NGROK_ВСЁ.bat")
        print("  2. Проверьте что ngrok API доступен: http://127.0.0.1:4040")
        print("  3. Подождите несколько секунд после запуска ngrok")
        sys.exit(1)
    
    formatted_output = format_tunnels(data)
    print(formatted_output)
    
    # Сохранить в файл для судей
    if save_to_file(formatted_output):
        print()
    
    # Также сохранить raw JSON для разработки
    save_raw_json(data)
    
    print("\n" + "=" * 80)
    print("✅ ГОТОВО! Все ссылки получены и сохранены")
    print("=" * 80)
    print()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Прервано пользователем")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Непредвиденная ошибка: {e}")
        sys.exit(1)

