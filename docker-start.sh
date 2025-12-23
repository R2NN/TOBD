#!/bin/bash
################################################################################
# Скрипт быстрого запуска Docker контейнера для Linux/macOS
################################################################################

echo ""
echo "========================================"
echo " FastAPI Log Analyzer - Docker Start"
echo "========================================"
echo ""

# Проверка установки Docker
if ! command -v docker &> /dev/null; then
    echo "[ERROR] Docker не установлен!"
    echo ""
    echo "Пожалуйста, установите Docker:"
    echo "https://docs.docker.com/get-docker/"
    exit 1
fi

echo "[OK] Docker установлен"
echo ""

# Проверка запущен ли Docker
if ! docker ps &> /dev/null; then
    echo "[ERROR] Docker не запущен!"
    echo ""
    echo "Пожалуйста, запустите Docker и повторите попытку."
    exit 1
fi

echo "[OK] Docker запущен"
echo ""

# Запрос режима запуска
echo "Выберите режим запуска:"
echo "1 - Обычный режим (в текущем окне, с логами)"
echo "2 - Фоновый режим (без вывода логов)"
echo ""
read -p "Введите номер (1 или 2): " mode

if [ "$mode" = "1" ]; then
    echo ""
    echo "[INFO] Запуск в обычном режиме..."
    echo "[INFO] Нажмите Ctrl+C для остановки"
    echo ""
    docker-compose up --build
elif [ "$mode" = "2" ]; then
    echo ""
    echo "[INFO] Запуск в фоновом режиме..."
    echo ""
    docker-compose up -d --build
    
    if [ $? -eq 0 ]; then
        echo ""
        echo "========================================"
        echo " [SUCCESS] Контейнер успешно запущен!"
        echo "========================================"
        echo ""
        echo " Приложение доступно на:"
        echo " http://localhost:8001"
        echo ""
        echo " Для просмотра логов:"
        echo " docker-compose logs -f"
        echo ""
        echo " Для остановки контейнера:"
        echo " docker-compose down"
        echo " или запустите ./docker-stop.sh"
        echo ""
    else
        echo ""
        echo "[ERROR] Ошибка при запуске контейнера!"
        echo ""
    fi
else
    echo ""
    echo "[ERROR] Неверный выбор!"
    exit 1
fi

