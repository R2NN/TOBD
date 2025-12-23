.PHONY: build up down restart logs shell clean help

# Переменные
COMPOSE = docker-compose
SERVICE = fastapi-app

help: ## Показать это сообщение помощи
	@echo "Доступные команды:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

build: ## Собрать Docker образ
	$(COMPOSE) build

up: ## Запустить контейнер
	$(COMPOSE) up

up-d: ## Запустить контейнер в фоновом режиме
	$(COMPOSE) up -d

down: ## Остановить и удалить контейнер
	$(COMPOSE) down

restart: ## Перезапустить контейнер
	$(COMPOSE) restart

logs: ## Показать логи контейнера
	$(COMPOSE) logs -f

shell: ## Войти в shell контейнера
	$(COMPOSE) exec $(SERVICE) bash

clean: ## Удалить контейнеры, образы и volumes
	$(COMPOSE) down -v --rmi all

rebuild: ## Пересобрать образ без кеша и запустить
	$(COMPOSE) build --no-cache
	$(COMPOSE) up -d

ps: ## Показать статус контейнеров
	$(COMPOSE) ps

stop: ## Остановить контейнер (без удаления)
	$(COMPOSE) stop

start: ## Запустить остановленный контейнер
	$(COMPOSE) start

dev: ## Запустить в режиме разработки с hot-reload
	$(COMPOSE) up --build

prune: ## Удалить неиспользуемые Docker ресурсы
	docker system prune -af

