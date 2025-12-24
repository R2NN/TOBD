# Enterprise Log Intelligence Platform

**Распределённая система обработки и анализа корпоративных логов с применением технологий Big Data.**


---

### Технологический стек

<table align="center">
  <tr>
    <td align="center"><strong>Orchestration</strong></td>
    <td align="center"><strong>Big Data</strong></td>
    <td align="center"><strong>Storage</strong></td>
    <td align="center"><strong>Backend</strong></td>
    <td align="center"><strong>AI / ML</strong></td>
    <td align="center"><strong>Monitoring</strong></td>
  </tr>
  <tr>
    <td align="center">
      <a href="https://airflow.apache.org/" target="_blank"><img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white" alt="Apache Airflow"/></a>
    </td>
    <td align="center">
      <a href="https://www.dask.org/" target="_blank"><img src="https://img.shields.io/badge/Dask-FDA061?style=for-the-badge&logo=dask&logoColor=white" alt="Dask"/></a>
    </td>
    <td align="center">
      <a href="https://www.postgresql.org/" target="_blank"><img src="https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL"/></a>
    </td>
    <td align="center">
      <a href="https://www.python.org/" target="_blank"><img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/></a><br>
      <a href="https://fastapi.tiangolo.com/" target="_blank"><img src="https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white" alt="FastAPI"/></a>
    </td>
    <td align="center">
      <a href="https://pytorch.org/" target="_blank"><img src="https://img.shields.io/badge/PyTorch-EE4C2C?style=for-the-badge&logo=pytorch&logoColor=white" alt="PyTorch"/></a><br>
      <a href="https://huggingface.co/sentence-transformers" target="_blank"><img src="https://img.shields.io/badge/Sentence_BERT-FFD21E?style=for-the-badge&logo=huggingface&logoColor=black" alt="Sentence-BERT"/></a>
    </td>
    <td align="center">
      <a href="https://www.docker.com/" target="_blank"><img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker"/></a><br>
      <a href="https://grafana.com/" target="_blank"><img src="https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white" alt="Grafana"/></a><br>
      <a href="https://prometheus.io/" target="_blank"><img src="https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=prometheus&logoColor=white" alt="Prometheus"/></a>
    </td>
  </tr>
</table>

---

##  Описание проекта

Система реализует полный цикл работы с данными:

```
Источник данных (ZIP / CSV / JSON)
         ↓
  Airflow (ETL Pipeline)
         ↓
PostgreSQL (RAW → Staging → DWH)
         ↓
   Dask (параллельная обработка)
         ↓
  ML-анализ (Sentence-BERT)
         ↓
 Визуализация (Grafana / Dashboard)
```

---

##  Быстрый старт

### Запуск всего стека одной командой:

```bash
# Клонируйте репозиторий
git clone <URL>
cd Enterprise-Log-Intelligence-Platform

# Запустите сервис
docker-compose up -d
```

### Доступ к сервисам:

| Сервис | URL | Описание |
|--------|-----|----------|
| **FastAPI** | http://localhost:8001 | Веб-интерфейс + REST API |
| **Grafana** | http://localhost:3000 | Дашборды (admin/admin) |
| **Dask Dashboard** | http://localhost:8787 | Мониторинг Dask |
| **Prometheus** | http://localhost:9090 | Метрики |
| **PostgreSQL** | localhost:5432 | База данных |

---

## 📁 Структура проекта

```
📁 Enterprise-Log-Intelligence-Platform/
 ┣ 📂 data/              # Исходные данные
 ┣ 📂 flows/             # ETL Pipeline (Extract, Transform, Load)
 ┣ 📂 dask_jobs/         # Параллельная обработка с Dask
 ┣ 📂 processing/        # ML-модули анализа
 ┣ 📂 grafana/dashboards/# дашборды с  визуализацией
 ┣ 📂 api/               # REST API
 ┣ 📄 docker-compose.yml # Docker конфигурация
 ┣ 📄 init-db.sql        # Инициализация PostgreSQL
 ┣ 📄 requirements.txt   # Python зависимости
 ┣ 📄 ARCHITECTURE.md    # Документация архитектуры
 ┗ 📄 report.md          # Отчёт по проекту
```


##  Компоненты системы

### ETL Pipeline (`flows/etl_flow.py`)
- **Extract**: Извлечение данных из ZIP, CSV, JSON
- **Transform**: Парсинг + ML-классификация с Dask
- **Load**: Сохранение в PostgreSQL и файлы

### Dask Processing (`dask_jobs/dask_processing.py`)
- Параллельный парсинг лог-файлов
- Распределённая генерация эмбеддингов
- Batch-обработка классификации

### PostgreSQL (`init-db.sql`)
- RAW Layer: `raw_logs`
- Staging Layer: `processed_logs`
- DWH Layer: `analysis_results`, `incidents`

### ML-анализ (`processing/`)
- Sentence-BERT для семантического анализа
- Классификация ERROR → Problem ID
- Классификация WARNING → Anomaly ID

---

##  Визуализация

### Grafana Dashboards (6 шт.)
- System Load
- ML Performance
- Errors & Anomalies
- FastAPI Metrics
- Logs Analysis
- Complete Overview

### FastAPI Dashboard
- KPI карточки
- Timeline Chart
- Time Machine
- Export (JSON, PDF, Excel)

---

##  Документация

- `ARCHITECTURE.md` — Подробная архитектура системы
- `report.md` — Отчёт по проектной работе
- `presentation.md` — Презентация проекта

---
