-- =============================================================================
-- init-db.sql - Инициализация базы данных PostgreSQL
-- =============================================================================
-- Этот скрипт выполняется автоматически при первом запуске контейнера PostgreSQL
-- и создаёт необходимые таблицы для хранения результатов анализа логов.
-- =============================================================================

-- Создание расширений
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =============================================================================
-- Таблица: raw_logs (Сырые данные - RAW Layer)
-- =============================================================================
CREATE TABLE IF NOT EXISTS raw_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    timestamp TIMESTAMP NOT NULL,
    level VARCHAR(20) NOT NULL,
    category VARCHAR(100),
    message TEXT,
    raw_log TEXT NOT NULL,
    file_name VARCHAR(255),
    line_number INTEGER,
    source_archive VARCHAR(255),
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Индексы для быстрого поиска
    CONSTRAINT chk_level CHECK (level IN ('ERROR', 'WARNING', 'INFO', 'DEBUG'))
);

CREATE INDEX IF NOT EXISTS idx_raw_logs_timestamp ON raw_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_logs_level ON raw_logs(level);
CREATE INDEX IF NOT EXISTS idx_raw_logs_file ON raw_logs(file_name);

-- =============================================================================
-- Таблица: processed_logs (Обработанные данные - Staging Layer)
-- =============================================================================
CREATE TABLE IF NOT EXISTS processed_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    raw_log_id UUID REFERENCES raw_logs(id),
    timestamp TIMESTAMP NOT NULL,
    level VARCHAR(20) NOT NULL,
    message TEXT,
    generalized_message TEXT,
    file_name VARCHAR(255),
    line_number INTEGER,
    
    -- ML классификация
    problem_id INTEGER DEFAULT 0,
    anomaly_id INTEGER DEFAULT 0,
    match_score FLOAT DEFAULT 0.0,
    
    -- Метаданные
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    model_used VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_processed_logs_timestamp ON processed_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_processed_logs_problem_id ON processed_logs(problem_id);
CREATE INDEX IF NOT EXISTS idx_processed_logs_anomaly_id ON processed_logs(anomaly_id);

-- =============================================================================
-- Таблица: analysis_results (Аналитика - Analytics/DWH Layer)
-- =============================================================================
CREATE TABLE IF NOT EXISTS analysis_results (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    scenario_id VARCHAR(50) NOT NULL,
    analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Статистика
    total_logs INTEGER DEFAULT 0,
    total_errors INTEGER DEFAULT 0,
    total_warnings INTEGER DEFAULT 0,
    unique_problems INTEGER DEFAULT 0,
    unique_anomalies INTEGER DEFAULT 0,
    
    -- Временной диапазон
    time_range_start TIMESTAMP,
    time_range_end TIMESTAMP,
    
    -- Результаты
    result_json JSONB,
    
    -- Метаданные
    model_used VARCHAR(100),
    processing_duration_sec FLOAT
);

CREATE INDEX IF NOT EXISTS idx_analysis_results_scenario ON analysis_results(scenario_id);
CREATE INDEX IF NOT EXISTS idx_analysis_results_date ON analysis_results(analysis_date);

-- =============================================================================
-- Таблица: incidents (Инциденты для отчетов)
-- =============================================================================
CREATE TABLE IF NOT EXISTS incidents (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    scenario_id VARCHAR(50) NOT NULL,
    anomaly_id INTEGER NOT NULL,
    problem_id INTEGER NOT NULL,
    
    -- Данные о проблеме
    error_file VARCHAR(255),
    error_line INTEGER,
    error_log TEXT,
    
    -- Данные об аномалии
    warning_file VARCHAR(255),
    warning_line INTEGER,
    warning_log TEXT,
    
    -- Метрики
    impact_score FLOAT DEFAULT 0.0,
    
    -- Временные метки
    error_timestamp TIMESTAMP,
    warning_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_incidents_scenario ON incidents(scenario_id);
CREATE INDEX IF NOT EXISTS idx_incidents_problem ON incidents(problem_id);

-- =============================================================================
-- Таблица: predictive_alerts (Предсказательные алерты)
-- =============================================================================
CREATE TABLE IF NOT EXISTS predictive_alerts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    scenario_id VARCHAR(50) NOT NULL,
    alert_type VARCHAR(50) DEFAULT 'PREDICTION',
    
    -- Триггер
    trigger_problem_id INTEGER,
    trigger_log TEXT,
    trigger_timestamp TIMESTAMP,
    
    -- Предсказание
    predicted_anomaly_id INTEGER,
    predicted_warning TEXT,
    confidence_score FLOAT,
    
    -- Статус
    is_verified BOOLEAN DEFAULT FALSE,
    verified_at TIMESTAMP,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- Таблица: novel_anomalies (Новые аномалии для исследования)
-- =============================================================================
CREATE TABLE IF NOT EXISTS novel_anomalies (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    scenario_id VARCHAR(50) NOT NULL,
    
    -- Данные WARNING
    warning_message TEXT,
    warning_log TEXT,
    warning_file VARCHAR(255),
    warning_line INTEGER,
    warning_timestamp TIMESTAMP,
    
    -- Корреляция с известной проблемой
    correlated_problem_id INTEGER,
    correlation_score FLOAT,
    time_delta_seconds FLOAT,
    
    -- Статус исследования
    status VARCHAR(50) DEFAULT 'NEW',
    reviewed_at TIMESTAMP,
    notes TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- Таблица: etl_jobs (История ETL задач)
-- =============================================================================
CREATE TABLE IF NOT EXISTS etl_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_name VARCHAR(100) NOT NULL,
    status VARCHAR(50) DEFAULT 'PENDING',
    
    -- Временные метки
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    duration_seconds FLOAT,
    
    -- Входные данные
    source_path TEXT,
    source_type VARCHAR(50),
    
    -- Результаты
    records_processed INTEGER DEFAULT 0,
    records_loaded INTEGER DEFAULT 0,
    errors_count INTEGER DEFAULT 0,
    error_message TEXT,
    
    -- Метаданные
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_etl_jobs_status ON etl_jobs(status);
CREATE INDEX IF NOT EXISTS idx_etl_jobs_created ON etl_jobs(created_at);

-- =============================================================================
-- Представления (Views) для удобного доступа к данным
-- =============================================================================

-- Сводка по инцидентам
CREATE OR REPLACE VIEW v_incidents_summary AS
SELECT 
    scenario_id,
    COUNT(*) as total_incidents,
    COUNT(DISTINCT problem_id) as unique_problems,
    COUNT(DISTINCT anomaly_id) as unique_anomalies,
    AVG(impact_score) as avg_impact_score,
    MIN(error_timestamp) as first_incident,
    MAX(error_timestamp) as last_incident
FROM incidents
GROUP BY scenario_id;

-- Последние ETL задачи
CREATE OR REPLACE VIEW v_recent_etl_jobs AS
SELECT 
    id,
    job_name,
    status,
    started_at,
    completed_at,
    duration_seconds,
    records_processed,
    records_loaded,
    errors_count
FROM etl_jobs
ORDER BY created_at DESC
LIMIT 100;

-- =============================================================================
-- Вставка тестовых данных (опционально)
-- =============================================================================

-- Тестовая запись в etl_jobs для проверки работы БД
INSERT INTO etl_jobs (job_name, status, started_at, completed_at, records_processed)
VALUES ('init_test', 'COMPLETED', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0)
ON CONFLICT DO NOTHING;

-- =============================================================================
-- Комментарии к таблицам
-- =============================================================================

COMMENT ON TABLE raw_logs IS 'Сырые логи (RAW Layer) - первичные данные без обработки';
COMMENT ON TABLE processed_logs IS 'Обработанные логи (Staging) - после парсинга и генерализации';
COMMENT ON TABLE analysis_results IS 'Результаты анализа (DWH) - агрегированные данные';
COMMENT ON TABLE incidents IS 'Таблица инцидентов для отчётов submit_report';
COMMENT ON TABLE predictive_alerts IS 'Предсказательные алерты на основе ML';
COMMENT ON TABLE novel_anomalies IS 'Новые аномалии для исследования (песочница)';
COMMENT ON TABLE etl_jobs IS 'История выполнения ETL задач';

-- Вывод сообщения об успешной инициализации
DO $$
BEGIN
    RAISE NOTICE '✅ База данных log_analytics успешно инициализирована!';
    RAISE NOTICE '📊 Созданы таблицы: raw_logs, processed_logs, analysis_results, incidents, predictive_alerts, novel_anomalies, etl_jobs';
END $$;

