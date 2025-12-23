"""
=============================================================================
processing/orchestrator.py - Главный модуль-оркестратор анализа логов
=============================================================================

Этот модуль координирует выполнение всего пайплайна анализа логов:
1. Распаковывает ZIP-архив с логами
2. Загружает базу знаний и ML-модель
3. Парсит и обрабатывает лог-файлы
4. Выполняет ML-классификацию
5. Генерирует все типы отчетов
6. Упаковывает результаты в ZIP-архив

Автор: Команда Atomichack 3.0
=============================================================================
"""

import os
import re
import io
import time
import zipfile
import tempfile
import traceback
import pandas as pd
from typing import Optional, Callable
from sentence_transformers import SentenceTransformer

from config import (
    KB_FILENAME, 
    KB_BASE_FILENAME,
    KB_SUPPORTED_EXTENSIONS,
    LIGHT_MODEL, 
    HEAVY_MODEL
)
from .knowledge_base import load_knowledge_base
from .log_parser import process_all_logs_for_case
from .ml_analysis import run_analysis_pipeline, get_device
from .report_generator import (
    generate_detailed_incident_report,
    generate_predictive_alerts,
    identify_novel_anomalies
)
from .playbooks import generate_playbook_recommendations

# Импортируем модуль метрик для мониторинга
try:
    import metrics
    METRICS_ENABLED = True
except ImportError:
    METRICS_ENABLED = False
    print(">>> Модуль metrics недоступен. Метрики отключены.")


# =============================================================================
# ОСНОВНАЯ ФУНКЦИЯ АНАЛИЗА
# =============================================================================

def run_full_analysis_from_zip_bytes(zip_content_bytes: bytes, 
                                     zip_filename: str, 
                                     model_choice: str = 'light',
                                     progress_callback: Optional[Callable[[str, int, str], None]] = None) -> dict[str, str | bytes]:
    """
    Выполняет полный цикл анализа логов из ZIP-архива, находящегося в памяти.
    
    Функция является главным оркестратором всего процесса анализа.
    Она координирует работу всех модулей системы для получения финального результата.
    
    Этапы выполнения:
    
    1. Распаковка архива:
       - Создает временную директорию
       - Извлекает содержимое ZIP-архива
       - Рекурсивно ищет файл базы знаний (anomalies_problems.csv/.xlsx/.xls)
    
    2. Загрузка ML-модели:
       - Автоматически определяет доступность GPU (CUDA)
       - Выбирает модель на основе параметра model_choice
       - Загружает SentenceTransformer модель на оптимальное устройство
    
    3. Подготовка данных:
       - Загружает базу знаний аномалий и проблем
       - Предвычисляет эмбеддинги для базы знаний
       - Парсит все .txt лог-файлы
    
    4. ML-анализ:
       - Классифицирует ERROR-логи как проблемы
       - Классифицирует WARNING-логи как аномалии
       - Связывает аномалии с проблемами
    
    5. Генерация отчетов:
       - submit_report.csv - основной отчет с результатами
       - predictive_alerts.csv - предсказательные алерты
       - novel_anomalies.csv - новые обнаруженные аномалии
       - playbooks_recommendations.csv/txt - рекомендации по устранению
    
    Параметры:
        zip_content_bytes (bytes): Содержимое ZIP-архива в виде байтов
        zip_filename (str): Имя архива (используется для именования)
        model_choice (str): Выбор модели - 'light' или 'heavy'
                           'light' - быстрая модель all-MiniLM-L6-v2
                           'heavy' - точная модель Qwen3-Embedding-0.6B
    
    Возвращает:
        dict[str, str | bytes]: Словарь, где ключи - имена файлов результатов,
                               значения - содержимое (строки для TXT/CSV, байты для XLSX)
        
        Примеры возвращаемых ключей:
            - "submit_report.xlsx" - всегда присутствует при успехе (Excel)
            - "predictive_alerts.xlsx" - если есть предсказания (Excel)
            - "novel_anomalies.xlsx" - если найдены новые аномалии (Excel)
            - "playbooks_recommendations.csv" - если есть рекомендации (CSV)
            - "playbooks_recommendations.txt" - текстовая версия рекомендаций (TXT)
        
        В случае ошибки:
            {"error": "Описание ошибки"}
        
        Если нет данных для отчета:
            {"message": "Информационное сообщение"}
    
    Обработка ошибок:
        - Распаковка архива: {"error": "Не удалось распаковать..."}
        - Отсутствие базы знаний: {"error": "Файл базы знаний не найден..."}
        - Ошибки ML-анализа: {"error": "Произошла критическая ошибка..."}
        - Все ошибки логируются в консоль с полным traceback
    
    Временная директория:
        Автоматически очищается после завершения работы функции
        (используется контекстный менеджер tempfile.TemporaryDirectory)
    """
    # Создаем временную директорию для работы
    with tempfile.TemporaryDirectory() as temp_dir:
        # =====================================================================
        # ЭТАП 1: РАСПАКОВКА АРХИВА (0-10%)
        # =====================================================================
        if progress_callback:
            progress_callback("Распаковка архива", 5, "Извлечение файлов из ZIP-архива...")
        
        try:
            with zipfile.ZipFile(io.BytesIO(zip_content_bytes)) as z:
                z.extractall(temp_dir)
        except Exception as e:
            return {"error": f"Не удалось распаковать ZIP-архив: {e}"}
        
        if progress_callback:
            progress_callback("Распаковка архива", 10, "Архив успешно распакован")

        # =====================================================================
        # ЭТАП 2: ПОИСК ФАЙЛА БАЗЫ ЗНАНИЙ (10-15%)
        # =====================================================================
        if progress_callback:
            progress_callback("Поиск базы знаний", 12, f"Поиск файла базы знаний '{KB_BASE_FILENAME}' в архиве...")
        
        # Выполняем рекурсивный поиск файла базы знаний в распакованной структуре
        kb_path = None
        case_dir = None
        found_format = None
        
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                # Получаем имя файла без расширения и само расширение
                file_name_without_ext = os.path.splitext(file)[0]
                file_extension = os.path.splitext(file)[1].lower()
                
                # Проверяем, совпадает ли базовое имя и есть ли расширение в списке поддерживаемых
                if (file_name_without_ext.lower() == KB_BASE_FILENAME.lower() and 
                    file_extension in KB_SUPPORTED_EXTENSIONS):
                    kb_path = os.path.join(root, file)
                    case_dir = root  # Директория, где найдена база знаний
                    found_format = file_extension
                    break
            if kb_path:
                break
        
        # Проверяем, что файл базы знаний найден
        if not kb_path or not os.path.exists(kb_path):
            supported_formats = ', '.join(KB_SUPPORTED_EXTENSIONS)
            return {"error": f"Файл базы знаний '{KB_BASE_FILENAME}' не найден в архиве! Поддерживаемые форматы: {supported_formats}"}
        
        if progress_callback:
            progress_callback("Поиск базы знаний", 15, f"База знаний найдена ({found_format} формат)")

        # Извлекаем имя сценария из имени ZIP-файла
        case_name = os.path.splitext(zip_filename)[0]
        
        # Создаем директорию для временных отчетов
        reports_dir = os.path.join(temp_dir, "reports")
        os.makedirs(reports_dir, exist_ok=True)
        
        # Словарь для хранения всех сгенерированных отчетов
        final_reports = {}

        try:
            # =================================================================
            # ЭТАП 3: ОПРЕДЕЛЕНИЕ УСТРОЙСТВА И ЗАГРУЗКА ML-МОДЕЛИ (15-30%)
            # =================================================================
            if progress_callback:
                progress_callback("Загрузка ML модели", 18, "Определение доступных вычислительных ресурсов...")
            
            # Определяем оптимальное устройство (GPU или CPU)
            device = get_device()
            
            # Выбираем модель в зависимости от параметра
            model_name = LIGHT_MODEL if model_choice == 'light' else HEAVY_MODEL
            model_display_name = "Легкая (быстрая)" if model_choice == 'light' else "Тяжелая (точная)"
            
            if progress_callback:
                progress_callback("Загрузка ML модели", 22, f"Загрузка модели {model_display_name}...")
            
            print(f">>> [ЭТАП 3] Загрузка модели [{model_display_name}]: '{model_name}'...")
            
            # Измеряем время загрузки модели
            model_load_start = time.time()
            model = SentenceTransformer(model_name, device=device)
            model_load_duration = time.time() - model_load_start
            
            # Записываем метрику загрузки модели
            if METRICS_ENABLED:
                metrics.record_model_loading(model_choice, model_load_duration)
            
            print(f">>> [ЭТАП 3] Модель успешно загружена за {model_load_duration:.2f} секунд!")
            
            if progress_callback:
                progress_callback("Загрузка ML модели", 30, f"Модель {model_name} загружена на {device}")
            
            # =================================================================
            # ЭТАП 4: ЗАГРУЗКА БАЗЫ ЗНАНИЙ (30-40%)
            # =================================================================
            if progress_callback:
                progress_callback("Загрузка базы знаний", 32, "Чтение файла базы знаний...")
            
            print(f">>> [ЭТАП 4] Загрузка базы знаний...")
            anomalies_kb, problems_kb = load_knowledge_base(kb_path)
            print(f">>> [ЭТАП 4] База знаний загружена. Аномалий: {len(anomalies_kb)}, Проблем: {len(problems_kb)}")
            
            if progress_callback:
                progress_callback("Загрузка базы знаний", 35, f"Загружено: {len(anomalies_kb)} аномалий, {len(problems_kb)} проблем")
            
            # =================================================================
            # ЭТАП 5: ГЕНЕРАЦИЯ ЭМБЕДДИНГОВ (40-50%)
            # =================================================================
            # Предвычисляем эмбеддинги для базы знаний
            # (это ускоряет последующие сопоставления)
            if progress_callback:
                progress_callback("Генерация эмбеддингов", 42, "Создание векторных представлений аномалий...")
            
            print(f">>> [ЭТАП 5] Генерация эмбеддингов базы знаний...")
            
            # Измеряем время генерации эмбеддингов аномалий
            anomaly_embed_start = time.time()
            anomalies_kb_embeddings = model.encode(
                anomalies_kb['Generalized_Anomaly'].tolist(), 
                convert_to_tensor=True, 
                device=device
            )
            anomaly_embed_duration = time.time() - anomaly_embed_start
            
            # Записываем метрику
            if METRICS_ENABLED:
                metrics.record_ml_inference(model_choice, 'anomaly_embedding_generation', anomaly_embed_duration)
            
            if progress_callback:
                progress_callback("Генерация эмбеддингов", 46, "Создание векторных представлений проблем...")
            
            # Измеряем время генерации эмбеддингов проблем
            problem_embed_start = time.time()
            problems_kb_embeddings = model.encode(
                problems_kb['Generalized_Problem'].tolist(), 
                convert_to_tensor=True, 
                device=device
            )
            problem_embed_duration = time.time() - problem_embed_start
            
            # Записываем метрику
            if METRICS_ENABLED:
                metrics.record_ml_inference(model_choice, 'problem_embedding_generation', problem_embed_duration)
            
            print(f">>> [ЭТАП 5] Эмбеддинги сгенерированы!")
            
            if progress_callback:
                progress_callback("Генерация эмбеддингов", 50, "Эмбеддинги базы знаний готовы")
            
            # =================================================================
            # ЭТАП 6: ПАРСИНГ ЛОГ-ФАЙЛОВ (50-60%)
            # =================================================================
            if progress_callback:
                progress_callback("Парсинг логов", 52, "Чтение и анализ лог-файлов...")
            
            print(f">>> [ЭТАП 6] Обработка лог-файлов...")
            logs_df = process_all_logs_for_case(case_dir)

            # Проверяем, есть ли данные для анализа
            if logs_df.empty:
                print(f">>> [ЭТАП 6] В логах не найдено WARNING или ERROR")
                return {"message": "В лог-файлах не найдено записей уровня WARNING или ERROR."}
            
            print(f">>> [ЭТАП 6] Обработано {len(logs_df)} записей")
            
            if progress_callback:
                progress_callback("Парсинг логов", 60, f"Обработано {len(logs_df)} записей WARNING/ERROR")

            # =================================================================
            # ЭТАП 7: ML-КЛАССИФИКАЦИЯ (60-75%)
            # =================================================================
            if progress_callback:
                progress_callback("ML-классификация", 62, "Классификация ERROR логов как проблем...")
            
            print(f">>> [ЭТАП 7] Запуск ML-анализа...")
            
            # Измеряем время ML-классификации
            classification_start = time.time()
            classified_logs = run_analysis_pipeline(
                logs_df, 
                anomalies_kb, 
                problems_kb, 
                model, 
                case_name,
                anomalies_kb_embeddings, 
                problems_kb_embeddings,
                device
            )
            classification_duration = time.time() - classification_start
            
            # Записываем метрику ML инференса
            if METRICS_ENABLED:
                metrics.record_ml_inference(model_choice, 'full_classification', classification_duration)
            
            print(f">>> [ЭТАП 7] ML-анализ завершен за {classification_duration:.2f} секунд!")
            
            if progress_callback:
                progress_callback("ML-классификация", 75, "Классификация завершена, связи аномалий и проблем установлены")
            
            # =================================================================
            # ЭТАП 8: ГЕНЕРАЦИЯ ДЕТАЛЬНОГО ОТЧЕТА (75-80%)
            # =================================================================
            if progress_callback:
                progress_callback("Генерация отчетов", 76, "Создание детального отчета по инцидентам...")
            
            print(f">>> [ЭТАП 8] Генерация детального отчета по инцидентам...")
            generate_detailed_incident_report(
                case_name, 
                case_dir, 
                classified_logs, 
                problems_kb, 
                reports_dir
            )
            
            if progress_callback:
                progress_callback("Генерация отчетов", 80, "Детальный отчет создан")

            # =================================================================
            # ЭТАП 9: ГЕНЕРАЦИЯ ДОПОЛНИТЕЛЬНЫХ ОТЧЕТОВ (80-85%)
            # =================================================================
            if progress_callback:
                progress_callback("Генерация отчетов", 81, "Создание предсказательных алертов...")
            
            predictive_df = generate_predictive_alerts(
                classified_logs, 
                anomalies_kb, 
                problems_kb, 
                case_name
            )
            
            if progress_callback:
                progress_callback("Генерация отчетов", 83, "Поиск новых аномалий...")
            
            novel_df = identify_novel_anomalies(classified_logs, case_name)
            
            if progress_callback:
                progress_callback("Генерация отчетов", 85, "Дополнительные отчеты готовы")

            # =================================================================
            # ЭТАП 10: ФОРМИРОВАНИЕ ОСНОВНОГО ОТЧЕТА (85-92%)
            # =================================================================
            if progress_callback:
                progress_callback("Формирование итогового отчета", 87, "Фильтрация и обработка аномалий...")
            
            # Фильтруем WARNING с привязанными проблемами
            reportable_warnings = classified_logs[
                (classified_logs['Level'] == 'WARNING') & 
                (classified_logs['final_problem_id'] != 0)
            ].copy()
            
            if not reportable_warnings.empty:
                # =============================================================
                # УДАЛЕНИЕ ДУБЛИКАТОВ АНОМАЛИЙ
                # =============================================================
                # Дубликатом считается аномалия с тем же ID и той же проблемой-причиной.
                # Оставляем только первое по времени вхождение для корректного анализа.
                initial_count = len(reportable_warnings)
                reportable_warnings = reportable_warnings.drop_duplicates(
                    subset=['final_anomaly_id', 'final_problem_id'], 
                    keep='first'
                )
                final_count = len(reportable_warnings)
                
                if initial_count > final_count:
                    print(f">>> [ЭТАП 9] Удалено {initial_count - final_count} дублирующихся аномалий")
                print(f">>> [ЭТАП 9] Уникальных аномалий для отчета: {final_count}")
                
                # Записываем метрики обнаруженных аномалий
                if METRICS_ENABLED:
                    metrics.record_anomalies_detected(model_choice, final_count, 'medium')
                
                # =============================================================
                # Находим первые ERROR для каждой проблемы
                error_details = classified_logs[
                    (classified_logs['Level'] == 'ERROR') & 
                    (classified_logs['final_problem_id'] != 0)
                ].sort_values('Timestamp').drop_duplicates(
                    subset=['final_problem_id'], 
                    keep='first'
                )
                
                # Создаем mapping ERROR информации по problem_id
                error_map = error_details.set_index('final_problem_id')[
                    ['file_name', 'line_number', 'log']
                ].to_dict('index')
                
                # Добавляем информацию об ERROR к каждому WARNING
                reportable_warnings['error_file_name'] = reportable_warnings['final_problem_id'].map(
                    lambda pid: error_map.get(pid, {}).get('file_name')
                )
                reportable_warnings['error_line_number'] = reportable_warnings['final_problem_id'].map(
                    lambda pid: error_map.get(pid, {}).get('line_number')
                )
                reportable_warnings['error_log'] = reportable_warnings['final_problem_id'].map(
                    lambda pid: error_map.get(pid, {}).get('log')
                )
                
                # Удаляем записи без ERROR информации
                reportable_warnings.dropna(subset=['error_log'], inplace=True)
                
                # Извлекаем ID сценария из имени
                scenario_id_match = re.search(r'\d+', case_name)
                reportable_warnings['scenario_id'] = (
                    scenario_id_match.group() if scenario_id_match else case_name
                )
                
                # Записываем метрики классифицированных проблем
                if METRICS_ENABLED:
                    unique_problems = reportable_warnings['final_problem_id'].nunique()
                    metrics.record_problems_classified(model_choice, unique_problems, 'generic')

                # =================================================================
                # ФОРМИРОВАНИЕ ОТЧЕТА
                # =================================================================
                
                # Формируем финальный DataFrame для отчета
                output_df = pd.DataFrame({
                    'ID сценария': reportable_warnings['scenario_id'],
                    'ID аномалии': reportable_warnings['final_anomaly_id'],
                    'ID проблемы': reportable_warnings['final_problem_id'],
                    'Файл с проблемой': reportable_warnings['error_file_name'],
                    '№ строки проблемы': reportable_warnings['error_line_number'],
                    'Строка лога проблемы': reportable_warnings['error_log'],
                    # Добавляем колонку с WARNING логом (берем из самого WARNING)
                    'Строка лога аномалии': reportable_warnings['log'].values
                })
                
                # Сортируем по времени WARNING (для корректного отображения)
                try:
                    def extract_timestamp_for_sort(log_str):
                        import re
                        if pd.isna(log_str):
                            return pd.NaT
                        match = re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})', str(log_str))
                        if match:
                            return pd.to_datetime(match.group(1))
                        return pd.NaT
                    
                    output_df['_timestamp_temp'] = output_df['Строка лога аномалии'].apply(extract_timestamp_for_sort)
                    output_df = output_df.sort_values('_timestamp_temp', na_position='last').drop(columns=['_timestamp_temp'])
                except Exception as e:
                    print(f">>> [ПРЕДУПРЕЖДЕНИЕ] Не удалось отсортировать по timestamp: {e}")
                
                # Приводим числовые колонки к правильным типам
                output_df = output_df.astype({
                    'ID аномалии': int, 
                    'ID проблемы': int, 
                    '№ строки проблемы': int
                })
                
                print(f">>> [ОТЧЕТ] Создан submit_report.xlsx: {len(output_df)} WARNING (аномалий) с привязкой к ERROR")
                print(f">>> [ОТЧЕТ] Уникальных проблем: {output_df['ID проблемы'].nunique()}")
                print(f">>> [ОТЧЕТ] Уникальных аномалий: {output_df['ID аномалии'].nunique()}")
                
                # Сохраняем в словарь результатов в формате Excel
                excel_buffer = io.BytesIO()
                output_df.to_excel(excel_buffer, index=False, engine='openpyxl')
                final_reports['submit_report.xlsx'] = excel_buffer.getvalue()
                
                if progress_callback:
                    progress_callback("Формирование итогового отчета", 90, "Основной отчет submit_report.xlsx создан")

            # =================================================================
            # ДОБАВЛЕНИЕ ДОПОЛНИТЕЛЬНЫХ ОТЧЕТОВ (90-92%)
            # =================================================================
            # Добавляем отчет с предсказаниями (если есть) в формате Excel
            if not predictive_df.empty:
                excel_buffer = io.BytesIO()
                predictive_df.to_excel(excel_buffer, index=False, engine='openpyxl')
                final_reports['predictive_alerts.xlsx'] = excel_buffer.getvalue()
            
            # Добавляем отчет с новыми аномалиями (если есть) в формате Excel
            if not novel_df.empty:
                excel_buffer = io.BytesIO()
                novel_df.to_excel(excel_buffer, index=False, engine='openpyxl')
                final_reports['novel_anomalies.xlsx'] = excel_buffer.getvalue()
            
            # =================================================================
            # ЭТАП 11: ГЕНЕРАЦИЯ РЕКОМЕНДАЦИЙ (92-100%)
            # =================================================================
            if progress_callback:
                progress_callback("Генерация рекомендаций", 93, "Создание playbook для устранения проблем...")
            
            playbook_csv, playbook_text = generate_playbook_recommendations(
                classified_logs, 
                problems_kb
            )
            
            # Добавляем рекомендации в CSV формате (если есть)
            if playbook_csv:
                final_reports['playbooks_recommendations.csv'] = playbook_csv
            
            # Добавляем рекомендации в текстовом формате (если есть)
            if playbook_text:
                final_reports['playbooks_recommendations.txt'] = playbook_text
            
            if progress_callback:
                progress_callback("Завершение", 100, "Анализ полностью завершен! Все отчеты готовы.")

        except Exception as e:
            # Логируем полный traceback для отладки
            traceback.print_exc()
            return {"error": f"Произошла критическая ошибка во время анализа: {e}"}
            
        # Возвращаем все сгенерированные отчеты
        return final_reports


# =============================================================================
# ФУНКЦИИ ИНТЕГРАЦИИ С FASTAPI
# =============================================================================

def process_single_txt(file_content: bytes) -> tuple[bool, str]:
    """
    Обработка одиночного TXT файла (не поддерживается в текущей версии).
    
    Функция-заглушка для обработки попыток загрузить одиночный TXT-файл
    вместо ZIP-архива. Возвращает понятное сообщение об ошибке.
    
    Параметры:
        file_content (bytes): Содержимое TXT-файла (игнорируется)
    
    Возвращает:
        tuple[bool, str]: Кортеж (False, сообщение_об_ошибке)
    
    Примечание:
        В будущих версиях может быть реализована поддержка одиночных файлов
        для быстрого анализа без необходимости создавать ZIP-архив.
    """
    error_message = (
        "Ошибка: Для анализа необходим ZIP-архив, содержащий лог-файлы (*.txt) "
        "и файл базы знаний 'anomalies_problems' (в формате .csv, .xlsx или .xls)."
    )
    return False, error_message


def process_zip_archive(file_content: bytes, 
                        filename: str, 
                        model_choice: str = 'light',
                        progress_callback: Optional[Callable[[str, int, str], None]] = None) -> tuple[bool, bytes, dict]:
    """
    Обрабатывает ZIP-архив и возвращает результаты в виде нового ZIP-архива.
    
    Функция является точкой входа для FastAPI. Она:
    1. Вызывает основную функцию анализа
    2. Упаковывает все результаты в новый ZIP-архив
    3. Возвращает архив в виде байтов для отправки клиенту
    
    Параметры:
        file_content (bytes): Содержимое загруженного ZIP-архива
        filename (str): Имя загруженного файла
        model_choice (str): Выбор модели - 'light' или 'heavy'
    
    Возвращает:
        tuple[bool, bytes, dict]: Кортеж из трех элементов:
            - success (bool): True если обработка успешна, False при ошибке
            - result_bytes (bytes): ZIP-архив с результатами или сообщение об ошибке
            - metadata (dict): Метаданные о результатах:
                {'files_count': int, 'file_names': list[str]}
    
    Примеры возврата:
        Успех:
            (True, <zip_bytes>, {'files_count': 4, 'file_names': [...]})
        
        Ошибка:
            (False, b"Error message", {})
        
        Нет данных:
            (False, b"No incidents found", {})
    
    Логирование:
        Все этапы работы логируются в консоль для отладки:
        - Начало операции с указанием модели
        - Количество сгенерированных файлов
        - Сообщения об ошибках с traceback
    """
    # Определяем текст для логирования
    model_text = "ЛЕГКОЙ моделью" if model_choice == 'light' else "ТЯЖЕЛОЙ моделью"
    print(f"--- НАЧАЛО ОПЕРАЦИИ (ZIP-АРХИВ В ПАМЯТИ, {model_text}): {filename} ---")
    
    try:
        # Запускаем полный анализ с передачей callback
        results = run_full_analysis_from_zip_bytes(file_content, filename, model_choice, progress_callback)

        # Проверяем на ошибки
        if "error" in results:
            print(f"!!! Ошибка при обработке: {results['error']}")
            return False, results['error'].encode('utf-8'), {}

        # Проверяем наличие основного отчета
        if "submit_report.xlsx" in results:
            # Создаем ZIP-архив со всеми результатами в памяти
            zip_buffer = io.BytesIO()
            
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                # Добавляем каждый файл результата в архив
                for file_name, file_content_data in results.items():
                    # Для текстовых файлов (CSV, TXT) оставляем строки, для остального - байты
                    if isinstance(file_content_data, str):
                        zip_file.writestr(file_name, file_content_data)
                    else:
                        zip_file.writestr(file_name, file_content_data)
            
            # Получаем байты ZIP-архива
            zip_bytes = zip_buffer.getvalue()
            
            print(f"--- ОПЕРАЦИЯ ЗАВЕРШЕНА УСПЕШНО ({len(results)} файлов) ---")
            
            # Формируем метаданные
            metadata = {
                'files_count': len(results),
                'file_names': list(results.keys())
            }
            
            return True, zip_bytes, metadata
        
        else:
            # Анализ выполнен, но нет данных для отчета
            message = "Анализ завершен. Инцидентов для включения в итоговый отчет не найдено."
            print(message)
            return False, message.encode('utf-8'), {}

    except Exception as e:
        # Обработка непредвиденных ошибок
        error_message = f"Произошла непредвиденная критическая ошибка: {e}"
        print(f"!!! {error_message}")
        traceback.print_exc()
        return False, "Произошла непредвиденная ошибка при обработке архива.".encode('utf-8'), {}

