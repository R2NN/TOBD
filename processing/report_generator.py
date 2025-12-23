"""
=============================================================================
processing/report_generator.py - Модуль генерации отчетов и алертов
=============================================================================

Этот модуль отвечает за создание различных типов отчетов на основе
результатов ML-анализа логов:
- Детальные отчеты по инцидентам с метриками влияния
- Предсказательные алерты о потенциальных проблемах
- Отчеты о новых (неизвестных) аномалиях

Автор: Команда Atomichack 3.0
=============================================================================
"""

import os
import re
from datetime import timedelta
import pandas as pd

from config import NOVEL_ANOMALY_WINDOW_MINUTES


# =============================================================================
# ФУНКЦИИ РАСЧЕТА МЕТРИК
# =============================================================================

def calculate_impact_metrics(classified_logs: pd.DataFrame) -> pd.DataFrame:
    """
    Вычисляет метрики влияния для каждой найденной проблемы.
    
    Функция рассчитывает Impact Score - интегральную метрику, показывающую
    серьезность проблемы на основе:
    - Количества связанных аномалий (чем больше WARNING, тем серьезнее)
    - Количества затронутых систем (чем больше файлов, тем шире проблема)
    
    Impact Score = anomaly_count × unique_systems_affected
    
    Параметры:
        classified_logs (pd.DataFrame): DataFrame с классифицированными логами,
                                        содержащий колонки: Level, final_problem_id,
                                        final_anomaly_id, file_name
    
    Возвращает:
        pd.DataFrame: DataFrame с метриками, индексированный по problem_id:
            - anomaly_count (int): Количество аномалий, связанных с проблемой
            - unique_systems_affected (int): Количество уникальных файлов/систем
            - Impact_Score (int): Интегральная метрика влияния
        
        Отсортирован по убыванию Impact_Score.
        Возвращает пустой DataFrame, если нет подходящих данных.
    
    Пример:
        Если проблема ID=1 имеет 10 аномалий в 3 разных файлах:
        Impact_Score = 10 × 3 = 30
    """
    # Фильтруем только WARNING с привязанными проблемами
    reportable_warnings = classified_logs[
        (classified_logs['Level'] == 'WARNING') & 
        (classified_logs['final_problem_id'] != 0)
    ].copy()
    
    # Если нет подходящих записей, возвращаем пустой DataFrame
    if reportable_warnings.empty:
        return pd.DataFrame()
    
    # Удаляем дубликаты аномалий перед расчетом метрик
    # Дубликат = та же аномалия для той же проблемы
    reportable_warnings = reportable_warnings.drop_duplicates(
        subset=['final_anomaly_id', 'final_problem_id'], 
        keep='first'
    )
    
    # Группируем по ID проблемы и вычисляем метрики
    impact_data = reportable_warnings.groupby('final_problem_id').agg(
        anomaly_count=('final_anomaly_id', 'count'),  # Количество аномалий
        unique_systems_affected=('file_name', 'nunique')  # Количество уникальных файлов
    )
    
    # Вычисляем интегральную метрику влияния
    impact_data['Impact_Score'] = (
        impact_data['anomaly_count'] * 
        impact_data['unique_systems_affected']
    )
    
    # Сортируем по убыванию важности
    return impact_data.sort_values(by='Impact_Score', ascending=False)


# =============================================================================
# ФУНКЦИИ ГЕНЕРАЦИИ ОТЧЕТОВ
# =============================================================================

def generate_detailed_incident_report(case_name: str, 
                                     case_dir: str, 
                                     classified_logs: pd.DataFrame, 
                                     problems_kb: pd.DataFrame, 
                                     reports_dir: str) -> None:
    """
    Генерирует детальный текстовый отчет по инцидентам с метриками влияния.
    
    Функция создает форматированный отчет, ранжируя проблемы по их Impact Score.
    Для каждой проблемы указывается:
    - Ранг по важности
    - Описание проблемы из базы знаний
    - Метрики влияния (Impact Score, количество аномалий, затронутые системы)
    
    Отчет сохраняется в файл в указанной директории.
    
    Параметры:
        case_name (str): Название сценария/кейса
        case_dir (str): Директория с исходными лог-файлами
        classified_logs (pd.DataFrame): Классифицированные логи
        problems_kb (pd.DataFrame): База знаний проблем
        reports_dir (str): Директория для сохранения отчета
    
    Создает файл:
        {reports_dir}/Incident_Report_{case_name}.txt
    
    Формат отчета:
        ======= ОТЧЕТ ПО ИНЦИДЕНТАМ ДЛЯ СЦЕНАРИЯ: Case1 =======
        
        ======================================================================
        РАНГ: 1 | ИНЦИДЕНТ: Database connection timeout (ID: 34)
        IMPACT SCORE: 60 (Аномалий: 20, Систем затронуто: 3)
        ======================================================================
        ...
    
    Примечание:
        Если нет данных для отчета (пустые метрики), функция завершается без создания файла.
    """
    # Вычисляем метрики влияния
    impact_metrics = calculate_impact_metrics(classified_logs)
    
    # Если нет данных, не создаем отчет
    if impact_metrics.empty:
        return
    
    # Формируем заголовок отчета
    report = [f"======= ОТЧЕТ ПО ИНЦИДЕНТАМ ДЛЯ СЦЕНАРИЯ: {case_name} ======="]
    
    # Получаем детали ERROR-логов для каждой проблемы (первое вхождение)
    error_details = classified_logs[
        (classified_logs['Level'] == 'ERROR') & 
        (classified_logs['final_problem_id'] != 0)
    ].sort_values('Timestamp').drop_duplicates(
        subset=['final_problem_id'], 
        keep='first'  # Берем самый ранний ERROR для каждой проблемы
    )
    
    # Создаем словарь для быстрого доступа к деталям ERROR по problem_id
    error_map = error_details.set_index('final_problem_id').to_dict('index')
    
    # Формируем записи для каждой проблемы
    rank = 1
    for problem_id, metrics in impact_metrics.iterrows():
        # Получаем детали ERROR
        error_info = error_map.get(problem_id)
        if not error_info:
            continue  # Пропускаем, если нет информации об ERROR
        
        # Получаем текстовое описание проблемы из базы знаний
        problem_text_series = problems_kb[problems_kb['problem_id'] == problem_id]['Problem_Text']
        problem_text = (
            problem_text_series.iloc[0] 
            if not problem_text_series.empty 
            else "Описание не найдено"
        )
        
        # Добавляем запись в отчет
        report.append(f"\n{'=' * 70}")
        report.append(f"РАНГ: {rank} | ИНЦИДЕНТ: {problem_text} (ID: {problem_id})")
        report.append(
            f"IMPACT SCORE: {metrics['Impact_Score']} "
            f"(Аномалий: {metrics['anomaly_count']}, "
            f"Систем затронуто: {metrics['unique_systems_affected']})"
        )
        
        rank += 1
    
    # Сохраняем отчет в файл
    report_filename = os.path.join(reports_dir, f"Incident_Report_{case_name}.txt")
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report))


def generate_predictive_alerts(classified_logs: pd.DataFrame, 
                               anomalies_kb: pd.DataFrame, 
                               problems_kb: pd.DataFrame, 
                               case_name: str) -> pd.DataFrame:
    """
    Генерирует предсказательные алерты о потенциальных аномалиях.
    
    Функция анализирует найденные ERROR-проблемы и предсказывает WARNING-аномалии,
    которые обычно сопровождают эти проблемы, но еще не были зафиксированы в логах.
    
    Логика предсказания:
    1. Для каждой найденной проблемы (ERROR) находим все связанные аномалии из базы знаний
    2. Проверяем, какие из этих аномалий еще не появились в логах
    3. Создаем предсказательный алерт для каждой отсутствующей аномалии
    
    Параметры:
        classified_logs (pd.DataFrame): Классифицированные логи
        anomalies_kb (pd.DataFrame): База знаний аномалий
        problems_kb (pd.DataFrame): База знаний проблем
        case_name (str): Название сценария (для извлечения ID)
    
    Возвращает:
        pd.DataFrame: DataFrame с предсказательными алертами, содержащий колонки:
            - ID сценария
            - Тип Алерта ("ПРЕДСКАЗАНИЕ")
            - Триггерная проблема (ID)
            - Описание проблемы
            - Время триггера
            - Лог триггерной ошибки
            - Предсказанная аномалия (ID)
            - Текст предсказанного WARNING
            - Обоснование
        
        Возвращает пустой DataFrame, если нет данных для предсказаний.
    
    Применение:
        Помогает проактивно подготовиться к возможным проблемам и предотвратить их эскалацию.
    """
    # Находим все ERROR с привязанными проблемами
    detected_errors = classified_logs[
        (classified_logs['Level'] == 'ERROR') & 
        (classified_logs['final_problem_id'] != 0)
    ]
    
    # Если нет ERROR, нет данных для предсказаний
    if detected_errors.empty:
        return pd.DataFrame()
    
    # Получаем уникальные ID найденных проблем
    unique_problem_ids = detected_errors['final_problem_id'].unique()
    
    predictions = []
    
    # Извлекаем ID сценария из названия (или используем название целиком)
    scenario_id = (
        re.search(r'\d+', case_name).group() 
        if re.search(r'\d+', case_name) 
        else case_name
    )
    
    # Анализируем каждую найденную проблему
    for problem_id in unique_problem_ids:
        # Находим все аномалии, связанные с этой проблемой в базе знаний
        potential_warnings_kb = anomalies_kb[anomalies_kb['problem_id'] == problem_id]
        
        # Находим аномалии, которые уже произошли в логах для этой проблемы
        # Удаляем дубликаты, чтобы получить уникальный список аномалий
        occurred_warnings = classified_logs[
            (classified_logs['Level'] == 'WARNING') & 
            (classified_logs['final_problem_id'] == problem_id)
        ].copy()
        occurred_warnings = occurred_warnings.drop_duplicates(
            subset=['final_anomaly_id', 'final_problem_id'], 
            keep='first'
        )
        already_occurred_anomalies = occurred_warnings['final_anomaly_id'].unique()
        
        # Получаем информацию о триггерной ошибке (первое вхождение)
        trigger_error_log = detected_errors[
            detected_errors['final_problem_id'] == problem_id
        ].sort_values('Timestamp').iloc[0]
        
        # Получаем описание проблемы из базы знаний
        problem_text_series = problems_kb[problems_kb['problem_id'] == problem_id]['Problem_Text']
        problem_text = (
            problem_text_series.iloc[0] 
            if not problem_text_series.empty 
            else f"Описание не найдено для ID {problem_id}"
        )
        
        # Создаем предсказания для аномалий, которые еще не появились
        for _, anomaly_row in potential_warnings_kb.iterrows():
            if anomaly_row['anomaly_id'] not in already_occurred_anomalies:
                prediction = {
                    'ID сценария': scenario_id,
                    'Тип Алерта': 'ПРЕДСКАЗАНИЕ',
                    'Триггерная проблема (ID)': problem_id,
                    'Описание проблемы': problem_text,
                    'Время триггера': trigger_error_log['Timestamp'],
                    'Лог триггерной ошибки': trigger_error_log['log'],
                    'Предсказанная аномалия (ID)': anomaly_row['anomaly_id'],
                    'Текст предсказанного WARNING': anomaly_row.get('Anomaly_Text', 'Текст не найден'),
                    'Обоснование': (
                        f"Это предупреждение часто сопровождает проблему ID {problem_id}, "
                        f"но еще не было зафиксировано в логах после возникновения триггера."
                    )
                }
                predictions.append(prediction)
    
    return pd.DataFrame(predictions)


def identify_novel_anomalies(classified_logs: pd.DataFrame, 
                            case_name: str) -> pd.DataFrame:
    """
    Идентифицирует новые (неизвестные) аномалии, коррелирующие с известными проблемами.
    
    ФОРМАТ ВЫХОДА аналогичен submit_report.xlsx:
    - ID сценария, ID аномалии, ID проблемы
    - Файл с проблемой, № строки
    - Строка из лога (с timestamp для Time Machine!)
    
    Функция находит WARNING-логи, которые не были сопоставлены с базой знаний
    (final_problem_id == 0), но появились в течение короткого времени после
    известной ERROR-проблемы. Это может указывать на:
    - Новые симптомы существующих проблем
    - Недостаточность базы знаний
    - Эволюцию известных проблем
    
    Логика идентификации:
    1. Находим все неклассифицированные WARNING
    2. Для каждого WARNING ищем ближайшую известную ERROR в временном окне
    3. Если ERROR найдена, создаем алерт о потенциальной корреляции
    
    Параметры:
        classified_logs (pd.DataFrame): Классифицированные логи
        case_name (str): Название сценария (для извлечения ID)
    
    Возвращает:
        pd.DataFrame: DataFrame по структуре submit_report.xlsx
        
        Возвращает пустой DataFrame, если нет новых аномалий или известных ошибок.
    
    Применение:
        Помогает обновлять базу знаний и обнаруживать эволюцию проблем.
    """
    # Находим неклассифицированные WARNING
    novel_warnings = classified_logs[
        (classified_logs['Level'] == 'WARNING') & 
        (classified_logs['final_problem_id'] == 0)
    ].copy()
    
    # Если нет неклассифицированных WARNING, завершаем
    if novel_warnings.empty:
        return pd.DataFrame()
    
    # Удаляем дубликаты неклассифицированных WARNING
    # Дубликаты определяются по обобщенному сообщению и файлу
    novel_warnings = novel_warnings.drop_duplicates(
        subset=['Generalized_Message', 'file_name'], 
        keep='first'
    )
    
    # Находим известные ERROR, отсортированные по времени
    known_errors = classified_logs[
        (classified_logs['Level'] == 'ERROR') & 
        (classified_logs['final_problem_id'] != 0)
    ].sort_values('Timestamp')
    
    # Если нет известных ошибок, не с чем коррелировать
    if known_errors.empty:
        return pd.DataFrame()
    
    novel_alerts = []
    
    # Извлекаем ID сценария из названия
    scenario_id = (
        re.search(r'\d+', case_name).group() 
        if re.search(r'\d+', case_name) 
        else case_name
    )
    
    # Определяем временное окно для поиска корреляций
    time_window = timedelta(minutes=NOVEL_ANOMALY_WINDOW_MINUTES)
    
    # Анализируем каждый неклассифицированный WARNING
    for index, warning in novel_warnings.iterrows():
        # Определяем временное окно поиска (N минут до WARNING)
        time_start = warning['Timestamp'] - time_window
        time_end = warning['Timestamp']
        
        # Ищем ERROR в этом временном окне
        potential_causes = known_errors[
            (known_errors['Timestamp'] >= time_start) & 
            (known_errors['Timestamp'] <= time_end)
        ]
        
        # Если нашли ERROR в окне, создаем алерт
        if not potential_causes.empty:
            # Берем ближайшую ERROR (последнюю перед WARNING)
            closest_error = potential_causes.iloc[-1]
            
            # Вычисляем временную дельту
            delta_seconds = (warning['Timestamp'] - closest_error['Timestamp']).total_seconds()
            
            # ФОРМАТ ПО АНАЛОГИИ С submit_report.xlsx
            # Структура: ID сценария | ID аномалии | ID проблемы | Файл с проблемой | № строки | Строка из лога
            
            alert = {
                'ID сценария': scenario_id,
                'ID аномалии': 0,  # 0 означает "новая, не в базе знаний"
                'ID проблемы': closest_error['final_problem_id'],
                'Файл с проблемой': warning.get('file_name', ''),
                '№ строки': warning.get('line_number', 0),
                'Строка из лога': warning['log']  # Полный лог с timestamp!
            }
            novel_alerts.append(alert)
    
    return pd.DataFrame(novel_alerts)


