from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
from pathlib import Path
import smtplib
from email.message import EmailMessage

sys.path.append('/opt/airflow/funcs')

from daily_parsing import main as run_parser, clean_json_file, send_email

def empty_task():
    for i in range(6):
        print(i)

def run_parsing_task(**context):
    print("Запуск парсинга")
    result = run_parser()
    print("Окончание парсинга")
    return {"status": "success"}

def clean_data_task():
    print("Запуск очистки")

    input_folder = Path("/opt/airflow/tbank_knowledge")
    output_folder = Path("/opt/airflow/tbank_knowledge_clean")
    output_folder.mkdir(exist_ok=True, parents=True)

    json_files = list(input_folder.glob("*.json"))
    if not json_files:
        print(f"файлы не найдены")
        return

    print(f"Найдено файлов: {len(json_files)}")

    for i, input_file in enumerate(json_files, 1):
        try:
            output_file = output_folder / input_file.name
            clean_json_file(input_file, output_file)
            print(f"[{i}/{len(json_files)}] Очищен: {input_file.name}")
        except Exception as e:
            print(f"[{i}/{len(json_files)}] Ошибка: {e}")
def send_success_email(**context):
    send_email(
        "Airflow: Парсинг выполнен успешно",
        f"""
            DAG: daily_data_update
            Статус: УСПЕХ
            Дата: {datetime.now()}
            Задачи: парсинг и очистка выполнены успешно
        """
    )

def send_failure_email(**context):
    send_email(
        "Airflow: Ошибка при выполнении парсинга",
        f"""
            DAG: daily_data_update
            Статус: ОШИБКА
            Дата: {datetime.now()}
            Проверьте логи Airflow для деталей
        """
    )


default_args = {
    'owner': 'clprm',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'daily_data_update',
        default_args=default_args,
        description='Ежедневный парсинг',
        schedule='0 0 * * *',
        start_date=datetime(2026, 6, 9),
        catchup=False,
        max_active_runs=1,
        tags=['tbank'],
) as dag:
    parsing_task = PythonOperator(
        task_id='run_tbank_parser',
        # python_callable=run_parsing_task,
        python_callable=empty_task,
        execution_timeout=timedelta(hours=2),
    )

    cleaning_task = PythonOperator(
        task_id='clean_data',
        # python_callable=clean_data_task,
        python_callable=empty_task,
        execution_timeout=timedelta(hours=1),
    )

    success_email = PythonOperator(
        task_id='send_success_email',
        python_callable=send_success_email,
        trigger_rule='all_success',
    )

    failure_email = PythonOperator(
        task_id='send_failure_email',
        python_callable=send_failure_email,
        trigger_rule='one_failed',
    )

    parsing_task >> cleaning_task
    cleaning_task >> [success_email, failure_email]