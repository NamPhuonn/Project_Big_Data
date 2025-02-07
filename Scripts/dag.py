from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import subprocess

def run_combine_csv():
    try:
        subprocess.run(["python", "/home/phuonn/XLDLTT/HW9,10/combine_csv.py"], check=True)
        print("CSV combine script executed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error during CSV combining: {e}")

dag = DAG(
    'get_data_from_hadoop_to_powerbi',
    schedule_interval='@daily',  # Run daily
    start_date=days_ago(0),  # Start today
    catchup=False,
)

# Task 1: Run the Python script combine_csv.py
get_data_from_hadoop = PythonOperator(
    task_id='run_combine_csv',
    python_callable=run_combine_csv,
    dag=dag,
)

# Task 2: Download CSV from HDFS using hadoop fs command via BashOperator
download_csv_from_hdfs = BashOperator(
    task_id='download_csv_from_hdfs',
    bash_command='rm -f /home/phuonn/XLDLTT/HW9,10/credit_card_tmp.csv && hadoop fs -get /merged_credit_card_output/*.csv /home/phuonn/XLDLTT/HW9,10/credit_card_tmp.csv',
    dag=dag,
)

# Task để sao chép nội dung file credit_card_tmp.csv sang credit_card.csv
copy_tmp_to_final = BashOperator(
    task_id='copy_tmp_to_final',
    bash_command='cp /home/phuonn/XLDLTT/HW9,10/credit_card_tmp.csv /home/phuonn/XLDLTT/HW9,10/credit_card.csv',
    dag=dag,
)

get_data_from_hadoop >> download_csv_from_hdfs >> copy_tmp_to_final
