from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'batman',
    'start_date': days_ago(0),
    'email': ['batman@gotham.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task 1: Unzip data
    unzip_data = BashOperator(
        task_id='unzip_data',
        bash_command='tar -xvzf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment/',
    )

    # Task 2: Extract data from CSV
    extract_data_from_csv = BashOperator(
        task_id='extract_data_from_csv',
        bash_command=(
            'cut -d"," -f1,2,3,4 /home/project/airflow/dags/finalassignment/vehicle-data.csv '
            '> /home/project/airflow/dags/finalassignment/csv_data.csv'
        ),
    )

    # Task 3: Extract data from TSV
    extract_data_from_tsv = BashOperator(
        task_id='extract_data_from_tsv',
        bash_command=(
            'cut -f5,6,7 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv '
            '> /home/project/airflow/dags/finalassignment/tsv_data.csv'
        ),
    )

    # Task 4: Extract data from fixed-width file
    extract_data_from_fixed_width = BashOperator(
        task_id='extract_data_from_fixed_width',
        bash_command=(
            'cut -c44-66,67-89 /home/project/airflow/dags/finalassignment/payment-data.txt '
            '> /home/project/airflow/dags/finalassignment/fixed_width_data.csv'
        ),
    )

    # Task 5: Consolidate data
    consolidate_data = BashOperator(
        task_id='consolidate_data',
        bash_command=(
            'paste -d"," /home/project/airflow/dags/finalassignment/csv_data.csv '
            '/home/project/airflow/dags/finalassignment/tsv_data.csv '
            '/home/project/airflow/dags/finalassignment/fixed_width_data.csv '
            '> /home/project/airflow/dags/finalassignment/extracted_data.csv'
        ),
    )

    # Task 6: Transform data
    transform_data = BashOperator(
        task_id='transform_data',
        bash_command=(
            'cat /home/project/airflow/dags/finalassignment/extracted_data.csv | '
            'awk -F"," \'{OFS=","; $4=toupper($4); print}\' '
            '> /home/project/airflow/dags/finalassignment/transformed_data.csv'
        ),
    )

    # Task pipeline
    unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width \
    >> consolidate_data >> transform_data
