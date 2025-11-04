from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import psycopg2
import csv
import time
import random
import concurrent.futures

DB_CONFIG ={
    'host': 'host.docker.internal',
    'port': 5432,
    'user': 'postgres',
    'password': "MyPostgres",
    'dbname': 'cats'
}

column_mapping = {
    'agent_type_id': 'id',
    'locality_type_id': 'locality_level',

}

ignore_columns = []

boolean_columns = ['stock_holding_capability', 'is_route_owner', 'is_route_agent', 'assign_items']

def parse_boolean(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value

    value_str = str(value).strip().lower()
    if value_str in ['true', 't', 'yes', '1', 'y']:
        return True
    elif value_str in ['false', 'f', 'no', '0', 'n']:
        return False
    elif value_str == '':
        return None
    else:
        return None

def extract_csv(**context):
    log = logging.getLogger("airflow.task")
    try:
        with open('/opt/airflow/data/agent-type.csv', 'r') as file:
            reader = csv.DictReader(file)
            rows = list(reader)
            original_header = reader.fieldnames
        context['ti'].xcom_push(key='raw_rows', value=rows)
        context['ti'].xcom_push(key='original_header', value=original_header)
        log.info("CSV extraction completed successfully.")
    except Exception as e:
        log.error(f"Error in extract_csv: {e}")
        raise

def transform(**context):
    log = logging.getLogger("airflow.task")
    try:
        rows = context['ti'].xcom_pull(key='raw_rows')
        original_header = context['ti'].xcom_pull(key='original_header')

        normalized_headers = []
        included_original_headers = []
        
        for col in original_header:
            lower_col = col.strip().lower()
            if lower_col in ignore_columns:
                continue
            mapped_col = column_mapping.get(lower_col, lower_col)
            normalized_headers.append(mapped_col)
            included_original_headers.append(col)

        transformed_data = []
        for row in rows:
            values = []
            for i, col in enumerate(included_original_headers):
                value = row[col]
                if value == '':
                    value = None
                else:
                    mapped_col = normalized_headers[i]
                    if mapped_col in boolean_columns:
                        value = parse_boolean(value)
                values.append(value)
            transformed_data.append(values)

        context['ti'].xcom_push(key='headers', value=normalized_headers)
        context['ti'].xcom_push(key='data', value=transformed_data)
    except Exception as e:
        log.error(f"Error in transform: {e}")
        raise

def load_to_postgres(**context):
    log = logging.getLogger("airflow.task")
    try:
        headers = context['ti'].xcom_pull(key='headers', task_ids='transform_task')
        data = context['ti'].xcom_pull(key='data', task_ids='transform_task')

        quoted_headers = [f'"{col}"' for col in headers]
        columns_str = ', '.join(quoted_headers)
        placeholders_str = ', '.join(['%s'] * len(headers))
        insert_query = f'''
            INSERT INTO agent_type ({columns_str})
            VALUES ({placeholders_str})
            ON CONFLICT DO NOTHING
        '''

        max_retries = 3
        conn = psycopg2.connect(**DB_CONFIG)

        def insert_row(row):
            #Insert a single row with retries.
            cur = conn.cursor()
            attempts = 0
            while attempts < max_retries:
                try:
                    t1 = time.perf_counter()
                    cur.execute(insert_query, row)
                    conn.commit()
                    t2 = time.perf_counter()
                    log.info(f"Row inserted successfully: {row} in {t2 - t1:.4f} seconds")
                    cur.close()
                    return 1  # Row inserted
                except psycopg2.Error as err:
                    attempts += 1
                    log.warning(f"Error inserting row {row}, attempt {attempts}: {err}")
                    conn.rollback()
                    if attempts >= max_retries:
                        log.error(f"Row failed after {max_retries} retries: {row}")
                        cur.close()
                        return 0
                    time.sleep(2 + random.uniform(0, 1))

        overall_start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            inserted_count = sum(executor.map(insert_row, data))
        
        conn.close()

        total_elapsed = time.perf_counter() - overall_start
        log.info(f"Total load time: {total_elapsed:.2f} seconds for {len(data)} rows.")
        log.info(f"Inserted {inserted_count} rows into agent_type table.")

    except Exception as e:
        log.error(f"Error in load_to_postgres: {e}") 
        raise

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'depend_on_past': False,
}
with DAG(
    dag_id='csv_to_postgres_dag',
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_csv,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load_to_postgres,
        provide_context=True
    )

    extract_task >> transform_task >> load_task