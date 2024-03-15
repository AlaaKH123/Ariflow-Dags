import datetime
import airflow
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

from xml.etree import ElementTree as ET
from airflow.models import Variable

def generate_json_data(config_path):
    """
    Parses the provided config file and generates the JSON data for the request.

    Args:
        config_path (str): Path to the configuration file (XML in this case).

    Returns:
        dict: The generated JSON data.
    """
    root = ET.fromstring(config_path)

    target_dataset = Variable.get("Dataset_name")
    target_tables = Variable.get("Tables_names")
    separators = []
    all_primary_keys = []
    modes = []

    if target_tables == "all":
        target_tables_list = []
        ingest_all = True
    else:
        target_tables_list = target_tables.split(',')
        ingest_all = False

    # Extract configuration data
    bucket_name = Variable.get("Bucket_name")
    primary_keys_list = []
    separator = None  # Initialize separator with None
    mode = ""
    target_tables_list_sent = []
    for dataset in root.find('bucket').findall('dataset'):
        if dataset.find('name').text == target_dataset:
            for table in dataset.findall('table'):
                table_name = table.find('name').text
                print(f"table name : {table_name}")
                if ingest_all or table_name in target_tables_list:
                    primary_keys_elements = table.findall('primary-keys/key')
                    if primary_keys_elements:
                        print(f"pk elements : {primary_keys_elements}")
                        primary_keys_list = [key.text for key in primary_keys_elements]
                        mode = "delta"
                    else:
                        primary_keys_list = []
                        mode = "full"
                    modes.append(mode)
                    all_primary_keys.append(primary_keys_list)
                    target_tables_list_sent.append(table_name)

                    separator = table.find('separator').text
                    separators.append(separator)
                print(f"mode : {mode}")
                print(f"separator : {separator}")
                print(f"primary keys : {primary_keys_list}")
                print(f"target tables list sent : {target_tables_list_sent}")

    # Generate the JSON request with nested structure
    table_info = {}
    for table_name, separator, mode, primary_keys in zip(target_tables_list_sent, separators, modes, all_primary_keys):
        table_info[table_name] = {
            "primary_keys": ",".join(primary_keys),  # Join primary keys with a comma
            "mode": mode,
            "separator": separator
        }
    print(f"table info : {table_info}")

    # Construct the JSON data
    json_data = {
        "dataset": target_dataset,
        "bucket_name": bucket_name,
        "tables": table_info
    }
    data = json.dumps(json_data)

    return data

with airflow.DAG(
        "docker_run_dag",
        default_args={
            'owner': 'airflow',
            'depends_on_past': False,
            'retries': 0,
            'start_date': datetime.datetime.utcnow(),  # Set start date to current time
            'email_on_failure': True,
            'email': 'alaa.khmiri@olivesoft.fr'
        },
        schedule_interval="0 */6 * * *") as dag:


    config_path = Variable.get("configs")
    # Generate the JSON data

    generate_json_data_task = PythonOperator(
        task_id="generate_json_data",
        python_callable=generate_json_data,
        op_args=[config_path],
        provide_context=True,
    )


    docker_run_dag = SimpleHttpOperator(
        task_id="docker_run_dag",
        method="POST",
        http_conn_id="flask_app_connection",
        endpoint="/primary_keys",
        data=generate_json_data_task.output,
        headers={"Content-Type": "application/json"}
    )

    generate_json_data_task >> docker_run_dag