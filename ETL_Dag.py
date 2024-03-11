import datetime
import airflow
import json
from airflow.providers.http.operators.http import SimpleHttpOperator


with airflow.DAG(
        "cloud_run_dag",
        default_args = {  # Use 'default_args' instead of 'dag_default_args'
            'depends_on_past': False,
            'retries': 0,
            "start_date": datetime.datetime(2024, 3, 7)  # Yesterday's date
        },
        schedule_interval="*/2 * * * *") as dag:
    # Données JSON à envoyer dans la requête
    json_data = {
        "dataset": "test_data_platforme",
        "bucket_name": "fivetran_flows",
        "primary_key": "cin",
        "separator": ","
    }

    data = json.dumps(json_data, ensure_ascii=False).encode("utf-8")

    cloud_run_service = SimpleHttpOperator(
        task_id="cloud_run_service",
        method="POST",
        http_conn_id="flask_app_connection",
        endpoint="/primary_keys",
        data=data,
        headers={"Content-Type": "application/json"}
    )

    cloud_run_service