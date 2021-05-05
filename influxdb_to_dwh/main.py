import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from dateutil.parser import parse
from datetime import timezone, timedelta
from etl_utils.airflow import get_connection
from etl_utils.airflow import dag_rules
from influxdb_to_dwh.extract import extract
from influxdb_to_dwh.load import load
from postgresql_utils.session import create_session

logger = logging.getLogger('influxdb_to_dwh')

REQUIRED_AIRFLOW_VARIABLE_KEYS = (
    "start_date", "owner", "max_active_runs", "trigger_rule", "retries", "retry_delay_seconds",
    "execution_timeout_hours", "catchup"
)

influxdb_to_dwh_etl_settings = Variable.get('influxdb_to_dwh_settings', deserialize_json=True)
default_dag_rules = dag_rules(influxdb_to_dwh_etl_settings["airflow_settings"], REQUIRED_AIRFLOW_VARIABLE_KEYS)
process_settings = influxdb_to_dwh_etl_settings['process_settings']

default_args = {
    'owner': default_dag_rules['owner'],
    'max_active_runs': default_dag_rules['max_active_runs'],
    'trigger_rule': default_dag_rules['trigger_rule'],
    'retries': default_dag_rules['retries'],
    'retry_delay': timedelta(seconds=default_dag_rules['retry_delay_seconds']),
    # max time allowed for the execution of this task instance
    'execution_timeout': timedelta(hours=default_dag_rules['execution_timeout_hours']),
    'catchup': default_dag_rules['catchup']
}


def influxdb_to_dwh_processing(execution_date, influxdb_process_settings):
    start_datetime = parse(execution_date).replace(tzinfo=timezone.utc)

    influxdb_connection_id = influxdb_process_settings["influxdb"]["connection_id"]
    influxdb_connection_parameters = influxdb_process_settings["influxdb"].get("connection_parameters", {})
    connection = get_connection(influxdb_connection_id, "InfluxDB", False)
    extract_objects = extract(start_datetime, connection.host, connection.port, connection.login, connection.password,
                              connection.schema, influxdb_connection_parameters)
    if not extract_objects:
        return

    postgresql_connection_id = influxdb_process_settings["dwh_connection_id"]
    psql_conn = get_connection(postgresql_connection_id, "PostgreSQL")
    sqlalchemy_url = \
        f"postgresql://{psql_conn.login}:{psql_conn.password}@{psql_conn.host}:{psql_conn.port}/{psql_conn.schema}"
    sqlalchemy_settings = {"sqlalchemy.url": sqlalchemy_url, "sqlalchemy.echo": "False"}
    session = create_session(sqlalchemy_settings)

    load(org_client_metrics=extract_objects, start_date=start_datetime, session=session)


with DAG('influxdb_to_dwh',
         description="loading data on the availability of hosts from the database Influxdb table flow_metrics to DWH",
         default_args=default_args,
         schedule_interval='*/10 * * * *',
         start_date=default_dag_rules['start_date'],
         ) as dag:
    influxdb_to_dwh = PythonOperator(task_id='influxdb_to_dwh_processing',
                                     python_callable=influxdb_to_dwh_processing,
                                     dag=dag,
                                     trigger_rule="all_success",
                                     op_kwargs={"execution_date": "{{execution_date}}",
                                                'influxdb_process_settings': process_settings})

influxdb_to_dwh
