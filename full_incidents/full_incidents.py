import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta

from configuration_loading import prepare_logging
from etl_utils.airflow import dag_rules

from full_incidents.orgreplicator.orgreplicator import orgreplicator
from full_incidents.vendors_to_dwh.main import vendors_to_dwh
from full_incidents.hive_to_dwh.main import hive_to_dwh_processing
from full_incidents.host_available_info_from_hive_to_dwh.main import host_available_info_from_hive_to_dwh
from full_incidents.replication_otrs_to_dwh.main import replication_otrs_to_dwh
from full_incidents.thehive_to_dwh.main import thehive_to_dwh_processing
from full_incidents.translator_to_dwh.main import translator_to_dwh
from full_incidents.siem_archiver.main import prepare_configs, create_spark_submit_operator


logger = logging.getLogger('full_incidents')

REQUIRED_AIRFLOW_VARIABLE_KEYS = (
    "start_date", "owner", "max_active_runs", "depends_on_past", "wait_for_downstream", "trigger_rule", "retries",
    "retry_delay_seconds", "execution_timeout_hours", "catchup"
)

full_incidents_etl_settings = Variable.get('full_incidents_etl_settings', deserialize_json=True)

vendors_to_dwh_etl_settings = full_incidents_etl_settings['vendors_to_dwh_etl_settings']
orgreplicator_settings = full_incidents_etl_settings["orgreplicator_settings"]
translator_to_dwh_settings = full_incidents_etl_settings['translator_to_dwh_settings']
replication_otrs_to_dwh_settings = full_incidents_etl_settings["replication_otrs_to_dwh_settings"]
host_available_info_settings = full_incidents_etl_settings["host_available_info_from_hive_to_dwh_settings"]
hive_to_dwh_etl_settings = full_incidents_etl_settings['hive_to_dwh_etl_settings']
thehive_to_dwh_etl_settings = full_incidents_etl_settings['thehive_to_dwh_etl_settings']
siem_archiver_settings = Variable.get("ETL_archiver_config", deserialize_json=True)
WORK_DIR = siem_archiver_settings["WORK_DIR"]
DATA_TYPES = siem_archiver_settings["DATA_TYPES"]

logging_settings = full_incidents_etl_settings["logging_settings"]
prepare_logging(logging_settings, logger)


default_dag_rules = dag_rules(full_incidents_etl_settings["airflow_settings"], REQUIRED_AIRFLOW_VARIABLE_KEYS)


default_args = {
    'owner': default_dag_rules['owner'],
    'depends_on_past': default_dag_rules['depends_on_past'],
    'wait_for_downstream': default_dag_rules['wait_for_downstream'],
    'trigger_rule': default_dag_rules['trigger_rule'],
    'retries': default_dag_rules['retries'],
    'retry_delay': timedelta(seconds=default_dag_rules['retry_delay_seconds']),
    # max time allowed for the execution of this task instance
    'execution_timeout': timedelta(hours=default_dag_rules['execution_timeout_hours']),
}


def prepare_siem_archiver_config_operator():
    streamer_configs = {}
    for process_type, conf_var in DATA_TYPES.items():
        try:
            conf = Variable.get(conf_var)
            log4j = Variable.get("archiver_log4j.conf")
            streamer_configs[process_type] = {'conf': conf, 'log4j': log4j}
        except (ValueError, KeyError) as e:
            logging.error(str(e))
            raise e

    prepare_config = PythonOperator(task_id='prepare_configs',
                                    python_callable=prepare_configs,
                                    depends_on_past=default_dag_rules['depends_on_past'],
                                    wait_for_downstream=default_dag_rules['wait_for_downstream'],
                                    trigger_rule=default_dag_rules['trigger_rule'],
                                    dag=dag,
                                    op_kwargs={'work_dir': WORK_DIR, "streamer_configs": streamer_configs})
    return prepare_config


def prepare_siem_archiver_operators():
    siem_archiver = {}
    try:
        for process_type, conf_var in DATA_TYPES.items():
            siem_archiver[f"archiver_{process_type}"] = create_spark_submit_operator(
                dag=dag, default_dag_rules=default_dag_rules, dag_config=siem_archiver_settings,  work_dir=WORK_DIR,
                spark_submit_operator_type=process_type
            )
    except (ValueError, KeyError) as e:
        logging.error(str(e))
        raise e
    return siem_archiver


with DAG('full_incidents',
         description="Loading incident data from Hive, Otrs, Streamers db, Translator db, TheHive to DWH",
         default_args=default_args,
         schedule_interval='0 0 * * *',
         start_date=default_dag_rules['start_date'],
         max_active_runs=default_dag_rules['max_active_runs'],
         catchup=default_dag_rules['catchup']
         ) as dag:

    start_dummy = DummyOperator(
        task_id='start_dummy',
        trigger_rule=default_dag_rules['trigger_rule'],
        depends_on_past=default_dag_rules['depends_on_past'],
        wait_for_downstream=default_dag_rules['wait_for_downstream'],
        dag=dag
    )

    vendors_to_dwh = PythonOperator(
        task_id='vendors_to_dwh',
        trigger_rule=default_dag_rules['trigger_rule'],
        python_callable=vendors_to_dwh,
        dag=dag,
        depends_on_past=default_dag_rules['depends_on_past'],
        wait_for_downstream=default_dag_rules['wait_for_downstream'],
        op_kwargs={"vendors_to_dwh_etl_settings": vendors_to_dwh_etl_settings}
    )

    orgreplicator = PythonOperator(
        task_id='orgreplicator',
        python_callable=orgreplicator,
        dag=dag,
        trigger_rule=default_dag_rules['trigger_rule'],
        depends_on_past=default_dag_rules['depends_on_past'],
        wait_for_downstream=default_dag_rules['wait_for_downstream'],
        op_kwargs={"orgreplicator_settings": orgreplicator_settings}
    )

    translator_to_dwh = PythonOperator(
        task_id='translator_to_dwh',
        python_callable=translator_to_dwh,
        dag=dag,
        trigger_rule=default_dag_rules['trigger_rule'],
        depends_on_past=default_dag_rules['depends_on_past'],
        wait_for_downstream=default_dag_rules['wait_for_downstream'],
        op_kwargs={"translator_to_dwh_settings": translator_to_dwh_settings}
    )

    replication_otrs_to_dwh = PythonOperator(
        task_id='replication_otrs_to_dwh',
        python_callable=replication_otrs_to_dwh,
        dag=dag,
        trigger_rule=default_dag_rules['trigger_rule'],
        depends_on_past=default_dag_rules['depends_on_past'],
        wait_for_downstream=default_dag_rules['wait_for_downstream'],
        op_kwargs={
            "execution_date": "{{execution_date}}", "replication_otrs_to_dwh_settings": replication_otrs_to_dwh_settings
        }
    )

    host_available_info = PythonOperator(
        task_id='shift_host_available_info_from_hive_to_dwh',
        python_callable=host_available_info_from_hive_to_dwh,
        dag=dag,
        trigger_rule=default_dag_rules['trigger_rule'],
        depends_on_past=default_dag_rules['depends_on_past'],
        wait_for_downstream=default_dag_rules['wait_for_downstream'],
        op_kwargs={
            "execution_date": "{{execution_date}}", "host_available_info_settings": host_available_info_settings
        }
    )

    hive_to_dwh = PythonOperator(
        task_id='hive_to_dwh_processing',
        python_callable=hive_to_dwh_processing,
        dag=dag,
        trigger_rule=default_dag_rules['trigger_rule'],
        depends_on_past=default_dag_rules['depends_on_past'],
        wait_for_downstream=default_dag_rules['wait_for_downstream'],
        op_kwargs={
            'extraction_date': '{{ execution_date }}', 'hive_to_dwh_etl_settings': hive_to_dwh_etl_settings
        }
    )

    thehive_to_dwh = PythonOperator(
        task_id='thehive_to_dwh_processing',
        python_callable=thehive_to_dwh_processing,
        dag=dag,
        trigger_rule=default_dag_rules['trigger_rule'],
        depends_on_past=default_dag_rules['depends_on_past'],
        wait_for_downstream=default_dag_rules['wait_for_downstream'],
        op_kwargs={
            'extraction_date': '{{ execution_date }}', 'thehive_to_dwh_etl_settings': thehive_to_dwh_etl_settings
        }
    )

    stop_dummy = DummyOperator(
        task_id='stop_dummy',
        trigger_rule=default_dag_rules['trigger_rule'],
        depends_on_past=default_dag_rules['depends_on_past'],
        wait_for_downstream=default_dag_rules['wait_for_downstream'],
        dag=dag
    )

prepare_config_operator = prepare_siem_archiver_config_operator()
siem_archiver_operators = prepare_siem_archiver_operators()
try:
    archiver_normalized = siem_archiver_operators["archiver_normalized"]
    archiver_chain = siem_archiver_operators["archiver_chain"]
    archiver_raw = siem_archiver_operators["archiver_raw"]
except (ValueError, KeyError) as e:
    logging.error(str(e))
    raise e

start_dummy >> prepare_config_operator >> archiver_normalized >> archiver_raw >> host_available_info >> stop_dummy
start_dummy >> prepare_config_operator >> archiver_normalized >> archiver_raw >> archiver_chain >> stop_dummy
start_dummy >> prepare_config_operator >> archiver_normalized >> hive_to_dwh >> replication_otrs_to_dwh >> thehive_to_dwh >> stop_dummy
start_dummy >> vendors_to_dwh >> hive_to_dwh >> replication_otrs_to_dwh >> thehive_to_dwh >> stop_dummy
start_dummy >> orgreplicator >> hive_to_dwh >> replication_otrs_to_dwh >> thehive_to_dwh >> stop_dummy
start_dummy >> translator_to_dwh >> hive_to_dwh >> replication_otrs_to_dwh >> thehive_to_dwh >> stop_dummy
start_dummy >> stop_dummy
