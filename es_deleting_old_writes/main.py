import logging
import requests
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow import AirflowException
from airflow.hooks.http_hook import HttpHook
from airflow.models import Variable
from datetime import datetime, timedelta
from configuration_loading import prepare_logging

logger = logging.getLogger('es_cleaner')
settings = Variable.get("es_deleting_old_writes_settings", deserialize_json=True)
prepare_logging()


class ElasticHook(HttpHook):
    """
    Для взаимодействия с ластиком
    надо будет, либо отрефакторить это, вынести в хуки,
    либо посмотреть в сторону https://github.com/apache/airflow/tree/master/airflow/providers/elasticsearch
    пока так
    """

    def delete(self, index_and_type, args):
        session = self.get_conn({})
        url = self.base_url + index_and_type + '/_delete_by_query'
        req = requests.Request('POST', url, json=args)
        prep_req = session.prepare_request(req)
        resp = session.send(prep_req)
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError:
            logging.error("HTTP error: " + resp.reason)
            raise AirflowException(str(resp.status_code) + ":" + resp.reason)

        return json.loads(resp.content)


def main():
    """
    Удаляются записи старше instruction['lt']
    :return:
    """
    hook = ElasticHook(http_conn_id=settings['es_connection_id'])
    for instruction in settings.get('instructions'):
        logger.info("Start clear %s" % instruction['index'])
        resp = hook.delete(f"/{instruction['index']}", {
            'query': {
                'range': {
                    f"{instruction['time_field_name']}": {
                        'lt': f"{instruction['lt']}",
                    }
                }
            }
        })
        logger.info("Deleted %s writes" % resp.get('deleted'))
    return True


default_args = {
    'start_date': datetime(2020, 6, 26),
    'max_active_runs': 1,
    'retries': 0,
    'retry_delay': timedelta(seconds=30)
}

with DAG(
        dag_id='ES_deleting_old_writes',
        description="Удаление старых записей из индексов ES",
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        catchup=False
) as dag:
    delete_old_writes = PythonOperator(
        task_id='delete_old_writes',
        python_callable=main,
        dag=dag
    )
