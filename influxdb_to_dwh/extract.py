import logging
from datetime import datetime, timedelta

from influxdb import InfluxDBClient

logger = logging.getLogger('influxdb_to_dwh')


def extract(start_datetime: datetime, influxdb_host: str, influxdb_port: int, influxdb_user: str, influxdb_password: str,
            influxdb_database: str, connection_parameters: dict):
    """
    Извлечение метрик из influx по организациям.
    По запросу в InfluxDB получаем список организаций. В цикле по каждой организации получаем поля таблицы ClientMetrics
    и записываем это в список. Если по каждой организации в таблице пусто - пишем соответствующее сообщение,
    return False и процесс дага останавливается.
    return: List[Tuple[Tuple[str, dict], Generator[ResultSet, None, None]]] or False
    """

    client = InfluxDBClient(host=influxdb_host, port=influxdb_port, username=influxdb_user, password=influxdb_password,
                            database=influxdb_database, **connection_parameters)
    execution_date_str = (start_datetime + timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
    query_t = f"SHOW TAG VALUES FROM ClientMetrics WITH KEY IN (client_org) WHERE client_org =~ /./ " \
              f"AND client_org != 'unknown'"
    query_results_t = client.query(query_t)
    results_t = query_results_t.items()
    if not results_t:
        logging.warning("No organization data in influence db")
        return False
    _, values_generator = results_t[0]
    values = list(values_generator)
    organizations = [value["value"] for value in values]

    hits_count = 0
    client_metrics_is_exist = False
    results = []
    start_datetime_str = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
    for organization in organizations:
        query = f"SELECT non_negative_derivative(mean(msgCount), 10m)  FROM ClientMetrics " \
                f"WHERE (client_org = '{organization}') AND status = 'raw' AND time >= '{start_datetime_str}' AND " \
                f"time <= '{execution_date_str}' GROUP BY time(10m), *"
        query_results = client.query(query)
        result = query_results.items()

        if not result:
            logging.warning(f"No client_metrics data for organization {organization} in influence db")
            continue
        client_metrics_is_exist = True

        hits_count += len(result)
        results.append(result)

    if not client_metrics_is_exist:
        return False

    logging.info("Found %d hits in InfluxDB in time %s to %s", hits_count, start_datetime_str, execution_date_str)

    return results
