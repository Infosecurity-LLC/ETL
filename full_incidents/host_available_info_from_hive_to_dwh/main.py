import logging
import subprocess
from kazoo.client import KazooClient
from typing import Dict
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
from datetime import timezone
from dateutil.parser import parse
from socutils.hive_connector import Connector

logger = logging.getLogger('host_available_info_from_hive_to_dwh')


class Selector:
    def __init__(self, settings):
        self.hive_settings = settings['hive_settings']
        self.zookeeper_settings = settings['zookeeper_settings']
        self.cursor = None
        self.kerberos_auth_keytab()
        self.connector = self.create_hive_connector()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()

    def get_hive_address(self):
        """ Получение фдреса Hive из ZooKeeper """
        zk = KazooClient(
            hosts=self.zookeeper_settings['hosts'])
        zk.start(timeout=5)
        for hiveserver2 in zk.get_children(path='hiveserver2'):
            self.hive_settings['host'], self.hive_settings['port'] = hiveserver2.split(';')[0].split('=')[1].split(
                ':')

    def create_hive_connector(self):
        """ Создаём коннектор к Hive"""
        self.get_hive_address()
        return Connector(
            auth=self.hive_settings['auth'],
            host=self.hive_settings['host'],
            port=self.hive_settings.get('port', 10000),
            username=self.hive_settings['user'],
            database=self.hive_settings['database'],
            kerberos_service_name=self.hive_settings.get('kerberos_service_name')
        )

    def kerberos_auth_keytab(self):
        """ Проходим Kerberos авторизацию """
        command = "/usr/bin/kinit -k -t %s %s@%s" % (
            self.hive_settings['keytab'],
            self.hive_settings['user'],
            self.hive_settings['domain'])
        # process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
        # output, error = process.communicate()
        try:
            subprocess.check_output(command, shell=True)
        except subprocess.CalledProcessError as err:
            raise Exception('FAIL auth [{}] with error code [{}]'.format(err.cmd, err.returncode))

    def raw_query(self, query: str):
        """ Отправляем запрос """
        self.cursor = self.connector.create()
        self.cursor.execute(query)
        return self.cursor


def resolve_doubles(data: list):
    """
     Находим дубли по хостам в организации, собираем из всех записей одну, пытаясь по максимуму заполнить все поля
    :param data: list
    :return: list
    """

    def search_doubles(writes: list):
        """ Поиск дублей"""
        doubles = {}
        for i in writes:
            for j in writes:
                if i != j and i['eventsource_host'] == j['eventsource_host'] and i['sys_org'] == j['sys_org']:
                    host_uid = f"{i['sys_org']}_{i['eventsource_host']}"
                    if not doubles.get(i['eventsource_host']):
                        doubles[host_uid] = []
                        doubles[host_uid].append(i)
                    if j not in doubles[host_uid]:
                        doubles[host_uid].append(j)
        print(f'Обнаружено дублей по {len(doubles)} хостам {doubles.keys()}')
        return doubles

    def merge_doubles(writes: list) -> Dict:
        """ Мержим несколько записей в одну, максимально заполняя значения словаря"""

        def get_value_from_other_writes(item_name: str):
            """ Выбираем полные данные (если есть) по полю среди всех записей"""
            full_value = None
            for w in writes:
                if w[item_name] is not None:
                    full_value = w[item_name]
            return full_value

        merged_write = {}
        for write in writes:
            for _k in write.keys():
                merged_write[_k] = get_value_from_other_writes(_k)
        return merged_write

    def clear_doubles(write_double_keys: list):
        """ Удаление дублей """
        import copy
        n = copy.deepcopy(data)
        for dk in write_double_keys:
            org, host = dk.split('_')
            for w in data:
                if w['sys_org'] == org and w['eventsource_host'] == host:
                    n.remove(w)
        return n

    doubles_writes = search_doubles(data)
    merged_writes = [merge_doubles(v) for v in doubles_writes.values()]
    data = clear_doubles(list(doubles_writes.keys()))
    data += merged_writes
    return data


def pull_data_from_hive(execution_date, settings):
    """ Получение данных из hive"""
    from dateutil.parser import parse
    execution_date = parse(execution_date)

    def to_dwh_format(write: dict) -> Dict:
        """ Переводим формат из hive в dwh"""
        d = datetime.utcnow().date()
        new_write = dict(
            fqdn=write.get('eventsource_fqdn'),
            hostname=write.get('eventsource_hostname'),
            ip=write.get('eventsource_ip'),
            first_seen=datetime(d.year, d.month, d.day),
            last_seen=datetime(execution_date.year, execution_date.month, execution_date.day),
            org_id=write.get('sys_org'),
            unique_name=f"{write.get('eventsource_hostname')}_{write.get('eventsource_ip')}_{write.get('sys_org')}")
        return new_write

    hive = Selector(settings)
    cur = hive.raw_query(f"""
            SELECT
                eventsource_host,
                eventsource_fqdn,
                eventsource_hostname,
                eventsource_ip,
                sys_org,
                max(eventtime) as latest_ev
            FROM raw AS r
            where sys_year = {execution_date.year} 
                and sys_month = {execution_date.month} 
                and sys_day = {execution_date.day}
            GROUP BY eventsource_host, eventsource_ip, eventsource_fqdn, eventsource_hostname, sys_org
            """)

    data = cur.fetchall()
    fields = list(field[0] for field in cur.description)
    writes = [dict(zip(fields, list(d))) for d in data]
    writes = resolve_doubles(writes)
    writes = [to_dwh_format(w) for w in writes]
    logger.info("Received %d writes", len(writes))
    return writes


def push_data_to_dwh(data: list, settings):
    """Загрузка данных в PostgreSQL"""
    import psycopg2
    from psycopg2 import errors
    def create_connection_pg(settings):
        """Создание подключения к PostgreSQL"""
        pg_hook = PostgresHook(postgres_conn_id=settings['pg_connection_id'])
        pg_conn = pg_hook.get_conn()
        pg_cursor = pg_conn.cursor()
        return pg_conn, pg_cursor

    conn, cursor = create_connection_pg(settings)
    push_query = """
            INSERT INTO host_status (fqdn, hostname, ip, first_seen, last_seen, org_id, unique_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            on conflict (unique_name) do update
            set last_seen = excluded.last_seen

        """
    for row in data:

        try:
            cursor.execute(push_query, tuple(row.values()))
        except psycopg2.errors.lookup("23503") as err:
            # logger.warning(f"Нет организации в dwh [{row}] {err}") # это норма
            conn.rollback()
        except Exception as err:
            logger.error(err)
            conn.rollback()
    conn.commit()
    conn.close()


def host_available_info_from_hive_to_dwh(execution_date, host_available_info_settings):
    execution_start_date = parse(execution_date).replace(tzinfo=timezone.utc)
    extraction_date = execution_start_date.strftime("%m-%d-%Y, %H:%M:%S")

    logging.info("Start time to extract the host available: %s", extraction_date)

    data = pull_data_from_hive(execution_date, host_available_info_settings)
    push_data_to_dwh(data, host_available_info_settings)
