import logging
from configuration_loading import prepare_logging
import pymysql
import psycopg2
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from typing import Dict, NoReturn, List
from airflow import DAG
from airflow.models import Variable
from etl_utils.airflow import get_connection
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, timezone
from dateutil.parser import parse

settings = Variable.get("incidents_for_dashboards_settings", deserialize_json=True)

logger = logging.getLogger('incidents_for_dashboard')
prepare_logging(settings)


class OtrsConnector:
    def __init__(self, connection_id: str):
        self.__connection = get_connection(connection_id)
        self.__connect = self.__connect_to_mysql()
        self.__cursor = self.__connect.cursor()

    def __connect_to_mysql(self) -> pymysql:
        mysql_conn = pymysql.connect(
            host=self.__connection.host,
            user=self.__connection.login,
            password=self.__connection.password,
            db=self.__connection.schema
        )
        return mysql_conn

    def disconnect(self):
        self.__connect.close()

    def extract(self, start_time: datetime, end_time: datetime) -> List:
        query = f"""
            select t.id as ticket_otrs_id,
               dfv.value_text as root_id,
               t.tn          as ticket_ticket_number,
               t.create_time as ticket_create_time,
               t.change_time as ticket_change_time,
               q.name        as ticket_queue_name,
               ts.name       as ticket_ticket_state_name,
               tst.name      as ticket_ticket_state_type_name
            from ticket as t
             left join ticket_state ts on ts.id = t.ticket_state_id
             left join ticket_state_type tst on ts.type_id = tst.id
             left join queue q on q.id = t.queue_id
             left outer join dynamic_field_value dfv on dfv.object_id = t.id and dfv.field_id=76
            where dfv.value_text is not NULL and t.change_time > "{start_time}" and t.change_time <= "{end_time}"
            """
        rows_len = self.__cursor.execute(query)
        rows = self.__cursor.fetchall()
        field_names = list(field[0] for field in self.__cursor.description)
        data = [dict(zip(field_names, list(d))) for d in rows]
        logger.info("Found %d writes in OTRS", rows_len)
        return data


class TheHiveConnector:
    def __init__(self, connection_id: str):
        self.__connection = get_connection(connection_id, authorization_required=False)
        self.__es_client = None

    def __connect(self) -> Elasticsearch:
        client = Elasticsearch(f"{self.__connection.host}:{self.__connection.port}")
        return client

    def extract(self, start_time: datetime, end_time: datetime):
        def format_time(time: datetime):
            """converting to milliseconds"""
            return int(time.timestamp()) * 1000

        self.__es_client = self.__connect()
        search_object = Search(using=self.__es_client, index=self.__connection.schema)
        start_time_thv = format_time(start_time)
        end_time_thv = format_time(end_time)
        query_object = search_object.filter(
            "range", **{"startDate": {"gte": start_time_thv, "lte": end_time_thv, "format": "epoch_millis"}}
        )
        query_object = query_object.query("match", tags="FINAL")
        hits_count = query_object.count()
        query_object = query_object.extra(size=hits_count)
        response = query_object.execute()
        logger.info("Found %d writes in TheHive ", hits_count)
        self.__es_client.close()
        return response


class DwhConnector:
    def __init__(self, connection_id: str):
        self.__connection = get_connection(connection_id)
        self.__cursor = None
        self.connect()

    def connect(self):
        self.__connection = psycopg2.connect(host=self.__connection.host, database=self.__connection.schema,
                                             user=self.__connection.login, password=self.__connection.password)
        self.__cursor = self.__connection.cursor()

    def commit(self):
        self.__connection.commit()

    def disconnect(self):
        self.__connection.close()

    def insert(self, incident):
        logger.info("Insert incident id: %s, ticket_number: %s", incident['incident_id'],
                    incident['ticket_ticket_number'])
        logger.debug("insert incident %s", incident)
        keys = f"({', '.join(incident.keys())})"
        query = f"""
                       insert into incidents_dashboard {keys}
                       values (%s{", %s" * (len(incident.keys()) - 1)})
                       on conflict (ticket_otrs_id) do update set
                       ticket_change_time = excluded.ticket_change_time,
                       ticket_queue_name = excluded.ticket_queue_name,
                       ticket_ticket_state_name = excluded.ticket_ticket_state_name,
                       ticket_ticket_state_type_name = excluded.ticket_ticket_state_type_name
                   """

        self.__cursor.execute(query, tuple(incident.values()))
        return


def get_extraction_time_window(execution_date):
    """ Get start-time and end_time for extraction data"""
    execution_date = parse(execution_date).replace(tzinfo=timezone.utc)
    start_time = execution_date - timedelta(minutes=30)
    end_time = execution_date
    logger.info(f"Get data from {start_time} to {end_time}")
    return start_time, end_time


def transform_thive_data(extract_objects) -> List:
    """ Change keymap and change TheHive data structure to dwh format"""

    def get_value(write: dict, key: str):
        if not key:
            return None
        value = write.get(key, None)
        if not value:
            return None
        return value

    def transform_time(thive_time):
        if thive_time:
            return datetime.fromtimestamp(thive_time / 1000)

    data = []
    incidents = extract_objects.hits.hits
    for incident in incidents:
        root_id = incident["_id"]
        inc = incident["_source"].to_dict()
        inc_f = {}
        for k, v in inc['customFields'].items():
            v.pop("order")
            if len(v) > 1:
                logger.error("WTF?!.. TheHive custom fields was change structure..")
                raise Exception()
            for _v in v.values():
                inc_f[k] = _v
        data.append(dict(
            root_id=root_id,
            incident_id=get_value(inc_f, 'id'),
            incident_usecase_id=get_value(inc_f, 'usecaseId'),
            incident_rule_id=get_value(inc_f, 'correlationRuleName'),
            incident_detect_time=transform_time(get_value(inc_f, 'detectedTime')),
            incident_event_time=transform_time(get_value(inc_f, 'correlationEventEventTime')),
            incident_organization_short_name=get_value(inc_f, 'correlationEventCollectorOrganization'),
            incident_severitylevel_level_name=get_value(inc_f, 'severityLevel'),
            incident_eventsourcecategory_name=get_value(inc_f, 'correlationEventCategory'),
            incident_destination_fqdn=get_value(inc_f, 'correlationEventDestinationFqdn'),
            incident_destination_hostname=get_value(inc_f, 'correlationEventDestinationHostname'),
            incident_destination_ip=get_value(inc_f, 'correlationEventDestinationIp'),
            incident_destination_mac=get_value(inc_f, 'correlationEventDestinationMac'),
            incident_destination_hostdomain=get_value(inc_f, ''),
            incident_source_fqdn=get_value(inc_f, 'correlationEventSourceFqdn'),
            incident_source_hostname=get_value(inc_f, ''),
            incident_eventsourcecategory_category_name=get_value(inc_f, 'correlationEventEventSourceCategory'),
            incident_source_ip=get_value(inc_f, 'correlationEventSourceIp'),
            incident_source_mac=get_value(inc_f, 'correlationEventSourceMac'),
            incident_source_hostdomain=get_value(inc_f, ''),
            incident_event_source_fqdn=get_value(inc_f, 'correlationEventEventSourceLocationFqdn'),
            incident_event_source_hostname=get_value(inc_f, 'correlationEventEventSourceLocationHostname'),
            incident_event_source_ip=get_value(inc_f, 'correlationEventEventSourceLocationIp'),
            incident_event_source_mac=get_value(inc_f, ''),
            incident_event_source_first_seen=get_value(inc_f, ''),
            incident_event_source_last_seen=get_value(inc_f, ''),
            incident_event_source_hostdomain=get_value(inc_f, ''),
            incident_event_source_server_status=get_value(inc_f, ''),
            incident_event_source_server_criticality=get_value(inc_f, ''),
            incident_collectorlocationhost_fqdn=get_value(inc_f, 'correlationEventCollectorLocationFqdn'),
            incident_collectorlocationhost_hostname=get_value(inc_f, 'correlationEventCollectorLocationHostname'),
            incident_collectorlocationhost_ip=get_value(inc_f, 'correlationEventCollectorLocationIp'),
            incident_collectorlocationhost_mac=get_value(inc_f, ''),
            incident_collectorlocationhost_hostdomain=get_value(inc_f, ''),
            incident_subject_domain=get_value(inc_f, 'correlationEventSubjectDomain'),
            incident_subject_group=get_value(inc_f, 'correlationEventSubjectGroup'),
            incident_subject_counterpart=get_value(inc_f, 'correlationEventSubjectCategory'),
            incident_subject_name=get_value(inc_f, 'correlationEventSubjectName'),
            incident_subject_version=get_value(inc_f, 'correlationEventSubjectVersion'),
            incident_subject_property=get_value(inc_f, ''),
            incident_subject_state=get_value(inc_f, ''),
            incident_subject_value=get_value(inc_f, ''),
            incident_subject_vendor=get_value(inc_f, ''),
            incident_subject_privileges=get_value(inc_f, 'correlationEventSubjectPrivileges'),
            incident_subject_path=get_value(inc_f, 'correlationEventObjectDomain'),
            incident_object_domain=get_value(inc_f, 'correlationEventObjectDomain'),
            incident_object_group=get_value(inc_f, 'correlationEventObjectGroup'),
            incident_object_counterpart=get_value(inc_f, 'correlationEventObjectCategory'),
            incident_object_name=get_value(inc_f, 'correlationEventObjectName'),
            incident_object_version=get_value(inc_f, 'correlationEventObjectVersion'),
            incident_object_property=get_value(inc_f, 'correlationEventObjectProperty'),
            incident_object_state=get_value(inc_f, 'correlationEventObjectState'),
            incident_object_value=get_value(inc_f, 'correlationEventObjectValue'),
            incident_object_vendor=get_value(inc_f, 'correlationEventObjectVendor'),
            incident_object_privileges=get_value(inc_f, ''),
            incident_object_path=get_value(inc_f, 'correlationEventObjectPath'),
            incident_inc_id=get_value(inc_f, 'correlationEventId'),
            incident_inputId=get_value(inc_f, 'correlationEventCollectorInputId'),
            incident_raw=get_value(inc_f, 'raw'),
            incident_source_port=get_value(inc_f, 'correlationEventSourcePort'),
            incident_destination_port=get_value(inc_f, 'correlationEventDestinationPort'),
            incident_subsys=get_value(inc_f, 'correlationEventEventSourceSubsys'),
            incident_vendor_name=get_value(inc_f, 'correlationEventEventSourceVendor'),
            incident_vendor_title=get_value(inc_f, 'correlationEventEventSourceTitle'),
            incident_device_category=get_value(inc_f, 'correlationEventEventSourceCategory'),
            incident_device_sub_category=get_value(inc_f, ''),
            incident_nat_hostname=get_value(inc_f, 'correlationEventSourceNatHostname'),
            incident_nat_ip=get_value(inc_f, 'correlationEventSourceNatIp'),
            incident_nat_port=get_value(inc_f, 'correlationEventSourceNatPort'),
            incident_interactiondescription_direction=get_value(inc_f, 'correlationEventInteractionDirection'),
            incident_interactiondescription_duration=get_value(inc_f, 'correlationEventInteractionDuration'),
            incident_interactiondescription_logon_type=get_value(inc_f, 'correlationEventInteractionLogonType'),
            incident_interactiondescription_protocol=get_value(inc_f, 'correlationEventInteractionProtocol'),
            incident_interactiondescription_reason=get_value(inc_f, 'correlationEventInteractionReason'),
            incident_interactiondescription_starttime=get_value(inc_f, 'correlationEventInteractionStartTime'),
            incident_interactionstatus_status_name=get_value(inc_f, 'correlationEventInteractionStatus'),
            incident_interactioncategory_category_name=get_value(inc_f, 'correlationEventInteractionAction'),
            incident_datapayload_bytes_total=get_value(inc_f, 'correlationEventDataBytesTotal'),
            incident_datapayload_bytes_in=get_value(inc_f, 'correlationEventDataBytesIn'),
            incident_datapayload_bytes_out=get_value(inc_f, 'correlationEventDataBytesOut'),
            incident_datapayload_packets_total=get_value(inc_f, 'correlationEventDataPacketsTotal'),
            incident_datapayload_packets_in=get_value(inc_f, 'correlationEventDataPacketsIn'),
            incident_datapayload_packets_out=get_value(inc_f, 'correlationEventDataPacketsOut'),
            incident_datapayload_interface=get_value(inc_f, 'correlationEventDataInterface'),
            incident_datapayload_msgid=get_value(inc_f, 'correlationEventDataMsgId'),
            incident_datapayload_origintime=transform_time(get_value(inc_f, 'correlationEventDataOriginTime')),
            incident_datapayload_recvfile=get_value(inc_f, 'correlationEventDataRecvFile'),
            incident_datapayload_tcpflag=get_value(inc_f, 'correlationEventDataTcpFlag'),
            incident_datapayload_time=transform_time(get_value(inc_f, 'correlationEventDataTime')),
            incident_datapayload_aux1=get_value(inc_f, 'correlationEventDataAux1'),
            incident_datapayload_aux2=get_value(inc_f, 'correlationEventDataAux2'),
            incident_datapayload_aux3=get_value(inc_f, 'correlationEventDataAux3'),
            incident_datapayload_aux4=get_value(inc_f, 'correlationEventDataAux4'),
            incident_datapayload_aux5=get_value(inc_f, 'correlationEventDataAux5'),
            incident_datapayload_aux6=get_value(inc_f, 'correlationEventDataAux6'),
            incident_datapayload_aux7=get_value(inc_f, 'correlationEventDataAux7'),
            incident_datapayload_aux8=get_value(inc_f, 'correlationEventDataAux8'),
            incident_datapayload_aux9=get_value(inc_f, 'correlationEventDataAux9'),
            incident_datapayload_aux10=get_value(inc_f, 'correlationEventDataAux10')
        ))
    return data


def get_otrs_data(otrs_settings: str, start_time: datetime, end_time: datetime):
    """ Get incident writes from OTRS """
    otrs = OtrsConnector(otrs_settings)
    otrs_data = otrs.extract(start_time, end_time)
    otrs.disconnect()
    return otrs_data


def get_thive_data(thive_settings: str, start_time: datetime, end_time: datetime):
    """ Get incident writes from TheHive """
    thive = TheHiveConnector(thive_settings)
    raw_thive_data = thive.extract(start_time, end_time)
    thive_data = transform_thive_data(raw_thive_data)
    return thive_data


def make_incidents(otrs_data: list, thive_data: list):
    """
    A complete list of incidents is created by combining the otrs and TheHive incident lists
    otrs incidet is supplemented with data from the incident TheHive write
    :param otrs_data:
    :param thive_data:
    :return:
    """

    def get_thive_incident(root_id: str):
        """ get incident from thehive incidents list by root_id"""
        for _thv_inc in thive_data:
            if not _thv_inc.get("root_id"):
                continue
            if root_id == _thv_inc['root_id']:
                return _thv_inc
        return None

    incidents = []
    for otrs_inc in otrs_data:
        if not otrs_inc.get('root_id') or len(otrs_inc['root_id']) != 20:
            logger.error(f"ticket_ticket_number: {otrs_inc['ticket_ticket_number']}, "
                         f"ticket_create_time: {otrs_inc['ticket_create_time']} "
                         f"ticket_change_time: {otrs_inc['ticket_change_time']} "
                         f"has no root_id (thive inc_d)")
            continue
        thv_inc = get_thive_incident(otrs_inc['root_id'])
        if not thv_inc:
            logger.error(f"ticket_ticket_number: {otrs_inc['ticket_ticket_number']}, "
                         f"ticket_create_time: {otrs_inc['ticket_create_time']} "
                         f"has no write in TheHive output incidents list. root_id: {otrs_inc['root_id']}")
            continue
        incident = {**otrs_inc, **thv_inc}
        incident.pop('root_id')
        incidents.append(incident)
    logger.info("Ready for upload %s incidents", len(incidents))
    return incidents


def upload_incidents_to_dwh(dwh_settings: str, incidents: list):
    dwh = DwhConnector(dwh_settings)
    for incident in incidents:
        dwh.insert(incident)
    dwh.commit()
    dwh.disconnect()


def main(execution_date):
    start_time, end_time = get_extraction_time_window(execution_date)
    otrs_data = get_otrs_data(settings['otrs_connection_id'], start_time, end_time)
    thive_data = get_thive_data(settings['es_connection_id'], start_time, end_time)
    incidents = make_incidents(otrs_data, thive_data)
    upload_incidents_to_dwh(settings['dwh_connection_id'], incidents)


default_dag_rules = settings['airflow_settings']
default_args = {
    'owner': default_dag_rules['owner'],
    'max_active_runs': default_dag_rules['max_active_runs'],
    'depends_on_past': default_dag_rules['depends_on_past'],
    'wait_for_downstream': default_dag_rules['wait_for_downstream'],
    'trigger_rule': default_dag_rules['trigger_rule'],
    'retries': default_dag_rules['retries'],
    'retry_delay': timedelta(seconds=default_dag_rules['retry_delay_seconds']),
    'start_date': parse(default_dag_rules['start_date']),
    # max time allowed for the execution of this task instance
    'execution_timeout': timedelta(minutes=default_dag_rules['execution_timeout_minutes']),
    'catchup': default_dag_rules['catchup']
}
with DAG(
        dag_id='incidents_for_dashboards',
        default_args=default_args,
        schedule_interval="*/10 * * * *",
        catchup=False
) as dag:
    t = PythonOperator(
        task_id='pull-push',
        trigger_rule='all_done',
        python_callable=main,
        dag=dag,
        op_kwargs={"execution_date": "{{execution_date}}"}
    )
