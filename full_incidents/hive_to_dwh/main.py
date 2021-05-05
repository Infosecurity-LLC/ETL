import logging

from dateutil.parser import parse
from datetime import timezone

from etl_utils.airflow import get_connection
from full_incidents.hive_to_dwh.extract import extract
from postgresql_utils.session import create_session
from full_incidents.hive_to_dwh.load import load

from exceptions import SettingFieldMissingError, SettingFieldTypeError

logger = logging.getLogger('hive_to_dwh')


def loading_start_date(hive_to_dwh_etl_settings):
    try:
        start_date_string = hive_to_dwh_etl_settings["airflow_settings"]["start_date"]
    except KeyError as err:
        raise SettingFieldMissingError(f"Field start is missing: {err}")
    try:
        start_date = parse(start_date_string)
    except Exception as err:
        raise SettingFieldTypeError(f"Error in start date parser: {err}")
    return start_date


def loading_hive_settings(hive_to_dwh_etl_settings):
    hive_settings = hive_to_dwh_etl_settings["hive_settings"]
    loaded_hive_settings = {}
    try:
        loaded_hive_settings["auth"] = hive_settings["auth"]
        loaded_hive_settings["kerberos_service_name"] = hive_settings["kerberos_service_name"]
        loaded_hive_settings["user"] = hive_settings["user"]
        loaded_hive_settings["domain"] = hive_settings["domain"]
        loaded_hive_settings["database"] = hive_settings["database"]
        loaded_hive_settings["keytab"] = hive_settings["keytab"]
        loaded_hive_settings["table"] = hive_settings["table"]
    except KeyError as e:
        raise SettingFieldMissingError(f"Required field for Hive connection is missing: {e}")
    return loaded_hive_settings


def loading_zookeeper_settings(hive_to_dwh_etl_settings):
    zookeeper_settings = hive_to_dwh_etl_settings["zookeeper_settings"]
    return zookeeper_settings


def hive_to_dwh_processing(extraction_date, hive_to_dwh_etl_settings):
    extraction_start_date = parse(extraction_date).replace(tzinfo=timezone.utc)
    extraction_date = extraction_start_date.strftime("%m-%d-%Y, %H:%M:%S")

    logging.info("Start time to extract the asset: %s", extraction_date)
    hive_settings = loading_hive_settings(hive_to_dwh_etl_settings)
    zookeeper_settings = loading_zookeeper_settings(hive_to_dwh_etl_settings)

    hive_assets, sys_date = extract(hive_settings, zookeeper_settings, extraction_start_date)

    postgresql_connection_id = hive_to_dwh_etl_settings["dwh_connection_id"]
    connection = get_connection(postgresql_connection_id, "PostgreSQL")
    sqlalchemy_url = \
        f"postgresql://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}"
    sqlalchemy_settings = {"sqlalchemy.url": sqlalchemy_url, "sqlalchemy.echo": "False"}
    session = create_session(sqlalchemy_settings)
    load(hive_assets, sys_date, session)
