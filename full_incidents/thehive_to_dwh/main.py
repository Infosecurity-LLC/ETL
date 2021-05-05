import logging

from airflow.hooks.base_hook import BaseHook
from dateutil.parser import parse
from datetime import timezone

from etl_utils.airflow import get_connection
from exceptions import SettingFieldMissingError
from full_incidents.thehive_to_dwh.extract import extract
from postgresql_utils.session import create_session

from full_incidents.thehive_to_dwh.transform_load import transform_load

logger = logging.getLogger('thive_to_dwh')


def loading_elasticsearch_settings(thehive_to_dwh_etl_settings):
    elasticsearch_connection_id = thehive_to_dwh_etl_settings["es_connection_id"]
    elasticsearch_settings = BaseHook.get_connection(elasticsearch_connection_id)
    try:
        elasticsearch_host = elasticsearch_settings.host
        elasticsearch_port = elasticsearch_settings.port
        elasticsearch_index = elasticsearch_settings.schema
    except KeyError as e:
        raise SettingFieldMissingError(f"Required field for elasticsearch connection is missing: {e}")
    return elasticsearch_host, elasticsearch_port, elasticsearch_index


def thehive_to_dwh_processing(extraction_date, thehive_to_dwh_etl_settings):
    extraction_start_date = parse(extraction_date).replace(tzinfo=timezone.utc)
    extraction_date = extraction_start_date.strftime("%m-%d-%Y, %H:%M:%S")

    logging.info("Start time to extract the incident: %s", extraction_date)

    es_host, es_port, es_index = loading_elasticsearch_settings(thehive_to_dwh_etl_settings)
    extract_objects = extract(es_host, es_port, es_index, extraction_start_date)

    postgresql_connection_id = thehive_to_dwh_etl_settings["dwh_connection_id"]
    connection = get_connection(postgresql_connection_id, "PostgreSQL")
    sqlalchemy_url = \
        f"postgresql://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}"
    sqlalchemy_settings = {"sqlalchemy.url": sqlalchemy_url, "sqlalchemy.echo": "False"}
    session = create_session(sqlalchemy_settings)

    transform_load(extract_objects, session)
