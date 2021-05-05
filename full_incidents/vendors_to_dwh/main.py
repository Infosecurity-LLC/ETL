import logging

from airflow.hooks.base_hook import BaseHook

from exceptions import SettingFieldMissingError
from full_incidents.vendors_to_dwh.extract import extract
from full_incidents.vendors_to_dwh.load import load
from postgresql_utils.session import create_session

logger = logging.getLogger('vendors_to_dwh')


def loading_postgresql_settings(connection_id):
    postgresql_connection = BaseHook.get_connection(connection_id)
    try:
        postgresql_host = postgresql_connection.host
        postgresql_database = postgresql_connection.schema
        postgresql_user = postgresql_connection.login
        postgresql_password = postgresql_connection.password
        postgresql_port = postgresql_connection.port
    except KeyError as e:
        raise SettingFieldMissingError(f"Required field for PostgreSQL connection is missing: {e}")
    return postgresql_user, postgresql_password, postgresql_host, postgresql_port, postgresql_database


def vendors_to_dwh(vendors_to_dwh_etl_settings):
    streamers_connection_id = vendors_to_dwh_etl_settings["streamers_connection_id"]
    streamers_user, streamers_password, streamers_host, streamers_port, streamers_db = \
        loading_postgresql_settings(streamers_connection_id)
    streamers_url = \
        f"postgresql://{streamers_user}:{streamers_password}@{streamers_host}:{streamers_port}/{streamers_db}"
    streamers_settings = {"sqlalchemy.url": streamers_url, "sqlalchemy.echo": "False"}
    translator_session = create_session(streamers_settings)
    device_vendors, device_types, device_categories, device_sub_categories = extract(translator_session)

    dwh_connection_id = vendors_to_dwh_etl_settings["dwh_connection_id"]
    dwh_user, dwh_password, dwh_host, dwh_port, dwh_db = loading_postgresql_settings(dwh_connection_id)
    dwh_url = f"postgresql://{dwh_user}:{dwh_password}@{dwh_host}:{dwh_port}/{dwh_db}"
    dwh_settings = {"sqlalchemy.url": dwh_url, "sqlalchemy.echo": "False"}
    dwh_session = create_session(dwh_settings)

    load(device_vendors, device_types, device_categories, device_sub_categories, dwh_session)
