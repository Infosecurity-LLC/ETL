import logging
from etl_utils.airflow import get_connection
from full_incidents.translator_to_dwh.extract import extract
from full_incidents.translator_to_dwh.load.load import load
from postgresql_utils.session import create_session
from full_incidents.translator_to_dwh.transform.transform import transform

logger = logging.getLogger('translator_to_dwh')


def translator_to_dwh(translator_to_dwh_settings):
    translator_connection_id = translator_to_dwh_settings["translator_connection_id"]
    translator_connection = get_connection(translator_connection_id, "Translator PostgreSQL")
    translator_url = \
        f"postgresql://{translator_connection.login}:{translator_connection.password}@" \
        f"{translator_connection.host}:{translator_connection.port}/{translator_connection.schema}"
    translator_settings = {"sqlalchemy.url": translator_url, "sqlalchemy.echo": "False"}
    translator_session = create_session(translator_settings)
    usecases, rules, categories, usecase_categories = extract(translator_session)

    dwh_connection_id = translator_to_dwh_settings["dwh_connection_id"]
    dwh_connection = get_connection(dwh_connection_id, "DWH PostgreSQL")
    dwh_url = f"postgresql://{dwh_connection.login}:{dwh_connection.password}@" \
              f"{dwh_connection.host}:{dwh_connection.port}/{dwh_connection.schema}"
    dwh_settings = {"sqlalchemy.url": dwh_url, "sqlalchemy.echo": "False"}
    dwh_session = create_session(dwh_settings)

    usecases = transform(usecases_objs=usecases, usecase_categories_objs=usecase_categories)
    load(usecases=usecases, rules=rules, categories=categories, session=dwh_session)
