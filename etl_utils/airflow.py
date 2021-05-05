import logging
from airflow.hooks.base_hook import BaseHook
from dateutil.parser import parse

from exceptions import SettingFieldMissingError, SettingFieldTypeError

logger = logging.getLogger('airflow')


def dag_rules(rules, required_keys):
    """
    Serializing dag parameters from variable. Checking for required fields using required_keys
    :return dict dag_rules. An example of how they should look, in ..._settings.json, "airflow_settings"
    """
    is_exists_rule_keys = all(key in rules.keys() for key in required_keys)
    if not is_exists_rule_keys:
        raise SettingFieldMissingError(
            f"Some of the required fields {','.join(required_keys)} are missing in Variable, "
            f"get: {','.join(rules.keys())}")
    try:
        rules["start_date"] = parse(rules["start_date"])
    except Exception as err:
        raise SettingFieldTypeError(f"Error in start date parser: {err}")
    return rules


def get_connection(connection_id: str, connection_type: str = None, authorization_required=True):
    if not connection_type:
        connection_type = connection_id
    connection = BaseHook.get_connection(connection_id)
    if authorization_required and (not connection.login or
                                   not connection.password):
        raise SettingFieldMissingError(f"In airflow Connection required field for {connection_type} "
                                       f"connection is missing")
    if not connection.host or not connection.port or not connection.schema:
        raise SettingFieldMissingError(f"In airflow Connection required field for {connection_type} "
                                       f"connection is missing")
    return connection
