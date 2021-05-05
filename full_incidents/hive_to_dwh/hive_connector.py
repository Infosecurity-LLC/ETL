from typing import Dict

from socutils.hive_connector import Connector


def create_hive_connector(settings: Dict):
    hive_settings = settings
    return Connector(
        auth=hive_settings['auth'],
        host=hive_settings['host'],
        port=hive_settings.get('port', 10000),
        username=hive_settings['user'],
        database=hive_settings['database'],
        kerberos_service_name=hive_settings.get('kerberos_service_name')
    )
