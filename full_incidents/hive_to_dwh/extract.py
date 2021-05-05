import logging

from kazoo.client import KazooClient

from full_incidents.hive_to_dwh.hive_connector import create_hive_connector


logger = logging.getLogger('hive_to_dwh')


def extract(hive_settings, zookeeper_settings, extraction_start_date):
        zk = KazooClient(
                hosts=zookeeper_settings)

        zk.start(timeout=5)

        for hiveserver2 in zk.get_children(path='hiveserver2'):
                # string like serverUri=host:port;version=version;sequence=sequence
                host_port = hiveserver2.split(';')[0].split('=')[1].split(':')
                hive_settings["host"] = host_port[0]
                hive_settings["port"] = host_port[1]

        login = "{user}@{domain}".format(user=hive_settings['user'], domain=hive_settings['domain'])
        command = "/usr/bin/kinit -k -t {keytab} {user}".format(keytab=hive_settings['keytab'], user=login)
        import subprocess
        process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
        process.communicate()

        sys_date = {}
        sys_date["sys_day"] = extraction_start_date.day
        sys_date["sys_month"] = extraction_start_date.month
        sys_date["sys_year"] = extraction_start_date.year

        hive_connector = create_hive_connector(hive_settings)
        cursor = hive_connector.create()

        cursor.execute(
            f"""
            SELECT collector_location_fqdn as fqdn, collector_location_hostname as hostname, 
collector_location_ip as ip, collector_organization as organization,
'' as title, true as is_collector, false as isnetworklocal,
false as is_source, false as is_eventsource
FROM {"normalized"} 
WHERE sys_day=%(extraction_start_day)s AND sys_month=%(extraction_start_month)s AND sys_year=%(extraction_start_year)s 
AND (collector_location_hostname is not NULL OR collector_location_ip is not NULL) 
AND collector_organization is not NULL

UNION 

SELECT eventsource_location_fqdn as fqdn, eventsource_location_hostname as hostname, eventsource_location_ip as ip,
sys_org as organization, eventsource_title as title, 
false as is_collector, false as isnetworklocal, false as is_source, true as is_eventsource
FROM {"normalized"} 
WHERE sys_day=%(extraction_start_day)s AND sys_month=%(extraction_start_month)s AND sys_year=%(extraction_start_year)s 
AND (eventsource_location_hostname is not NULL OR eventsource_location_ip is not NULL) AND sys_org is not NULL

UNION 

SELECT source_fqdn as fqdn, source_hostname as hostname, source_ip as ip, sys_org as organization,
'' as title, false as is_collector, 
source_enrichment_isnetworklocal as isnetworklocal, true as is_source, false as is_eventsource
FROM {"normalized"} 
WHERE sys_day=%(extraction_start_day)s AND sys_month=%(extraction_start_month)s AND sys_year=%(extraction_start_year)s 
AND (source_hostname is not NULL OR source_ip is not NULL) AND sys_org is not NULL

UNION 

SELECT destination_fqdn as fqdn, destination_hostname as hostname, destination_ip as ip, sys_org as organization, 
'' as title, false as is_collector, 
false as isnetworklocal, false as is_source, false as is_eventsource
FROM {"normalized"} 
WHERE sys_day=%(extraction_start_day)s AND sys_month=%(extraction_start_month)s AND sys_year=%(extraction_start_year)s 
AND (destination_hostname is not NULL OR destination_ip is not NULL) AND sys_org is not NULL 
AND destination_enrichment_isnetworklocal=true
                            """,
                parameters={'extraction_start_year': sys_date["sys_year"],
                            'extraction_start_month': sys_date["sys_month"],
                            'extraction_start_day': sys_date["sys_day"]}
        )
        raws = cursor.fetchall()
        fields = [field[0].split('.')[1] for field in cursor.description]
        assets = []
        logging.info("Size of data extracted from the hive: %d", len(raws))
        for raw in raws:
                asset = dict(zip(fields, raw))
                assets.append(asset)
        cursor.close()
        return assets, sys_date
