import os
import re
import sys
import csv
import radix
import logging
import psycopg2
from psycopg2.extras import execute_batch
import dask.dataframe as dd
from airflow import DAG
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from configuration_loading import prepare_logging

settings = Variable.get("geoetl_settings", deserialize_json=True)
temp_file_path = os.path.join("/tmp", "geoinfo")
source_tmp_path = os.path.join(temp_file_path, "source")
processing_tmp_path = os.path.join(temp_file_path, "processing")

logger = logging.getLogger('geoinfo')
prepare_logging(settings)


class DB:
    def __init__(self, settings):
        self.__settings = settings
        self.__connect = None
        self.cursor = None
        self.start_connection()

    def start_connection(self):
        self.__connect = psycopg2.connect(**self.__settings)
        if self.__connect:
            self.cursor = self.__connect.cursor()
            self.__connect.autocommit = True

    def close_connection(self):
        if self.__connect:
            self.__connect.close()

    def insert_to_db(self, query: str, values_list: list, batch_len: int):
        if not self.cursor:
            raise Exception("Cursor does not exist")
        if values_list:
            execute_batch(self.cursor, query, values_list, page_size=batch_len)

    def nextval(self, sequence_name):
        self.cursor.execute(f"SELECT nextval('{sequence_name}');")
        return self.cursor.fetchone()[0]

    def clear_deprecated_data(self, table_name: str, offset_load_id: int = 2):
        """ Удаление старых записей, где load"""
        query = f"delete from {table_name} " \
                f"where load_id <= (select max(load_id) from {table_name})-{offset_load_id}"
        self.cursor.execute(query)


def dump_routes(input_file, output_file):
    infp = open(input_file, 'r', encoding='ISO-8859-1')
    outfp = open(output_file, 'wt', encoding='UTF8')

    logger.debug("Parsing %s -> %s ..." % (input_file, output_file))

    route = {}
    re_route = re.compile('^route\:\s*(.+)$')
    re_route6 = re.compile('^route6\:\s*(.+)$')
    re_org = re.compile('^org\:\s*(.+)$')
    outfp.write("network,org_id\n")
    for line in infp:
        if line == '\n':
            if route.get('route', None) is not None or route.get('route6', None) is not None:
                if route.get('route'):
                    network = route.get('route', '').strip()
                elif route.get('route6'):
                    network = route.get('route6', '').strip()
                else:
                    print('ALARM')
                    sys.exit(1)

                org_id = route.get('org', '').strip()
                s = str.format("%s,%s\n" % (network, org_id))
                outfp.write(s)
            route = {}
            continue

        elif line.startswith('route:'):
            p = re_route.match(line)
            if p:
                route['route'] = p.group(1)

        elif line.startswith('route6:'):
            p = re_route6.match(line)
            if p:
                route['route6'] = p.group(1)

        elif line.startswith('org:'):
            p = re_org.match(line)
            if p:
                route['org'] = p.group(1)

    infp.close()
    outfp.close()


def dump_organisation(input_file, output_file):
    infp = open(input_file, 'r', encoding='ISO-8859-1')
    outfp = open(output_file, 'wt', encoding='UTF8')

    logger.debug("Parsing %s -> %s ..." % (input_file, output_file))

    route = {}
    re_org_id = re.compile('^organisation\:\s*(.+)$')
    re_org_name = re.compile('^org-name\:\s*(.+)$')
    outfp.write("org_id,org_name\n")
    for line in infp:
        if line == '\n':
            if route.get('org_id', None) is not None:
                org_id = route.get('org_id', '').strip()
                org_name = route.get('org_name', '').strip()
                s = str.format("%s,%s\n" % (org_id, org_name))
                outfp.write(s)
            route = {}
            continue

        elif line.startswith('organisation:'):
            p = re_org_id.match(line)
            if p:
                route['org_id'] = p.group(1)

        elif line.startswith('org-name:'):
            p = re_org_name.match(line)
            if p:
                route['org_name'] = p.group(1)

    infp.close()
    outfp.close()


def pull_data():
    import requests
    import gzip
    import shutil
    import zipfile

    proxy = {
        "http": '',
        "https": ''
    }
    if "proxy" in settings and settings.get("proxy"):
        proxy['http'] = "http://%s" % settings['proxy']
        proxy['https'] = "https://%s" % settings['proxy']

    def ungzip(source_filepath, dest_filepath, block_size=65536):
        """ Распаковка gz архива """
        logger.debug("Unpack %s" % source_filepath)
        with gzip.open(source_filepath, 'rb') as s_file, open(dest_filepath, 'wb') as d_file:
            shutil.copyfileobj(s_file, d_file, block_size)

    def unzip(source_filepath, extract_to):
        """ Распаковка zip фрхива """
        logger.debug("Unpack %s" % source_filepath)
        with zipfile.ZipFile(source_filepath, 'r') as zip_ref:
            zip_ref.extractall(extract_to)

    def download_file(url, file_name):
        """ Скачивание файла """
        logger.debug("Downloading %s ..." % file_name)
        if not os.path.exists(source_tmp_path):
            os.makedirs(source_tmp_path)

        with open(os.path.join(source_tmp_path, file_name), 'wb') as out_stream:
            req = requests.get(url, stream=True, proxies=proxy, verify=False)
            for chunk in req.iter_content(1024):  # Куски по 1 КБ
                out_stream.write(chunk)

    def download_source():
        def download_ripe_source():
            logger.info("Start download RIPE source data")
            urls = [
                "https://ftp.ripe.net/ripe/dbase/split/ripe.db.organisation.gz",
                "https://ftp.ripe.net/ripe/dbase/split/ripe.db.route.gz",
                "https://ftp.ripe.net/ripe/dbase/split/ripe.db.route6.gz"
            ]
            for url in urls:
                file_name = url.split("/")[-1]

                download_file(url, file_name)
                ungzip(os.path.join(source_tmp_path, file_name),
                       os.path.join(source_tmp_path, file_name.replace(".gz", "")))
                os.remove(os.path.join(source_tmp_path, file_name))
            logger.info("Download RIPE source data [OK]")
            return

        def download_maxmind_source():
            logger.info("Start download MaxMind source data")
            license_key = settings['maxmind_license_key']
            d_files = {
                "GeoLite2-Country-CSV":
                    "https://download.maxmind.com/app/geoip_download?"
                    "edition_id=GeoLite2-Country-CSV&license_key={}&suffix=zip",
                "GeoLite2-City-CSV":
                    "https://download.maxmind.com/app/geoip_download?"
                    "edition_id=GeoLite2-City-CSV&license_key={}&suffix=zip"
            }
            for file_name, url in d_files.items():
                download_file(url.format(license_key), f"{file_name}.zip")
                unzip(os.path.join(source_tmp_path, f"{file_name}.zip"), os.path.join(source_tmp_path))
                os.remove(os.path.join(source_tmp_path, f"{file_name}.zip"))
                csv_files = ["GeoLite2-City-Blocks-IPv4.csv",
                             "GeoLite2-City-Blocks-IPv6.csv",
                             "GeoLite2-City-Locations-en.csv",
                             "GeoLite2-Country-Blocks-IPv4.csv",
                             "GeoLite2-Country-Blocks-IPv6.csv",
                             "GeoLite2-Country-Locations-en.csv"]
                for dirname, dirnames, filenames in os.walk(source_tmp_path):
                    if file_name not in dirname:
                        continue
                    for file in csv_files:
                        if file in os.listdir(os.path.join(dirname)):
                            logger.debug("Move %s to %s" % (file, os.path.join(source_tmp_path)))
                            if os.path.isfile(os.path.join(source_tmp_path, file)):
                                os.remove(os.path.join(source_tmp_path, file))
                            shutil.move(os.path.join(dirname, file), os.path.join(source_tmp_path))
                    shutil.rmtree(os.path.join(dirname))
            logger.info("Download MaxMind source data [OK]")
            return

        download_ripe_source()
        download_maxmind_source()

    def prepare_ripe_data():
        """ Подготовка данных из ripe """
        logger.info("Start prepare RIPE data")
        dump_routes(os.path.join(source_tmp_path, 'ripe.db.route'),
                    os.path.join(source_tmp_path, 'ripe_route_ipv4.csv'))
        dump_routes(os.path.join(source_tmp_path, 'ripe.db.route6'),
                    os.path.join(source_tmp_path, 'ripe_route_ipv6.csv'))
        dump_organisation(os.path.join(source_tmp_path, 'ripe.db.organisation'),
                          os.path.join(source_tmp_path, 'ripe_organisation.csv'))

        ripe_route_ipv4 = dd.read_csv(os.path.join(source_tmp_path, 'ripe_route_ipv4.csv'),
                                      usecols=["network", "org_id"],
                                      assume_missing=True)
        ripe_route_ipv4 = ripe_route_ipv4.drop_duplicates("network")
        # TODO:  пере-перепроверить коррекктность удаления дублей

        ripe_route_ipv6 = dd.read_csv(os.path.join(source_tmp_path, 'ripe_route_ipv6.csv'),
                                      usecols=["network", "org_id"],
                                      assume_missing=True)
        ripe_route_ipv6 = ripe_route_ipv6.drop_duplicates("network")

        ripe_organisation = dd.read_csv(os.path.join(source_tmp_path, 'ripe_organisation.csv'),
                                        usecols=["org_id", "org_name"])

        logger.debug("  Merging ripe_route and ripe_organisation for ipv4")
        result_ripe_org_ipv4 = ripe_route_ipv4.merge(ripe_organisation, on="org_id", how="left")
        result_ripe_org_ipv4.to_csv(os.path.join(processing_tmp_path, 'ripe_result_ipv4.csv'),
                                    single_file=True,
                                    columns=["network", "org_id", "org_name"], index=False)

        logger.debug("  Merging ripe_route and ripe_organisation for ipv6")
        result_ripe_org_ipv6 = ripe_route_ipv6.merge(ripe_organisation, on="org_id", how="left")
        result_ripe_org_ipv6.to_csv(os.path.join(processing_tmp_path, 'ripe_result_ipv6.csv'),
                                    single_file=True,
                                    columns=["network", "org_id", "org_name"], index=False)

        logger.info("Prepare RIPE data [OK]")

    def prepare_maxmind_data():
        """ Подготовка данных из maxmind """
        logger.info("Start prepare MaxMind data")
        # prepare city
        city_ipv4 = dd.read_csv(os.path.join(source_tmp_path, 'GeoLite2-City-Blocks-IPv4.csv'),
                                usecols=["network", "geoname_id"],
                                assume_missing=True)
        city_ipv6 = dd.read_csv(os.path.join(source_tmp_path, 'GeoLite2-City-Blocks-IPv6.csv'),
                                usecols=["network", "geoname_id"],
                                assume_missing=True)
        city_location = dd.read_csv(os.path.join(source_tmp_path, 'GeoLite2-City-Locations-en.csv'),
                                    usecols=["geoname_id", "city_name", "country_name"])

        # geoname_id иногда отсутствуют, заменяем, что бы не получить float
        city_ipv4['geoname_id'] = city_ipv4['geoname_id'].fillna(-1).astype(int)
        city_ipv6['geoname_id'] = city_ipv6['geoname_id'].fillna(-1).astype(int)

        logger.debug("  Merging route and location for city ipv4")
        result_city_ipv4 = city_ipv4.merge(city_location, on="geoname_id", how="left")
        result_city_ipv4.to_csv(os.path.join(processing_tmp_path, 'maxminde_city_ipv4.csv'),
                                single_file=True,
                                columns=["network", "city_name", "country_name"], index=False)

        logger.debug("  Merging route and location for city ipv6")
        result_city_ipv6 = city_ipv6.merge(city_location, on="geoname_id", how="left")
        result_city_ipv6.to_csv(os.path.join(processing_tmp_path, 'maxminde_city_ipv6.csv'),
                                single_file=True,
                                columns=["network", "city_name", "country_name"], index=False)
        # prepare country
        country_ipv4 = dd.read_csv(os.path.join(source_tmp_path, 'GeoLite2-Country-Blocks-IPv4.csv'),
                                   usecols=["network", "geoname_id"],
                                   assume_missing=True)
        country_ipv6 = dd.read_csv(os.path.join(source_tmp_path, 'GeoLite2-Country-Blocks-IPv6.csv'),
                                   usecols=["network", "geoname_id"],
                                   assume_missing=True)
        country_location = dd.read_csv(os.path.join(source_tmp_path, 'GeoLite2-Country-Locations-en.csv'),
                                       usecols=["geoname_id", "country_name"])

        # geoname_id иногда отсутствуют, заменяем, что бы не получить float
        country_ipv4['geoname_id'] = country_ipv4['geoname_id'].fillna(-1).astype(int)
        country_ipv6['geoname_id'] = country_ipv6['geoname_id'].fillna(-1).astype(int)

        # Удаление из датафрейма country записи, которые есть в city
        country_ipv4 = country_ipv4[~country_ipv4.network.isin(city_ipv4.compute().network)].reset_index(drop=True)
        country_ipv6 = country_ipv6[~country_ipv6.network.isin(city_ipv6.compute().network)].reset_index(drop=True)

        logger.debug("  Merging route and location for country ipv4")
        result_country_ipv4 = country_ipv4.merge(country_location, on="geoname_id", how="left")
        result_country_ipv4.to_csv(os.path.join(processing_tmp_path, 'maxminde_country_ipv4.csv'),
                                   single_file=True,
                                   columns=["network", "country_name"], index=False)

        logger.debug("  Merging route and location for country ipv6")
        result_country_ipv6 = country_ipv6.merge(country_location, on="geoname_id", how="left")
        result_country_ipv6.to_csv(os.path.join(processing_tmp_path, 'maxminde_country_ipv6.csv'),
                                   single_file=True,
                                   columns=["network", "country_name"], index=False)

        logger.info("Start prepare MaxMind data [OK]")

    def merge_data():
        """ Объединение данных из ripe и maxmind"""
        logger.info("Start merging RIPE and MAxMind data")
        mm_city_ipv4 = dd.read_csv(os.path.join(processing_tmp_path, 'maxminde_city_ipv4.csv'),
                                   usecols=["network", "city_name", "country_name"], assume_missing=True)
        mm_city_ipv6 = dd.read_csv(os.path.join(processing_tmp_path, 'maxminde_city_ipv6.csv'),
                                   usecols=["network", "city_name", "country_name"], assume_missing=True)
        mm_country_ipv4 = dd.read_csv(os.path.join(processing_tmp_path, 'maxminde_country_ipv4.csv'),
                                      usecols=["network", "country_name"], assume_missing=True)
        mm_country_ipv6 = dd.read_csv(os.path.join(processing_tmp_path, 'maxminde_country_ipv6.csv'),
                                      usecols=["network", "country_name"], assume_missing=True)
        organisations_ipv4 = dd.read_csv(os.path.join(processing_tmp_path, 'ripe_result_ipv4.csv'),
                                         usecols=["network", "org_id", "org_name"])
        organisations_ipv6 = dd.read_csv(os.path.join(processing_tmp_path, 'ripe_result_ipv6.csv'),
                                         usecols=["network", "org_id", "org_name"])

        logger.debug("  Merging MaxMind city and RIPE data for ipv4")
        result = mm_city_ipv4.merge(organisations_ipv4, on="network", how="left")
        result.to_csv(os.path.join(processing_tmp_path, 'merged_city_ipv4.csv'),
                      single_file=True,
                      columns=["network", "city_name", "country_name", "org_id", "org_name"], index=False)

        logger.debug("  Merging MaxMind city and RIPE data for ipv6")
        result = mm_city_ipv6.merge(organisations_ipv6, on="network", how="left")
        result.to_csv(os.path.join(processing_tmp_path, 'merged_city_ipv6.csv'),
                      single_file=True,
                      columns=["network", "city_name", "country_name", "org_id", "org_name"], index=False)

        logger.debug("  Merging MaxMind country and RIPE data for ipv4")
        result = mm_country_ipv4.merge(organisations_ipv4, on="network", how="left")
        result.to_csv(os.path.join(processing_tmp_path, 'merged_country_ipv4.csv'),
                      single_file=True,
                      columns=["network", "country_name", "org_id", "org_name"], index=False)

        logger.debug("  Merging MaxMind country and RIPE data for ipv6")
        result = mm_country_ipv6.merge(organisations_ipv6, on="network", how="left")
        result.to_csv(os.path.join(processing_tmp_path, 'merged_country_ipv6.csv'),
                      single_file=True,
                      columns=["network", "country_name", "org_id", "org_name"], index=False)
        logger.info("Merging RIPE and MAxMind data [OK]")

    def check_data_radix_tree():
        """ Проверяем, не перекрывает ли одна подсеть другую """

        def is_network_in_tree(rtree, network):
            """ Проверяем, есть ли в дереве перекрывающие сети (включающие эту)"""
            res_list = rtree.search_covering(network)
            if rtree.search_covering(network):
                return True, [n.prefix for n in res_list]
            return False, res_list

        def filter_covered_network(input_file, output_file):
            logger.info("Start check %s for covered networks on radix tree" % input_file)
            rt = radix.Radix()
            with open(os.path.join(processing_tmp_path, input_file), "r") as inf, \
                    open(os.path.join(processing_tmp_path, output_file), "w") as ouf:
                r = csv.reader(inf)
                w = csv.writer(ouf, delimiter=',')
                titles = next(r)
                w.writerow(titles)
                covered_networks = 0
                for row in r:
                    res, rtl = is_network_in_tree(rt, row[0])
                    if res and row[0]:
                        logger.warning("    FAIL %s - %s" % (row, rtl))
                        covered_networks += 1
                    else:
                        rt.add(row[0])
                        w.writerow(row)
                logger.debug("  Number detected covered networks %s" % covered_networks)
            logger.debug("  Delete %s" % os.path.join(processing_tmp_path, input_file))
            os.remove(os.path.join(processing_tmp_path, input_file))
            logger.info("Checked [OK] - generated %s" % os.path.join(processing_tmp_path, output_file))

        files = [
            {"input_file": "merged_country_ipv4.csv", "output_file": "result_country_ipv4.csv"},
            {"input_file": "merged_country_ipv6.csv", "output_file": "result_country_ipv6.csv"},
            {"input_file": "merged_city_ipv4.csv", "output_file": "result_city_ipv4.csv"},
            {"input_file": "merged_city_ipv6.csv", "output_file": "result_city_ipv6.csv"}
        ]
        for file in files:
            filter_covered_network(**file)

    download_source()
    prepare_ripe_data()
    prepare_maxmind_data()
    merge_data()
    check_data_radix_tree()


def push_data():
    # TODO: хранение сети в бд https://www.postgresql.org/docs/9.1/datatype-net-types.html
    db = DB(settings['postgresql'])
    load_id = db.nextval("geo")

    def upload_data(table_name, csv_data_file_path):
        """ выгрузка данных из csv в БД """

        def send2db(_batch, _batch_len):
            query = ""
            if table_name == "geoinfo_city":
                query = (f"INSERT INTO geoinfo_city(network,city,country,org,description,load_id)"
                         f"VALUES (%s,%s,%s,%s,%s,{load_id})")
            elif table_name == "geoinfo_country":
                query = (f"INSERT INTO geoinfo_country(network,country,org,description,load_id)"
                         f"VALUES (%s,%s,%s,%s,{load_id})")
            try:
                db.insert_to_db(query, _batch, _batch_len)
            except Exception as err:
                logger.error(err)
                return False
            return True

        def sql_normalise(line: list):
            for i, w in enumerate(line):
                if w == '':
                    line[i] = None
            return line

        with open(csv_data_file_path, "r") as csvfile:
            x = csv.reader(csvfile)
            next(x)  # skip titles line
            batch_len = 1000
            batch = []
            ok = 0
            logger.info("Start upload data from %s to db [%s]", csv_data_file_path, table_name)

            for write in x:
                batch.append(sql_normalise(write))
                if len(batch) < batch_len:
                    continue
                else:
                    if send2db(_batch=batch, _batch_len=batch_len):
                        ok += len(batch)
                    batch = []
            if send2db(_batch=batch, _batch_len=batch_len):
                ok += len(batch)
            if ok == 0:
                logger.error("  Sent to db [%s] [ %s ] writes from %s" % (ok, table_name, csv_data_file_path))
            else:
                logger.info("   Sent to db [%s] [ %s ] writes from %s" % (ok, table_name, csv_data_file_path))

            logger.info("Upload data from %s to db [OK]", csv_data_file_path)

    upload_data("geoinfo_country", os.path.join(processing_tmp_path, 'result_country_ipv4.csv'), )
    upload_data("geoinfo_country", os.path.join(processing_tmp_path, 'result_country_ipv6.csv'), )
    upload_data("geoinfo_city", os.path.join(processing_tmp_path, 'result_city_ipv4.csv'), )
    upload_data("geoinfo_city", os.path.join(processing_tmp_path, 'result_city_ipv6.csv'), )
    db.close_connection()


def clear_deprecated_data():
    db = DB(settings['postgresql'])
    db.clear_deprecated_data("geoinfo_country", settings.get('limit_storage_pull_version'))
    db.clear_deprecated_data("geoinfo_city", settings.get('limit_storage_pull_version'))
    db.close_connection()


default_args = {
    'start_date': datetime(2020, 6, 26),
    'max_active_runs': 1,
    'retries': 0,
    'retry_delay': timedelta(seconds=30)
}

with DAG(
        dag_id='geoinfo',
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        catchup=False
) as dag:
    pull = PythonOperator(
        task_id='geo_download',
        python_callable=pull_data,
        dag=dag
    )
    push = PythonOperator(
        task_id='geo_upload',
        python_callable=push_data,
        dag=dag
    )
    clear = PythonOperator(
        task_id='geo_clear_deprecated_data',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        python_callable=clear_deprecated_data,
        dag=dag
    )
    pull >> push >> clear
