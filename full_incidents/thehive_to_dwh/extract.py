import logging
from datetime import datetime, timedelta

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

logger = logging.getLogger('thehive_to_dwh')


def extract(elasticsearch_host: str, elasticsearch_port: int, index: str, extraction_start_date):
    client = Elasticsearch(f"{elasticsearch_host}:{elasticsearch_port}")
    search_object = Search(using=client, index=index)
    start_time_in_second = extraction_start_date.timestamp()
    start_time = int(start_time_in_second * 1000)
    end_time_in_second = (extraction_start_date + timedelta(hours=24)).timestamp()
    end_time = int(end_time_in_second * 1000)  # converting to milliseconds
    query_object = search_object.filter(
        "range", **{"startDate": {"gte": start_time, "lte": end_time, "format": "epoch_millis"}}
    )
    query_object = query_object.query("match", tags="FINAL")
    hits_count = query_object.count()
    query_object = query_object.extra(size=hits_count)
    response = query_object.execute()
    logging.info("Found %d hits in TheHive in time %s to %s", hits_count, datetime.utcfromtimestamp(start_time_in_second)
                 .strftime("%m-%d-%Y, %H:%M:%S"), datetime.utcfromtimestamp(end_time_in_second)
                 .strftime("%m-%d-%Y, %H:%M:%S"))

    return response
