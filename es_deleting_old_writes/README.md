# es_deleting_old_writes
Удаляет устаревшие данные из индексов еластика:

Variable in airflow: "es_deleting_old_writes_settings"
```
{
  "es_deleting_old_writes_settings": {
    "sentry_url": null,
    "logging": {
      "basic_level": "DEBUG",
      "term_level": "DEBUG"
    },
    "es_connection_id": "elasticsearch",
    "instructions": [
      {"index": "incident", "time_field_name": "@timestamp", "lt": "now-7d/d"}
    ]
  }
}
```
es_connection_id - указывает на id подключения в airflow (заполняется в ui airflow)
instructions - список словарей, в которых указывается индекс, в котором необходимо провести чистку, поле со временем, время старше которого все события должны быть удалены 