# geo_pullpusher
Получает geo данные из ripe и maxmind, складывает в postgresql 
requirements:
* py-radix == 0.10.0
* psycopg2-binary == 2.8.5
* dask == 2.18.1
* raven == 6.10.0

Variable in airflow: "geoetl_settings"
```
{
  "geoinfo_settings": {
    "sentry_url": null, 
    "logging": {
        "basic_level": "DEBUG", 
        "term_level": "DEBUG"
        }, 
    "proxy": null, 
    "maxmind_license_key": "", 
    "postgresql": {
        "host": "", 
        "port": , 
        "user": "", 
        "password": "", 
        "database": ""
        }, 
    "limit_storage_pull_version": 14
  }
} 	
```
### Запросы  для подготовки бд
```
querys = {
    "table_geoinfo_city": "create table geoinfo_city("
                          "network varchar not null,"
                          "city varchar,"
                          "country varchar,"
                          "org varchar,"
                          "description varchar,"
                          "load_id int);",
    "table_geoinfo_country": "create table geoinfo_country("
                             "network varchar not null,"
                             "country varchar,"
                             "org varchar,"
                             "description varchar,"
                             "load_id int);",
    "sequence": "create sequence geo START 1;"
}
```