import logging
import os

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

logger = logging.getLogger('siem_archiver')


def prepare_configs(work_dir, streamer_configs):
    if not os.path.exists(work_dir):
        os.mkdir(work_dir)
    for type in streamer_configs:
        
        conf_file = "%s/archiver_%s.conf" % (work_dir, type)
        log4j_file = "%s/archiver_%s.log4j.properties" % (work_dir, type)

        with open(conf_file, 'w') as out:
            out.write(streamer_configs[type]['conf'] + '\n')
        with open(log4j_file, 'w') as out:
            out.write(streamer_configs[type]['log4j'] + '\n')


def create_spark_submit_operator(dag, default_dag_rules, dag_config, work_dir, spark_submit_operator_type):
    try:
        jar_file = dag_config["JAR_FILE"]
        execute_class = dag_config["EXECUTE_CLASS"]
        num_executor = dag_config["NUM_EXECUTOR"]
        executor_cores = dag_config["EXECUTOR_CORES"]
        executor_memory = dag_config["EXECUTOR_MEMORY"]
        driver_memory = dag_config["DRIVER_MEMORY"]
        principal = dag_config["PRINCIPAL"]
        keytab = dag_config["KEYTAB"]
    except (ValueError, KeyError) as e:
        logging.error(str(e))
        raise e

    conf_file = "%s/archiver_%s.conf" % (work_dir, spark_submit_operator_type)
    log4j_file = "%s/archiver_%s.log4j.properties" % (work_dir, spark_submit_operator_type)

    spark_conf = {
        'spark.network.timeout': '800s',
        'spark.executor.heartbeatInterval': '60s',
        'spark.executor.extraJavaOptions': '-Djava.security.auth.login.config=./tech_siem.jaas -Dlog4j.configuration=archiver_%s.log4j.properties' % (spark_submit_operator_type),
        'spark.driver.extraJavaOptions': "-Dlog4j.configuration=archiver_%s.log4j.properties -Dconfig.file=archiver_%s.conf -Djava.security.auth.login.config=./tech_siem.jaas" % (spark_submit_operator_type, spark_submit_operator_type),
    }
    
    res = SparkSubmitOperator(
        task_id="archiver_%s" % spark_submit_operator_type,
        name="archiver_%s" % spark_submit_operator_type,
        application=jar_file,
        conf=spark_conf,
        java_class=execute_class,
        executor_cores=executor_cores,
        executor_memory=executor_memory,
        num_executors=num_executor,
        driver_memory=driver_memory,
        principal=principal,
        keytab=keytab,
        depends_on_past=default_dag_rules['depends_on_past'],
        wait_for_downstream=default_dag_rules['wait_for_downstream'],
        trigger_rule=default_dag_rules['trigger_rule'],
        files="%s,%s,/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml,/etc/hbase/conf/hbase-site.xml" %
              (conf_file, log4j_file),
        application_args=["{{ execution_date }}"],
        dag=dag)
    return res
