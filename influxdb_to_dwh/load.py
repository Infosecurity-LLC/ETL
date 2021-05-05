import logging
from typing import List, Generator, Tuple

from influxdb.resultset import ResultSet

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, VARCHAR
from sqlalchemy.orm import Session

from exceptions import InfluxDBFieldMissingError

logger = logging.getLogger('influxdb_to_dwh')

Base = declarative_base()


class FlowMetrics(Base):
    __tablename__ = 'flow_metrics'
    id = Column(Integer, autoincrement=True, primary_key=True)
    dev_type_id = Column(VARCHAR)
    cnt = Column(Integer)
    org_id = Column(VARCHAR)
    host_name = Column(VARCHAR)
    time = Column(VARCHAR)


class Organizations(Base):
    __tablename__ = 'organizations'
    id = Column(Integer, autoincrement=True, primary_key=True)
    short_name = Column(VARCHAR)


def select_or_insert_flow_metrics(session: Session, host, organization, devtype, count, time, influxdb_number):
    """
    Check if the database contains a line with nat fields from the incident. If there is, we get the id of the
    record, if not, we create a new row in the database and also get its id. The resulting id is written to the incident
    table.
    """
    flow_metrics_id = session.query(FlowMetrics.id).filter(
        FlowMetrics.host_name == host,
        FlowMetrics.org_id == organization,
        FlowMetrics.dev_type_id == devtype
    ).first()
    if flow_metrics_id:
        session.query(FlowMetrics).filter(FlowMetrics.id == flow_metrics_id).update(
            {"time": time, "cnt": count})
        influxdb_number["updated"] += 1
        return

    flow_metrics = FlowMetrics()
    flow_metrics.dev_type_id = devtype
    flow_metrics.cnt = count
    flow_metrics.host_name = host
    flow_metrics.org_id = organization
    flow_metrics.time = time
    session.add(flow_metrics)
    session.flush()
    influxdb_number["inserted"] += 1


def load(org_client_metrics: List[Tuple[Tuple[str, dict], Generator[ResultSet, None, None]]], start_date,
         session: Session):
    """
    Загрузка метрик в DWH в таблицу flow_metrics. Загружаются поля time, count, devtype, host, organization.
    Time соответствует времени запуска дага - 10 мин. Count берётся в списке словарей по ключу time.
    Если такого time в списке нет, то пишется лог и строка не записывается.
    """
    influxdb_number = {"inserted": 0,
                       "updated": 0}
    execution_date_str = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    prepared_organizations = session.query(Organizations.short_name).all()
    organizations = [org[0] for org in prepared_organizations]
    try:
        for org_client_metric in org_client_metrics:
            for key_tuple, values_generator in org_client_metric:
                _, main_fields = key_tuple
                if main_fields["client_org"] not in organizations:
                    logging.warning("In dwh in organizations table short_name=%s is missing", main_fields["client_org"])
                    break
                aggregation_fields = list(values_generator)

                # попытка получить поля
                time = start_date.strftime("%Y-%m-%d %H:%M:%S")
                try:
                    count = [aggregation_field["non_negative_derivative"] for aggregation_field in aggregation_fields
                             if aggregation_field["time"] == execution_date_str]
                    try:
                        count = count[0]
                    except IndexError:
                        logging.warning("Row for %s time for %s organization is missing", execution_date_str,
                                        main_fields["client_org"])
                        continue
                    devtype = main_fields["devtype"]
                    host = main_fields["source"]
                    organization = main_fields["client_org"]
                except (KeyError, TypeError) as e:
                    raise InfluxDBFieldMissingError(f"Required field for InfluxDB is missing: {e}")

                select_or_insert_flow_metrics(
                    session=session, host=host, organization=organization, devtype=devtype, count=count, time=time,
                    influxdb_number=influxdb_number
                )
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        logging.info("Inserted client_metrics number: %d", influxdb_number["inserted"])
        logging.info("Updated client_metrics number: %d", influxdb_number["updated"])
        session.close()
