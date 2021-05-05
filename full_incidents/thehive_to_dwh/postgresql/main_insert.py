import logging

from sqlalchemy.orm import Session

import datetime

from exceptions import PostgreSQLFieldMissingError
from full_incidents.thehive_to_dwh.postgresql.insert import is_exist_rule, is_exists_usecase, \
    select_organization_short_name, select_eventsourcecategory, select_vendor_solutions, select_severity_level, \
    insert_datapayload, select_or_insert_entity, select_or_insert_asset, insert_interaction_description, \
    select_or_insert_natinfo
from full_incidents.thehive_to_dwh.postgresql.models import Incident, Counterpart


def insert_incident(root_id, tags: list, incident_id: str, rule_name: str, usecase_id: str, raw: str, subsys: str,
                    eventsource_id: str, eventsourcecategory: str,
                    eventsourcevendor: str, eventsourcetitle: str,
                    organization_id: str,
                    event_time: int, detected_time,
                    subject_category: str, subject_privileges: str, subject_groupname: str, subject_domain: str,
                    subject_name, subject_version, object_category: str, object_path: str, object_groupname: str,
                    object_domain, object_vendor, object_version, object_state, object_property, object_name,
                    object_value, rawids: str, identity_hash: str,
                    events_source_ip, events_source_hostname,
                    destination_hostname, destination_ip, destination_port, destination_enrichment_isnetworklocal,
                    source_hostname, source_ip, source_port, source_enrichment_isnetworklocal,
                    collector_hostname, collector_ip,
                    protocol, action, direction, reason, status,
                    bytes_total, bytes_out, packets_total, packets_in, packets_out, interface, msgid, origintime,
                    recvfile, tcpflag, time, aux1, aux2, aux3, aux4, aux5, aux6, aux7, aux8, aux9, aux10,
                    natinfo_ip, natinfo_port, natinfo_hostname,
                    session: Session, asset_numbers):
    """
    Insert into main table. The values are formed after inserts and selects in other tables, most often reference ones.
    """

    incident = Incident()
    incident.inc_id = incident_id
    incident.identity_hash = identity_hash
    incident.destination_port = destination_port
    incident.source_port = source_port
    incident.root_id = root_id

    # checking for rule, usecase and organization in the database
    if not is_exist_rule(rule_name, session):
        raise PostgreSQLFieldMissingError("Incident with id %s NOT uploaded, because rule_name %s is missing in "
                                          "PostgreSQL", incident_id, rule_name)
    incident.rule_id = rule_name

    if not is_exists_usecase(usecase_id, session):
        raise PostgreSQLFieldMissingError("Incident with id %s NOT uploaded, because usecase_id %s is missing in "
                                          "PostgreSQL", incident_id, usecase_id)
    incident.usecase_id = usecase_id

    if not select_organization_short_name(organization_id, session):
        raise PostgreSQLFieldMissingError("Incident with id %s NOT uploaded, because organizations with short_name=%s "
                                          "is missing in PostgreSQL", incident_id, organization_id)
    incident.organization_id = organization_id

    if eventsourcecategory:
        eventsourcecategory_id = select_eventsourcecategory(eventsourcecategory, session)
        if eventsourcecategory_id:
            incident.eventsourcecategory_id = eventsourcecategory_id

    if eventsourcevendor:
        vendor_solutions_id = select_vendor_solutions(
            vendor_name=eventsourcevendor, title=eventsourcetitle, session=session
        )
        if vendor_solutions_id:
            incident.vendor_id = vendor_solutions_id

    severity_level_id = select_severity_level(tags, incident_id, session)
    if severity_level_id:
        incident.severity_level_id = severity_level_id

    if raw:
        incident.raw = raw
    if subsys:
        incident.subsys = subsys
    if eventsource_id:
        incident.inputId = eventsource_id

    rawids = rawids.split("; ")
    datapayload_id = insert_datapayload(
        rawids=rawids,
        bytes_total=bytes_total,
        bytes_out=bytes_out,
        packets_total=packets_total,
        packets_in=packets_in,
        packets_out=packets_out,
        interface=interface,
        msgid=msgid,
        origintime=origintime,
        recvfile=recvfile,
        tcpflag=tcpflag,
        time=time,
        aux1=aux1,
        aux2=aux2,
        aux3=aux3,
        aux4=aux4,
        aux5=aux5,
        aux6=aux6,
        aux7=aux7,
        aux8=aux8,
        aux9=aux9,
        aux10=aux10,
        session=session
    )
    incident.data = datapayload_id

    incident.event_time = datetime.datetime.utcfromtimestamp(event_time/1000)
    incident.detect_time = datetime.datetime.utcfromtimestamp(detected_time/1000)

    subject_counterpart_id = None
    if subject_category and subject_category != "UnknownCounterpart":
        subject_counterpart_id = session.query(Counterpart.id).filter(Counterpart.counter_name == subject_category).\
            first()
        if subject_counterpart_id is None:
            logging.warning("Subject with category=%s is missing in PostgreSQL", subject_category)

    subject_id = None
    if subject_privileges or subject_groupname or subject_domain or subject_name or subject_version or \
            subject_counterpart_id:
        subject_id = select_or_insert_entity(
            session=session, privileges=subject_privileges, groupname=subject_groupname, domain=subject_domain,
            name=subject_name, version=subject_version, counterpart_subject_id=subject_counterpart_id
        )
    incident.subject_id = subject_id

    object_counterpart_id = None
    if object_category and object_category != "UnknownCounterpart":
        object_counterpart_id = session.query(Counterpart.id).filter(Counterpart.counter_name == object_category).\
            first()
        if object_counterpart_id is None:
            logging.warning("Object with category=%s is missing in PostgreSQL", object_category)

    object_id = None
    if object_path or object_groupname or object_domain or object_vendor or object_version or object_state \
            or object_property or object_name or object_value or object_counterpart_id:
        object_id = select_or_insert_entity(
            session=session, path=object_path, groupname=object_groupname, domain=object_domain, vendor=object_vendor,
            version=object_version, state=object_state, property=object_property, name=object_name, value=object_value,
            counterpart_object_id=object_counterpart_id
        )
    incident.object_id = object_id

    if events_source_hostname or events_source_ip:
        incident.event_source = select_or_insert_asset(
            session=session, hostname=events_source_hostname, ip=events_source_ip, organization=organization_id,
            asset_numbers=asset_numbers
        )

    if destination_hostname or destination_ip:
        destination_organization = organization_id
        if destination_enrichment_isnetworklocal is False:
            destination_organization = None
        incident.destination_id = select_or_insert_asset(
            session=session, hostname=destination_hostname, ip=destination_ip, organization=destination_organization,
            asset_numbers=asset_numbers
        )

    if source_hostname or source_ip:
        source_organization = organization_id
        if source_enrichment_isnetworklocal is False:
            source_organization = None
        incident.source_id = select_or_insert_asset(
            session=session, hostname=source_hostname, ip=source_ip, organization=source_organization,
            asset_numbers=asset_numbers
        )

    if collector_hostname or collector_ip:
        incident.collectorlocationhost_id = select_or_insert_asset(
            session=session, hostname=collector_hostname, ip=collector_ip, organization=organization_id,
            asset_numbers=asset_numbers
        )

    interaction_columns = [protocol, action, direction, reason, status]
    if any([value is not None for value in interaction_columns]):
        interaction_description_id = insert_interaction_description(
            protocol=protocol, action=action, reason=reason, direction=direction, status=status, session=session
        )
        incident.interaction = interaction_description_id

    if natinfo_ip or natinfo_port or natinfo_hostname:
        natinfo_id = select_or_insert_natinfo(
            session=session, ip=natinfo_ip, port=natinfo_port, hostname=natinfo_hostname
        )
        incident.nat_id = natinfo_id

    session.add(incident)
    session.flush()
    logging.info("Incident with id %s uploaded to PostgreSQL", incident_id)
