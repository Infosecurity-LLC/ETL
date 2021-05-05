import hashlib
import logging

from sqlalchemy.orm import Session

from exceptions import ElasticSearchFieldMissingError
from full_incidents.thehive_to_dwh.postgresql.delete import delete_incident
from full_incidents.thehive_to_dwh.postgresql.main_insert import insert_incident
from full_incidents.thehive_to_dwh.postgresql.models import Incident

logger = logging.getLogger('thehive_to_dwh')


def required_fields_validation(incident_id, rule_id, usecase_id, organization_id, event_time, collector_hostname,
                               root_id):
    if incident_id is None:
        raise ElasticSearchFieldMissingError("Incident NOT uploaded, because required field incident_id is missing")
    error_string = "Incident with id %s NOT uploaded, because required field %s is missing"
    if rule_id is None:
        raise ElasticSearchFieldMissingError(error_string, incident_id, "rule_id")
    if usecase_id is None:
        raise ElasticSearchFieldMissingError(error_string, incident_id, "usecase_id")
    if organization_id is None:
        raise ElasticSearchFieldMissingError(error_string, incident_id, "organization_id")
    if event_time is None:
        raise ElasticSearchFieldMissingError(error_string, incident_id, "event_time")
    if collector_hostname is None:
        raise ElasticSearchFieldMissingError(error_string, incident_id, "collector_hostname")
    if root_id is None:
        raise ElasticSearchFieldMissingError(error_string, incident_id, "root_id")


def transform_load(extract_objects, session: Session):
    """
    Cycle for all incidents from elastic, extraction and mapping, field validation
    """
    incidents = extract_objects["hits"]["hits"]
    upload_incidents = 0
    dublicated_incidents = 0
    asset_numbers = {
        "inserted": 0,
        "selected": 0
    }
    try:
        for incident in incidents:
            extract_incident = incident["_source"].to_dict()

            root_id = incident["_id"]
            tags = incident["_source"]["tags"]

            incident_id = extract_incident['customFields'].get("id", {}).get("string", None)
            rule_id = extract_incident["customFields"].get("correlationRuleName", {}).get("string", None)
            usecase_id = extract_incident["customFields"].get("usecaseId", {}).get("string", None)
            rawids = extract_incident["customFields"].get("correlationEventDataRawIds", {}).get("string", None)
            raw = extract_incident["customFields"].get("raw", {}).get("string", None)
            subsys = extract_incident["customFields"].get("correlationEventEventSourceSubsys", {}).get("string", None)
            eventsource_id = extract_incident["customFields"].get("correlationEventEventSourceId", {}).\
                get("string", None)

            eventsourcecategory = extract_incident["customFields"].get("correlationEventEventSourceCategory", {}).\
                get("string", None)

            eventsourcevendor = extract_incident["customFields"].get("correlationEventEventSourceVendor", {}).\
                get("string", None)
            eventsourcetitle = extract_incident["customFields"].get("correlationEventEventSourceTitle", {}).\
                get("string", None)

            organization_id = extract_incident["customFields"].get("correlationEventCollectorOrganization", {}).\
                get("string", None)

            event_time = extract_incident["customFields"].get("correlationEventEventTime", {}).get("date", None)
            detected_time = extract_incident["customFields"].get("detectedTime", {}).get("date", None)

            subject_category = extract_incident["customFields"].get("correlationEventSubjectCategory", {}).\
                get("string", None)
            subject_privileges = extract_incident["customFields"].get("correlationEventSubjectPrivileges", {}).\
                get("string", None)
            subject_groupname = extract_incident["customFields"].get("correlationEventSubjectGroup", {}).\
                get("string", None)
            subject_domain = extract_incident["customFields"].get("correlationEventSubjectDomain", {}).\
                get("string", None)
            subject_name = extract_incident["customFields"].get("correlationEventSubjectName", {}).get("string", None)
            subject_version = extract_incident["customFields"].get("correlationEventSubjectVersion", {}).\
                get("string", None)

            object_category = extract_incident["customFields"].get("correlationEventObjectCategory", {}).\
                get("string", None)
            object_path = extract_incident["customFields"].get("correlationEventObjectPath", {}).get("string", None)
            object_groupname = extract_incident["customFields"].get("correlationEventObjectGroup", {}).\
                get("string", None)
            object_domain = extract_incident["customFields"].get("correlationEventObjectDomain", {}).get("string", None)
            object_vendor = extract_incident["customFields"].get("correlationEventObjectVendor", {}).get("string", None)
            object_version = extract_incident["customFields"].get("correlationEventObjectVersion", {}).\
                get("string", None)
            object_state = extract_incident["customFields"].get("correlationEventObjectState", {}).get("string", None)
            object_property = extract_incident["customFields"].get("correlationEventObjectProperty", {}).\
                get("string", None)
            object_name = extract_incident["customFields"].get("correlationEventObjectName", {}).get("string", None)
            object_value = extract_incident["customFields"].get("correlationEventObjectValue", {}).get("string", None)

            events_source_hostname = extract_incident["customFields"].\
                get("correlationEventEventSourceLocationHostname", {}).get("string", None)
            events_source_ip = extract_incident["customFields"].get("correlationEventEventSourceLocationIp", {}).\
                get("string", None)

            destination_hostname = extract_incident["customFields"].get("correlationEventDestinationHostname", {}).\
                get("string", None)
            destination_ip = extract_incident["customFields"].get("correlationEventDestinationIp", {}).\
                get("string", None)
            destination_port = extract_incident["customFields"].get("correlationEventDestinationPort", {}).\
                get("number", None)
            destination_enrichment_isnetworklocal = extract_incident["customFields"].\
                get("correlationEventDestinationEnrichmentIsNetworkLocal", {}).get("string", None)

            source_hostname = extract_incident["customFields"].get("correlationEventSourceHostname", {}).\
                get("string", None)
            source_ip = extract_incident["customFields"].get("correlationEventSourceIp", {}).get("string", None)
            source_port = extract_incident["customFields"].get("correlationEventSourcePort", {}).get("number", None)
            source_enrichment_isnetworklocal = extract_incident["customFields"].\
                get("correlationEventSourceEnrichmentIsNetworkLocal", {}).get("string", None)

            collector_hostname = extract_incident["customFields"].get("correlationEventCollectorLocationHostname", {}).\
                get("string", None)
            collector_ip = extract_incident["customFields"].get("correlationEventCollectorLocationIp", {}).\
                get("string", None)

            protocol = extract_incident["customFields"].get("correlationEventInteractionProtocol", {}).\
                get("string", None)
            action = extract_incident["customFields"].get("correlationEventInteractionAction", {}).get("string", None)
            direction = extract_incident["customFields"].get("correlationEventInteractionDirection", {}).\
                get("string", None)
            reason = extract_incident["customFields"].get("correlationEventInteractionReason", {}).\
                get("string", None)
            status = extract_incident["customFields"].get("correlationEventInteractionStatus", {}).get("string", None)

            bytes_total = extract_incident["customFields"].get("correlationEventDataBytes_total", {}).\
                get("string", None)
            bytes_out = extract_incident["customFields"].get("correlationEventDataBytes_out", {}).get("string", None)
            packets_total = extract_incident["customFields"].get("correlationEventDataPackets_total", {}).\
                get("string", None)
            packets_in = extract_incident["customFields"].get("correlationEventDataPackets_in", {}).get("string", None)
            packets_out = extract_incident["customFields"].get("correlationEventDataPackets_out", {}).\
                get("string", None)
            interface = extract_incident["customFields"].get("correlationEventDataInterface", {}).get("string", None)
            msgid = extract_incident["customFields"].get("correlationEventDataMsgId", {}).get("string", None)
            origintime = extract_incident["customFields"].get("correlationEventDataOriginTime", {}).get("date", None)
            recvfile = extract_incident["customFields"].get("correlationEventDataRecvFile", {}).get("string", None)
            tcpflag = extract_incident["customFields"].get("correlationEventDataTcpFlag", {}).get("number", None)
            time = extract_incident["customFields"].get("correlationEventDataTime", {}).get("date", None)
            aux1 = extract_incident["customFields"].get("correlationEventDataAux1", {}).get("string", None)
            aux2 = extract_incident["customFields"].get("correlationEventDataAux2", {}).get("string", None)
            aux3 = extract_incident["customFields"].get("correlationEventDataAux3", {}).get("string", None)
            aux4 = extract_incident["customFields"].get("correlationEventDataAux4", {}).get("string", None)
            aux5 = extract_incident["customFields"].get("correlationEventDataAux5", {}).get("string", None)
            aux6 = extract_incident["customFields"].get("correlationEventDataAux6", {}).get("string", None)
            aux7 = extract_incident["customFields"].get("correlationEventDataAux7", {}).get("string", None)
            aux8 = extract_incident["customFields"].get("correlationEventDataAux8", {}).get("string", None)
            aux9 = extract_incident["customFields"].get("correlationEventDataAux9", {}).get("string", None)
            aux10 = extract_incident["customFields"].get("correlationEventDataAux10", {}).get("string", None)

            nat_ip_es = extract_incident["customFields"].get("correlationEventSourceNatIp", {}).get("string", None)
            nat_ip_des = extract_incident["customFields"].get("correlationEventDestinationNatIp", {}).get("string", None)
            natinfo_ip = nat_ip_es or nat_ip_des
            nat_port_es = extract_incident["customFields"].get("correlationEventSourceNatPort", {}).get("number", None)
            nat_port_des = extract_incident["customFields"].get("correlationDestinationNatPort", {}).get("number", None)
            natinfo_port = nat_port_es or nat_port_des
            nat_hostname_es = extract_incident["customFields"].get("correlationEventSourceNatHostname", {}).\
                get("string", None)
            nat_hostname_des = extract_incident["customFields"].get("correlationDestinationNatHostname", {}). \
                get("string", None)
            natinfo_hostname = nat_hostname_es or nat_hostname_des

            required_fields_validation(incident_id, rule_id, usecase_id, organization_id, event_time,
                                       collector_hostname, root_id)

            incident_string = f"{root_id} {tags} {incident_id} {rule_id} {usecase_id} {raw} {subsys} {eventsource_id}" \
                              f"{eventsourcecategory} {eventsourcevendor} " \
                              f"{eventsourcetitle} {organization_id} {event_time} {detected_time}" \
                              f"{subject_category} {subject_privileges} {subject_groupname} {subject_domain} " \
                              f"{subject_name} {subject_version} " \
                              f"{object_category} {object_path} {object_groupname} {object_domain} {object_vendor} " \
                              f"{object_version} {object_state} {object_property} {object_name} {object_value} " \
                              f"{rawids} " \
                              f"{events_source_hostname} {events_source_ip} " \
                              f"{destination_hostname} {destination_ip} {destination_port} " \
                              f"{destination_enrichment_isnetworklocal} " \
                              f"{source_hostname} {source_ip} {source_port} {source_enrichment_isnetworklocal} " \
                              f"{collector_hostname} {collector_ip} " \
                              f"{protocol} {action} {direction} {reason} {status} " \
                              f"{bytes_total} {bytes_out} {packets_total} {packets_in} {packets_out} {interface} " \
                              f"{msgid} {origintime} {recvfile} {tcpflag} {time} {aux1} {aux2} {aux3} {aux4} {aux5} " \
                              f"{aux6} {aux7} {aux8} {aux9} {aux10}" \
                              f"{natinfo_ip} {natinfo_port}"
            identity_hash = hashlib.md5(incident_string.encode('utf-8')).hexdigest()

            selected_incident = session.query(Incident).filter(Incident.inc_id == incident_id).first()
            if selected_incident is not None:
                database_identity_hash = selected_incident.identity_hash
                if identity_hash == database_identity_hash:
                    logging.info("Incident with id %s already exist in PostgreSQL", incident_id)
                    dublicated_incidents = dublicated_incidents + 1
                    continue
                delete_incident(session=session, incident_id=incident_id)

            insert_incident(
                root_id=root_id,
                tags=tags,
                incident_id=incident_id,
                rule_name=rule_id,
                usecase_id=usecase_id,
                raw=raw,
                subsys=subsys,
                eventsource_id=eventsource_id,
                eventsourcecategory=eventsourcecategory,
                eventsourcevendor=eventsourcevendor,
                eventsourcetitle=eventsourcetitle,
                organization_id=organization_id,
                event_time=event_time,
                detected_time=detected_time,
                subject_category=subject_category,
                subject_privileges=subject_privileges,
                subject_groupname=subject_groupname,
                subject_domain=subject_domain,
                subject_name=subject_name,
                subject_version=subject_version,
                object_category=object_category,
                object_path=object_path,
                object_groupname=object_groupname,
                object_domain=object_domain,
                object_vendor=object_vendor,
                object_version=object_version,
                object_state=object_state,
                object_property=object_property,
                object_name=object_name,
                object_value=object_value,
                rawids=rawids,
                identity_hash=identity_hash,
                events_source_hostname=events_source_hostname,
                events_source_ip=events_source_ip,
                destination_hostname=destination_hostname,
                destination_ip=destination_ip,
                destination_port=destination_port,
                destination_enrichment_isnetworklocal=destination_enrichment_isnetworklocal,
                source_hostname=source_hostname,
                source_ip=source_ip,
                source_port=source_port,
                source_enrichment_isnetworklocal=source_enrichment_isnetworklocal,
                collector_hostname=collector_hostname,
                collector_ip=collector_ip,
                protocol=protocol,
                action=action,
                direction=direction,
                reason=reason,
                status=status,
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
                natinfo_ip=natinfo_ip,
                natinfo_port=natinfo_port,
                natinfo_hostname=natinfo_hostname,
                session=session,
                asset_numbers=asset_numbers
            )
            upload_incidents = upload_incidents + 1
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        logging.info("Selected asset number: %d", asset_numbers["selected"])
        logging.info("Inserted asset number: %d", asset_numbers["inserted"])
        logging.info("Upload %d incidents", upload_incidents)
        logging.info("Dublicated %d incidents", dublicated_incidents)
        session.close()
