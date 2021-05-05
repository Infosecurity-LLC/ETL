import json
import logging

from sqlalchemy.orm import Session

import datetime

from full_incidents.thehive_to_dwh.postgresql.models import InteractionDescription, InteractionCategory, Asset, \
    Organizations, Privilege, Entity, Rule, UseCase, Raws, DataPayLoad, InteractionStatus, Ticket, SeverityLevel, \
    EventSourceCategory, VendorSolutions, Vendors, Natinfo

logger = logging.getLogger('thehive_to_dwh')


def insert_interaction_description(protocol, action, direction, reason, status, session):
    """
    Insert into InteractionDescription table. The values are taken from the incident and the
    InteractionCategory, InteractionStatus reference tables. If there are no values in the reference tables - the field
    is not written and the message is written to the log
    """
    interaction_description = InteractionDescription()
    if protocol is not None:
        interaction_description.protocol = protocol
    if direction is not None:
        interaction_description.direction = direction
    if reason:
        interaction_description.reason = reason

    if action is not None:
        interaction_category_id = session.query(InteractionCategory.id).filter(
            InteractionCategory.category_name == action).first()
        if interaction_category_id is None:
            logging.warning(f"Field id from table InteractionCategory with category_name=%s is missing",
                            interaction_category_id)
        interaction_description.action = interaction_category_id

        if status is not None and status != "UnknownInteractionStatus":
            interaction_status = session.query(InteractionStatus.id).filter(InteractionStatus.status_name == status).\
                first()
            if interaction_status is None:
                logging.warning(f"Field id from table InteractionStatus with status_name=%s is missing", status)
            interaction_description.status = interaction_status

    session.add(interaction_description)
    session.flush()
    return interaction_description.id


def select_or_insert_entity(session: Session, privileges=None, groupname=None, path=None, domain=None,
                            counterpart_subject_id=None, counterpart_object_id=None, vendor=None, version=None,
                            state=None, property=None, name=None, value=None):
    """
    Check if the database contains a string with nat fields from the incident. If there is, we get the id of the record,
    if not, we create a new row in the database and also get its id. The resulting id is written to the incident table.
    """
    privilege_id = None
    if privileges:
        privilege_id = session.query(Privilege.id).filter(Privilege.privilege_name == privileges).first()
        if not privilege_id:
            logging.warning("Privilege with privilege_name=%s is missing in PostgreSQL", privileges)

    entity_id = session.query(Entity.id).filter(
        Entity.counterpart_subject_id == counterpart_subject_id,
        Entity.counterpart_object_id == counterpart_object_id,
        Entity.privileges == privilege_id,
        Entity.group == groupname,
        Entity.path == path,
        Entity.domain == domain,
        Entity.vendor == vendor,
        Entity.version == version,
        Entity.state == state,
        Entity.property == property,
        Entity.name == name,
        Entity.value == value
    ).first()
    if entity_id:
        return entity_id
    entity = Entity()
    entity.counterpart_subject_id = counterpart_subject_id
    entity.counterpart_object_id = counterpart_object_id
    entity.privileges = privilege_id
    entity.group = groupname
    entity.path = path
    entity.domain = domain
    entity.vendor = vendor
    entity.version = version
    entity.state = state
    entity.property = property
    entity.name = name
    entity.value = value
    session.add(entity)
    session.flush()
    return entity.id


def select_or_insert_asset(session, asset_numbers, hostname=None, ip=None, organization=None):
    asset_id = session.query(Asset.id).filter(Asset.hostname == hostname,
                                              Asset.ip == ip,
                                              Asset.org_id == organization).first()
    if asset_id:
        asset_numbers["selected"] += 1
    else:
        asset = Asset()
        asset.hostname = hostname
        asset.ip = ip
        asset.org_id = organization
        session.add(asset)
        session.flush()
        asset_numbers["inserted"] += 1
        asset_id = asset.id

    return asset_id


def select_organization_short_name(organization_name: str, session: Session):
    organization = session.query(Organizations).filter(Organizations.short_name == organization_name).first()
    if not organization:
        return False
    return True


def is_exist_rule(rule_name, session: Session):
    rule = session.query(Rule).filter(Rule.rule_name == rule_name).first()
    if not rule:
        return False
    return True


def is_exists_usecase(usecase_id: str, session: Session) -> bool:
    use_case = session.query(UseCase).filter(UseCase.usecase_id == usecase_id).first()
    if not use_case:
        return False
    return True


def insert_datapayload(rawids: list, bytes_total, bytes_out, packets_total, packets_in, packets_out, interface, msgid,
                       origintime, recvfile, tcpflag, time, aux1, aux2, aux3, aux4, aux5, aux6, aux7, aux8, aux9, aux10,
                       session: Session):
    """
    Check if the database contains a line with entity fields from the incident. If there is, we get the id of the
    record, if not, we create a new row in the database and also get its id. The resulting id is written to the incident
    table.
    """
    if time:
        time = datetime.datetime.utcfromtimestamp(time / 1000)
    if origintime:
        origintime = datetime.datetime.utcfromtimestamp(origintime / 1000)
    datapayload_id = session.query(DataPayLoad.id).filter(
        DataPayLoad.bytes_total == bytes_total,
        DataPayLoad.bytes_out == bytes_out,
        DataPayLoad.packets_total == packets_total,
        DataPayLoad.packets_in == packets_in,
        DataPayLoad.packets_out == packets_out,
        DataPayLoad.interface == interface,
        DataPayLoad.msgid == msgid,
        DataPayLoad.origintime == origintime,
        DataPayLoad.recvfile == recvfile,
        DataPayLoad.tcpflag == tcpflag,
        DataPayLoad.time == time,
        DataPayLoad.aux1 == aux1,
        DataPayLoad.aux2 == aux2,
        DataPayLoad.aux3 == aux3,
        DataPayLoad.aux4 == aux4,
        DataPayLoad.aux5 == aux5,
        DataPayLoad.aux6 == aux6,
        DataPayLoad.aux7 == aux7,
        DataPayLoad.aux8 == aux8,
        DataPayLoad.aux9 == aux9,
        DataPayLoad.aux10 == aux10
    ).first()
    if not datapayload_id:
        datapayload = DataPayLoad()
        datapayload.bytes_total = bytes_total
        datapayload.bytes_out = bytes_out
        datapayload.packets_total = packets_total
        datapayload.packets_in = packets_in
        datapayload.packets_out = packets_out
        datapayload.interface = interface
        datapayload.msgid = msgid
        datapayload.origintime = origintime
        datapayload.recvfile = recvfile
        datapayload.tcpflag = tcpflag
        datapayload.time = time
        datapayload.aux1 = aux1
        datapayload.aux2 = aux2
        datapayload.aux3 = aux3
        datapayload.aux4 = aux4
        datapayload.aux5 = aux5
        datapayload.aux6 = aux6
        datapayload.aux7 = aux7
        datapayload.aux8 = aux8
        datapayload.aux9 = aux9
        datapayload.aux10 = aux10
        session.add(datapayload)
        session.flush()
        datapayload_id = datapayload.id

    for rawid in rawids:
        raws_id = session.query(Raws.id).filter(Raws.raw_name == rawid,
                                                Raws.datapayload_id == datapayload_id).first()
        if not raws_id:
            raws = Raws()
            raws.raw_name = rawid
            raws.datapayload_id = datapayload_id
            session.add(raws)
            session.flush()
    return datapayload_id


def select_ticket_id(root_id, session: Session):
    ticket_id = session.query(Ticket.id).filter(Ticket.root_id == root_id).first()
    if ticket_id is None:
        return False
    return ticket_id


def select_severity_level(tags: list, incident_id, session: Session):
    severity_levels = session.query(SeverityLevel).all()
    for severity_level in severity_levels:
        level_name = severity_level.level_name
        if level_name in tags:
            return severity_level.id
    logging.warning("Severity level from tags %s for incident_id=%s is missing in PostgreSQL", json.dumps(list(tags)),
                    incident_id)
    return False


def select_eventsourcecategory(eventsourcecategory, session: Session):
    eventsourcecategory_id = session.query(EventSourceCategory.id).filter(EventSourceCategory.category_name ==
                                                                          eventsourcecategory).first()
    if eventsourcecategory_id is None:
        logging.warning("Eventsourcecategory %s is missing in PostgreSQL", eventsourcecategory)
        return False
    return eventsourcecategory_id


def select_vendor_solutions(vendor_name, title, session: Session):
    vendors_id = session.query(Vendors.id).filter(Vendors.vendor_name == vendor_name).first()
    vendor_solutions_id = None
    if vendors_id:
        vendor_solutions_id = session.query(VendorSolutions.id).filter(VendorSolutions.title == title,
                                                                       VendorSolutions.vendor_id == vendors_id).first()
    if not vendor_solutions_id:
        logging.warning("vendor_solutions_id for vendor_name=%s and title=%s is missing in PostgreSQL", vendor_name,
                        title)
        return False
    return vendor_solutions_id


def select_or_insert_natinfo(session: Session, ip=None, port=None, hostname=None):
    """
    Check if the database contains a line with nat fields from the incident. If there is, we get the id of the
    record, if not, we create a new row in the database and also get its id. The resulting id is written to the incident
    table.
    """
    natinfo_id = session.query(Natinfo.id).filter(Natinfo.ip == ip,
                                                  Natinfo.port == port,
                                                  Natinfo.hostname == hostname).first()
    if natinfo_id:
        return natinfo_id
    natinfo = Natinfo()
    natinfo.ip = ip
    natinfo.port = port
    natinfo.hostname = hostname
    session.add(natinfo)
    session.flush()
    return natinfo.id
