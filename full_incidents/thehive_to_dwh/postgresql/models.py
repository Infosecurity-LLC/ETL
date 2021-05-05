from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, VARCHAR, ForeignKey

from sqlalchemy.orm import relationship

Base = declarative_base()


class Incident(Base):
    __tablename__ = 'incident'
    id = Column(Integer, autoincrement=True, primary_key=True)
    inc_id = Column(VARCHAR())
    event_time = Column(VARCHAR)
    detect_time = Column(VARCHAR)
    usecase_id = Column(VARCHAR())
    identity_hash = Column(VARCHAR())
    collectorlocationhost_id = Column(Integer)
    raw = Column(VARCHAR())
    subsys = Column(VARCHAR())
    inputId = Column(VARCHAR())
    source_port = Column(Integer)
    destination_port = Column(Integer)
    root_id = Column(VARCHAR())

    event_source = Column(Integer, ForeignKey('asset.id'))
    source_id = Column(Integer, ForeignKey('asset.id'))
    destination_id = Column(Integer, ForeignKey('asset.id'))
    organization_id = Column(Integer, ForeignKey('organizations.id'))
    subject_id = Column(Integer, ForeignKey('entity.id'))
    object_id = Column(Integer)
    interaction = Column(Integer, ForeignKey('interactiondescription.id'))
    eventsourcecategory_id = Column(Integer, ForeignKey('eventsourcecategory.id'))
    data = Column(Integer, ForeignKey('datapayload.id'))
    vendor_id = Column(Integer, ForeignKey('vendor_solutions.id'))
    ticket_id = Column(Integer, ForeignKey('ticket.id'))
    severity_level_id = Column(Integer, ForeignKey('severitylevel.id'))
    rule_id = Column(Integer, ForeignKey('rule.id'))
    nat_id = Column(Integer, ForeignKey('natinfo.id'))

    asset_event_source = relationship("Asset", foreign_keys=[event_source])
    asset_source_id = relationship("Asset", foreign_keys=[source_id])
    asset_destination_id = relationship("Asset", foreign_keys=[destination_id])


class DataPayLoad(Base):
    __tablename__ = 'datapayload'
    id = Column(Integer, autoincrement=True, primary_key=True)
    bytes_total = Column(VARCHAR)
    bytes_out = Column(VARCHAR)
    packets_total = Column(VARCHAR)
    packets_in = Column(VARCHAR)
    packets_out = Column(VARCHAR)
    interface = Column(VARCHAR)
    msgid = Column(VARCHAR)
    origintime = Column(VARCHAR)
    recvfile = Column(VARCHAR)
    tcpflag = Column(VARCHAR)
    time = Column(VARCHAR)
    aux1 = Column(VARCHAR)
    aux2 = Column(VARCHAR)
    aux3 = Column(VARCHAR)
    aux4 = Column(VARCHAR)
    aux5 = Column(VARCHAR)
    aux6 = Column(VARCHAR)
    aux7 = Column(VARCHAR)
    aux8 = Column(VARCHAR)
    aux9 = Column(VARCHAR)
    aux10 = Column(VARCHAR)

    incident = relationship("Incident")


class Raws(Base):
    __tablename__ = 'raws'
    id = Column(Integer, autoincrement=True, primary_key=True)
    raw_name = Column(VARCHAR)
    datapayload_id = Column(Integer)


class Entity(Base):
    __tablename__ = 'entity'
    id = Column(Integer, autoincrement=True, primary_key=True)
    privileges = Column(Integer, ForeignKey('privilege.id'))
    counterpart_object_id = Column(Integer)
    counterpart_subject_id = Column(Integer)
    group = Column(VARCHAR())
    path = Column(VARCHAR)
    name = Column(Integer)
    domain = Column(Integer)
    vendor = Column(VARCHAR)
    version = Column(VARCHAR)
    state = Column(VARCHAR)
    property = Column(VARCHAR)
    value = Column(VARCHAR)

    incident = relationship("Incident")


class Counterpart(Base):
    __tablename__ = 'counterpart'
    id = Column(Integer, autoincrement=True, primary_key=True)
    counter_name = Column(VARCHAR())


class Asset(Base):
    __tablename__ = 'asset'
    id = Column(Integer, autoincrement=True, primary_key=True)
    hostname = Column(VARCHAR)
    ip = Column(VARCHAR)
    org_id = Column(VARCHAR)


class EventSourceCategory(Base):
    __tablename__ = 'eventsourcecategory'
    id = Column(Integer, autoincrement=True, primary_key=True)
    category_name = Column(VARCHAR)

    incident = relationship("Incident")


class Organizations(Base):
    __tablename__ = 'organizations'
    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(VARCHAR)
    short_name = Column(VARCHAR)

    incident = relationship("Incident")


class UseCase(Base):
    __tablename__ = 'usecase'
    id = Column(Integer, autoincrement=True, primary_key=True)
    usecase_id = Column(VARCHAR)


class Rule(Base):
    __tablename__ = 'rule'
    id = Column(Integer, autoincrement=True, primary_key=True)
    rule_name = Column(VARCHAR)

    incident = relationship("Incident")


class InteractionDescription(Base):
    __tablename__ = 'interactiondescription'
    id = Column(Integer, autoincrement=True, primary_key=True)
    protocol = Column(VARCHAR)
    action = Column(VARCHAR, ForeignKey('interactioncategory.category_name'))
    direction = Column(VARCHAR)
    reason = Column(VARCHAR)
    status = Column(VARCHAR)

    incident = relationship("Incident")


class InteractionCategory(Base):
    __tablename__ = 'interactioncategory'
    id = Column(Integer, autoincrement=True, primary_key=True)
    category_name = Column(VARCHAR)

    interactiondescription = relationship("InteractionDescription")


class InteractionStatus(Base):
    __tablename__ = 'interactionstatus'
    id = Column(Integer, autoincrement=True, primary_key=True)
    status_name = Column(VARCHAR)


class Category(Base):
    __tablename__ = 'category'
    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(VARCHAR)


class Privilege(Base):
    __tablename__ = 'privilege'
    id = Column(Integer, autoincrement=True, primary_key=True)
    privilege_name = Column(VARCHAR())

    entity = relationship("Entity")


class Ticket(Base):
    __tablename__ = 'ticket'
    id = Column(Integer, autoincrement=True, primary_key=True)
    root_id = Column(VARCHAR())

    incident = relationship("Incident")


class SeverityLevel(Base):
    __tablename__ = 'severitylevel'
    id = Column(Integer, autoincrement=True, primary_key=True)
    level_name = Column(VARCHAR())

    incident = relationship("Incident")


class VendorSolutions(Base):
    __tablename__ = 'vendor_solutions'
    id = Column(Integer, autoincrement=True, primary_key=True)
    vendor_id = Column(VARCHAR, ForeignKey('vendors.id'))
    title = Column(VARCHAR())

    incident = relationship("Incident")


class Vendors(Base):
    __tablename__ = 'vendors'
    id = Column(Integer, autoincrement=True, primary_key=True)
    vendor_name = Column(VARCHAR())

    incident = relationship("VendorSolutions")


class Natinfo(Base):
    __tablename__ = 'natinfo'
    id = Column(Integer, autoincrement=True, primary_key=True)
    hostname = Column(VARCHAR)
    ip = Column(VARCHAR)
    port = Column(Integer)

    incident = relationship("Incident")
