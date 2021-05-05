import logging

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, VARCHAR, ForeignKey
from sqlalchemy.orm import Session

from sqlalchemy.orm import relationship

logger = logging.getLogger('vendors_to_dwh')
Base = declarative_base()


class DeviceVendors(Base):
    __tablename__ = 'device_vendors'
    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(VARCHAR)

    vendors = relationship("DeviceTypes")


class DeviceTypes(Base):
    __tablename__ = 'device_types'
    id = Column(Integer, autoincrement=True, primary_key=True)
    product = Column(VARCHAR)
    dev_type = Column(VARCHAR)
    vendor_id = Column(Integer, ForeignKey('device_vendors.id'))
    category_id = Column(Integer, ForeignKey('device_categories.id'))
    sub_category_id = Column(Integer, ForeignKey('device_sub_categories.id'))


class DeviceCategories(Base):
    __tablename__ = 'device_categories'
    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(VARCHAR)

    vendors = relationship("DeviceTypes")


class DeviceSubCategories(Base):
    __tablename__ = 'device_sub_categories'
    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(VARCHAR)

    vendors = relationship("DeviceTypes")


def extract(session: Session):
    device_vendors = session.query(DeviceVendors).all()
    device_types = session.query(DeviceTypes).all()
    device_categories = session.query(DeviceCategories).all()
    device_sub_categories = session.query(DeviceSubCategories).all()
    return device_vendors, device_types, device_categories, device_sub_categories
