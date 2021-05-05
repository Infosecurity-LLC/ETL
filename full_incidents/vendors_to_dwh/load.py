import logging

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import Column, Integer, VARCHAR, ForeignKey
from sqlalchemy.orm import Session

from sqlalchemy.orm import relationship

logger = logging.getLogger('vendors_to_dwh')
Base = declarative_base()


class Vendors(Base):
    __tablename__ = 'vendors'
    id = Column(Integer, autoincrement=True, primary_key=True)
    vendor_name = Column(VARCHAR)

    vendors = relationship("VendorSolutions")


class VendorSolutions(Base):
    __tablename__ = 'vendor_solutions'
    id = Column(Integer, autoincrement=True, primary_key=True)
    title = Column(VARCHAR)
    dev_type = Column(VARCHAR)
    vendor_id = Column(Integer, ForeignKey('vendors.id'))
    category_id = Column(Integer, ForeignKey('device_categories.id'))
    sub_category_id = Column(Integer, ForeignKey('device_sub_categories.id'))


class DeviceCategories(Base):
    __tablename__ = 'device_categories'
    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(VARCHAR)

    vendors = relationship("VendorSolutions")


class DeviceSubCategories(Base):
    __tablename__ = 'device_sub_categories'
    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(VARCHAR)

    vendors = relationship("VendorSolutions")


def insert_and_update_vendors(session: Session, vendors_id, name):
    incident_insert_object = insert(table=Vendors).values(
        id=vendors_id,
        vendor_name=name
    )
    incident_upsert_object = incident_insert_object.on_conflict_do_update(index_elements=['id'], set_=dict(
        vendor_name=name
    ))
    session.execute(incident_upsert_object)


def insert_and_update_device_categories(session: Session, device_id, name):
    incident_insert_object = insert(table=DeviceCategories).values(
        id=device_id,
        name=name,
    )
    incident_upsert_object = incident_insert_object.on_conflict_do_update(index_elements=['id'], set_=dict(
        name=name
    ))
    session.execute(incident_upsert_object)


def insert_device_sub_categories(session: Session, device_id, name):
    incident_insert_object = insert(table=DeviceSubCategories).values(
        id=device_id,
        name=name
    )
    incident_upsert_object = incident_insert_object.on_conflict_do_update(index_elements=['id'], set_=dict(
        name=name
    ))
    session.execute(incident_upsert_object)


def insert_and_update_vendor_solutions(session: Session, vendor_solutions_id, title, dev_type, vendors_id,
                                       device_categories_id, device_sub_categories_id, vendor_solutions_number):
    def filling_vendor_solutions(prepared_vendor_solutions, with_id=True):
        if with_id:
            prepared_vendor_solutions.id = vendor_solutions_id
        prepared_vendor_solutions.title = title
        prepared_vendor_solutions.dev_type = dev_type
        prepared_vendor_solutions.vendors_id = vendors_id
        prepared_vendor_solutions.device_categories_id = device_categories_id
        prepared_vendor_solutions.device_sub_categories_id = device_sub_categories_id
        return prepared_vendor_solutions

    vendor_solutions = session.query(VendorSolutions).filter(VendorSolutions.id == vendor_solutions_id).first()

    if not vendor_solutions:
        insert_vendor_solutions = VendorSolutions()
        vendor_solutions = filling_vendor_solutions(insert_vendor_solutions)
        vendor_solutions_number["inserted"] += 1
    elif vendor_solutions.title == title and vendor_solutions.dev_type == dev_type and \
            vendor_solutions.category_id == device_categories_id \
            and vendor_solutions.sub_category_id == device_sub_categories_id:
        vendor_solutions_number["existed"] += 1
        return
    else:
        filling_vendor_solutions(vendor_solutions, with_id=False)
        vendor_solutions_number["updated"] += 1

    session.add(vendor_solutions)
    session.flush()
    return


def load(device_vendors, device_types, device_categories, device_sub_categories, session: Session):
    vendor_solutions_number = {"inserted": 0,
                               "updated": 0,
                               "existed": 0}

    try:
        if device_vendors:
            for device_vendor in device_vendors:
                insert_and_update_vendors(session, vendors_id=device_vendor.id, name=device_vendor.name)

        if device_categories:
            for device_category in device_categories:
                insert_and_update_device_categories(session, device_id=device_category.id, name=device_category.name)

        if device_sub_categories:
            for device_sub_category in device_sub_categories:
                insert_device_sub_categories(session=session, device_id=device_sub_category.id,
                                             name=device_sub_category.name)

        if device_types:
            for device_type in device_types:
                insert_and_update_vendor_solutions(session=session, vendor_solutions_id=device_type.id,
                                                   title=device_type.product, dev_type=device_type.dev_type,
                                                   vendors_id=device_type.vendor_id,
                                                   device_categories_id=device_type.category_id,
                                                   device_sub_categories_id=device_type.sub_category_id,
                                                   vendor_solutions_number=vendor_solutions_number)
        session.commit()
    except Exception:
        session.rollback()
        raise Exception
    finally:
        session.close()
        logging.info("Inserted vendor_solutions number: %d", vendor_solutions_number["inserted"])
        logging.info("Updated vendor_solutions number: %d", vendor_solutions_number["updated"])
        logging.info("Already existed vendor_solutions number: %d", vendor_solutions_number["existed"])
