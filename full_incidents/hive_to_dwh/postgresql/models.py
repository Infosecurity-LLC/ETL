from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, VARCHAR, Date, Boolean

Base = declarative_base()


class Asset(Base):
    __tablename__ = 'asset'
    id = Column(Integer, autoincrement=True, primary_key=True)
    hostname = Column(VARCHAR)
    ip = Column(VARCHAR)
    fqdn = Column(VARCHAR)
    org_id = Column(VARCHAR)
    first_seen = Column(Date)
    last_seen = Column(Date)
    is_collector = Column(Boolean)
