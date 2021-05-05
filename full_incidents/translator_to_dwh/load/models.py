from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, VARCHAR


Base = declarative_base()


class Usecase(Base):
    __tablename__ = 'usecase'
    id = Column(Integer, autoincrement=True, primary_key=True)
    lang_id = Column(Integer)
    usecase_id = Column(VARCHAR)
    description = Column(VARCHAR)
    category_id = Column(VARCHAR)


class Rule(Base):
    __tablename__ = 'rule'
    id = Column(Integer, autoincrement=True, primary_key=True)
    lang_id = Column(Integer)
    rule_name = Column(VARCHAR)
    description = Column(VARCHAR)


class Languages(Base):
    __tablename__ = 'languages'
    id = Column(Integer, autoincrement=True, primary_key=True)
    locale = Column(VARCHAR)


class Categories(Base):
    __tablename__ = 'categories'
    id = Column(Integer, autoincrement=True, primary_key=True)
    category_id = Column(VARCHAR)
    lang_id = Column(VARCHAR)
    description = Column(VARCHAR)
