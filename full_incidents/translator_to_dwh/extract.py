import logging

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, VARCHAR
from sqlalchemy.orm import Session

logger = logging.getLogger('translator_to_dwh')
Base = declarative_base()


class Usecases(Base):
    __tablename__ = 'usecases'
    id = Column(Integer, autoincrement=True, primary_key=True)
    lang_id = Column(VARCHAR)
    usecase_id = Column(VARCHAR)
    description = Column(VARCHAR)


class Rules(Base):
    __tablename__ = 'rules'
    id = Column(Integer, autoincrement=True, primary_key=True)
    lang_id = Column(VARCHAR)
    rule_name = Column(VARCHAR)
    description = Column(VARCHAR)


class Categories(Base):
    __tablename__ = 'categories'
    id = Column(Integer, autoincrement=True, primary_key=True)
    category_id = Column(VARCHAR)
    lang_id = Column(VARCHAR)
    description = Column(VARCHAR)


class UsecaseCategories(Base):
    __tablename__ = 'usecase_categories'
    id = Column(Integer, autoincrement=True, primary_key=True)
    usecase_id = Column(VARCHAR)
    category_id = Column(VARCHAR)


def extract(session: Session):
    usecases = session.query(Usecases).all()
    rules = session.query(Rules).all()
    categories = session.query(Categories).all()
    usecase_categories = session.query(UsecaseCategories).all()
    return usecases, rules, categories, usecase_categories
