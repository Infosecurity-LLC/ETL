from sqlalchemy import engine_from_config
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.engine import Engine


def create_session(config: dict) -> (Engine, Session):
    engine = engine_from_config(config)
    session = sessionmaker(bind=engine)()
    return session
