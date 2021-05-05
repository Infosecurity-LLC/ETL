import logging
from typing import Dict, NoReturn

from raven.conf import setup_logging
from raven.handlers.logging import SentryHandler


def prepare_logging(settings: Dict = None, logger: logging.Logger = None) -> NoReturn:
    """
    Метод подготовки логгера
    :param settings: настройки сервиса, в первую очередь те, что касаются логирования
    :param logger: экземпляр класса логгера сервиса
    :return:
    """
    if not logger:
        return None  # нет логгера - нет логов
    if settings is None:
        settings = {'logging': {'basic_level': "DEBUG", 'term_level': "DEBUG"}}
    basic_level = settings['logging']['basic_level']
    term_level = settings['logging']['term_level']
    logger.setLevel(basic_level)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(term_level)
    stream_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(levelname)-1s - %(message)s')
    )
    logger.addHandler(stream_handler)
    if settings.get('sentry_url'):
        sentry_handler = SentryHandler(settings['sentry_url'])
        sentry_handler.setLevel(settings['logging'].get('sentry_level', "ERROR"))
        setup_logging(sentry_handler)
        logger.addHandler(sentry_handler)
