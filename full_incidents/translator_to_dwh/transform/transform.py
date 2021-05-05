import logging

from full_incidents.translator_to_dwh.transform.deserialization import deserialization

logger = logging.getLogger('translator_to_dwh')


def transform(usecases_objs: list, usecase_categories_objs: list):
    """
    Joining the usecases and usecase_categories tables using a common usecase_id field.
    :param usecases_objs: list of usecases objects
    :param usecase_categories_objs: list of usecase_categories objects
    :return: joining tables as a list of dictionaries
    """
    usecases = deserialization(usecases_objs)
    usecase_categories = deserialization(usecase_categories_objs)
    usecase_category_ids = {elem['usecase_id']: elem['category_id'] for elem in usecase_categories}
    for usecase in usecases:
        category_id = usecase_category_ids.get(usecase['usecase_id'], None)
        if category_id is None:
            logging.info("Usecase id %s is missing in usecase_category table in translator db", usecase['usecase_id'])
        usecase['category_id'] = category_id
    return usecases
