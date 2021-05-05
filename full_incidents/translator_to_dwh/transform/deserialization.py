from sqlalchemy import inspect


def deserialization(objects: list):
    """
    Deserializing sqlalchemy objects into a list of dictionaries
    :param objects: list of sqlalchemy objects
    :return: list of data dictionaries from tables
    """
    lst = []
    for obj in objects:
        dct = {c.key: getattr(obj, c.key) for c in inspect(obj).mapper.column_attrs}
        lst.append(dct)
    return lst
