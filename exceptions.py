class ElastalertFieldMissingError(Exception):
    pass


class SettingFieldMissingError(Exception):
    pass


class SettingFieldTypeError(Exception):
    pass


class PostgreSQLFieldMissingError(Exception):
    pass


class OTRSFieldMissingError(Exception):
    pass


class ElasticSearchFieldMissingError(Exception):
    pass


class InfluxDBFieldMissingError(Exception):
    pass


class ErrorVariableParser(BaseException):
    pass


class ErrorParametersParser(BaseException):
    pass


class ErrorStartDateParser(BaseException):
    pass


class ErrorMySqlParameters(Exception):
    pass


class ErrorPostgreParameters(Exception):
    pass
