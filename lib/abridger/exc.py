class AbridgerError(Exception):
    pass


class ConfigFileLoaderError(AbridgerError):
    pass


class IncludeError(ConfigFileLoaderError):
    pass


class DataError(ConfigFileLoaderError):
    pass


class FileNotFoundError(ConfigFileLoaderError):
    pass


class DatabaseUrlError(AbridgerError):
    pass


class ExtractionModelError(AbridgerError):
    pass


class UnknownTableError(AbridgerError):
    pass


class UnknownColumnError(AbridgerError):
    pass


class InvalidConfigError(ExtractionModelError):
    pass


class RelationIntegrityError(ExtractionModelError):
    pass


class GeneratorError(Exception):
    pass


class CyclicDependencyError(GeneratorError):
    pass
