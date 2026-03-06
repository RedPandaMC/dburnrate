class DburnrateError(Exception):
    pass


class ParseError(DburnrateError):
    pass


class ConfigError(DburnrateError):
    pass


class PricingError(DburnrateError):
    pass


class EstimationError(DburnrateError):
    pass
