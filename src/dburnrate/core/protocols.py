from datetime import date
from decimal import Decimal
from typing import Protocol, runtime_checkable


@runtime_checkable
class Estimator(Protocol):
    def estimate(self, query: str, **kwargs: object) -> "CostEstimate": ...


@runtime_checkable
class Parser(Protocol):
    def parse(self, source: str) -> "ParseResult": ...


@runtime_checkable
class ExchangeRateProvider(Protocol):
    def get_rate(self, date: date, from_currency: str, to_currency: str) -> Decimal: ...


class CostEstimate:
    pass


class ParseResult:
    pass
