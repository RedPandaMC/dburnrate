"""Protocol classes for dburnrate."""

from datetime import date
from decimal import Decimal
from typing import Protocol, runtime_checkable


@runtime_checkable
class Estimator(Protocol):
    """Protocol for cost estimators."""

    def estimate(self, query: str, **kwargs: object) -> "CostEstimate":
        """Estimate cost for a query."""
        ...


@runtime_checkable
class Parser(Protocol):
    """Protocol for query parsers."""

    def parse(self, source: str) -> "ParseResult":
        """Parse source code into an AST."""
        ...


@runtime_checkable
class ExchangeRateProvider(Protocol):
    """Protocol for exchange rate providers."""

    def get_rate(self, date: date, from_currency: str, to_currency: str) -> Decimal:
        """Get exchange rate between currencies."""
        ...


class CostEstimate:
    """Placeholder for CostEstimate model."""

    pass


class ParseResult:
    """Placeholder for ParseResult model."""

    pass
