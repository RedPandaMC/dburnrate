"""Test fixtures for burnt.

Provides mock backends and test data for unit and integration tests.
"""

from .mock_backend import SQLiteBackend, create_mock_backend
from .mock_data import (
    DBU_MAX,
    DBU_MEAN,
    DBU_MIN,
    SKU_DBU_RATES,
    SKU_DISTRIBUTION,
    SKU_TO_PRODUCT,
    generate_query_history,
    init_mock_database,
    load_benchmark_tables,
    load_billing_data,
    load_compute_node_types,
    load_pricing_data,
)

__all__ = [
    # Backend
    "SQLiteBackend",
    "create_mock_backend",
    # Data loading
    "init_mock_database",
    "load_billing_data",
    "generate_query_history",
    "load_pricing_data",
    "load_compute_node_types",
    "load_benchmark_tables",
    # Constants
    "SKU_DISTRIBUTION",
    "SKU_TO_PRODUCT",
    "SKU_DBU_RATES",
    "DBU_MIN",
    "DBU_MAX",
    "DBU_MEAN",
]
