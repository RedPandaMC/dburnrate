"""Pytest configuration for benchmark tests.

Provides fixtures for loading SQL queries and expected costs,
and defines the @pytest.mark.benchmark marker.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest


# Add benchmark marker
def pytest_configure(config):
    """Configure pytest with benchmark marker."""
    config.addinivalue_line(
        "markers",
        "benchmark: benchmark tests for cost estimation accuracy",
    )


@pytest.fixture(scope="session")
def benchmark_dir() -> Path:
    """Return path to benchmark directory."""
    return Path(__file__).parent


@pytest.fixture(scope="session")
def queries_dir(benchmark_dir: Path) -> Path:
    """Return path to queries directory."""
    return benchmark_dir / "queries"


@pytest.fixture(scope="session")
def expected_costs(benchmark_dir: Path) -> dict[str, Any]:
    """Load expected_costs.json."""
    costs_path = benchmark_dir / "expected_costs.json"
    with open(costs_path) as f:
        return json.load(f)


@pytest.fixture
def simple_select_sql(queries_dir: Path) -> str:
    """Load simple_select.sql."""
    with open(queries_dir / "simple_select.sql") as f:
        return f.read()


@pytest.fixture
def single_table_filter_sql(queries_dir: Path) -> str:
    """Load single_table_filter.sql."""
    with open(queries_dir / "single_table_filter.sql") as f:
        return f.read()


@pytest.fixture
def groupby_agg_sql(queries_dir: Path) -> str:
    """Load groupby_agg.sql."""
    with open(queries_dir / "groupby_agg.sql") as f:
        return f.read()


@pytest.fixture
def two_table_join_sql(queries_dir: Path) -> str:
    """Load two_table_join.sql."""
    with open(queries_dir / "two_table_join.sql") as f:
        return f.read()


@pytest.fixture
def five_table_join_sql(queries_dir: Path) -> str:
    """Load five_table_join.sql."""
    with open(queries_dir / "five_table_join.sql") as f:
        return f.read()


@pytest.fixture
def all_benchmark_queries(
    simple_select_sql: str,
    single_table_filter_sql: str,
    groupby_agg_sql: str,
    two_table_join_sql: str,
    five_table_join_sql: str,
) -> dict[str, str]:
    """Return all benchmark queries as a dict."""
    return {
        "simple_select": simple_select_sql,
        "single_table_filter": single_table_filter_sql,
        "groupby_agg": groupby_agg_sql,
        "two_table_join": two_table_join_sql,
        "five_table_join": five_table_join_sql,
    }


@pytest.fixture
def cost_estimator():
    """Create a default CostEstimator for testing."""
    from burnt.estimators.static import CostEstimator

    return CostEstimator()


@pytest.fixture
def hybrid_estimator():
    """Create a HybridEstimator for testing."""
    from burnt.estimators.hybrid import HybridEstimator

    return HybridEstimator()


@pytest.fixture
def default_cluster():
    """Create a default cluster configuration for testing."""
    from burnt.core.models import ClusterConfig

    return ClusterConfig(
        instance_type="Standard_DS3_v2",
        num_workers=2,
        dbu_per_hour=0.75,
    )


@pytest.fixture
def large_cluster():
    """Create a larger cluster configuration for testing."""
    from burnt.core.models import ClusterConfig

    return ClusterConfig(
        instance_type="Standard_DS5_v2",
        num_workers=4,
        dbu_per_hour=3.0,
    )


@pytest.fixture
def mock_backend():
    """Create a SQLiteBackend for testing.

    This fixture uses an in-memory SQLite database with mock data.
    """
    from tests.fixtures.mock_backend import create_mock_backend

    backend = create_mock_backend(scale_factor=1)
    yield backend
    backend.close()
