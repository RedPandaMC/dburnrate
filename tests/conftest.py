import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "unit: fast isolated tests with no external dependencies"
    )
    config.addinivalue_line(
        "markers", "integration: tests requiring Databricks connectivity"
    )
    config.addinivalue_line("markers", "slow: tests taking >5s")


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "unit" not in item.keywords and "integration" not in item.keywords:
            if "tests/unit" in str(item.fspath):
                item.add_marker(pytest.mark.unit)
