# Override root conftest to avoid real ClickHouse dependency
import pytest


@pytest.fixture(scope="session")
def clickhouse_manager():
    return None
