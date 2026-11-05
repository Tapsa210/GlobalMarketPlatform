
import pytest


@pytest.fixture(autouse=True)
def use_test_database(monkeypatch):
    monkeypatch.setenv("POSTGRES_DB", "market_data_test")
