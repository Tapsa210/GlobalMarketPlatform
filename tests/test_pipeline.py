from ingestion import pipeline


def test_ingest_stock_data(monkeypatch):
    sample_response = {
        "Meta Data": {"2. Symbol": "AAPL"},
        "Time Series (Daily)": {
            "2026-11-04": {
                "1. open": "270.00",
                "2. high": "275.00",
                "3. low": "268.00",
                "4. close": "273.00",
                "5. volume": "1000000",
            }
        },
    }

    saved_data = {}

    class FakeAPIClient:
        def get_daily_data(self, symbol):
            assert symbol == "AAPL"
            return sample_response

    def fake_save_raw_response(symbol, source, response_data):
        saved_data["symbol"] = symbol
        saved_data["source"] = source
        saved_data["response_data"] = response_data
        return 42

    monkeypatch.setattr(pipeline, "APIClient", FakeAPIClient)

    monkeypatch.setattr(pipeline, "save_raw_response", fake_save_raw_response)

    record_id = pipeline.ingest_stock_data(" aapl ")

    assert record_id == 42
    assert saved_data["symbol"] == "AAPL"
    assert saved_data["source"] == "alpha_vantage"
    assert saved_data["response_data"] == sample_response


import pytest


def test_ingest_stock_data_api_failure(monkeypatch):
    def fake_get_daily_data(self, symbol):
        raise RuntimeError("API connection failed")

    def fail_if_saved(*args, **kwargs):
        pytest.fail("Data must not be saved when API fails")

    monkeypatch.setattr(pipeline.APIClient, "get_daily_data", fake_get_daily_data)

    monkeypatch.setattr(pipeline, "save_raw_response", fail_if_saved)

    with pytest.raises(RuntimeError, match="API connection failed"):
        pipeline.ingest_stock_data("AAPL")


def test_ingest_stock_data_database_failure(monkeypatch):
    sample_response = {
        "Time Series (Daily)": {
            "2026-11-04": {
                "1. open": "270.00",
                "2. high": "275.00",
                "3. low": "268.00",
                "4. close": "273.00",
                "5. volume": "1000000",
            }
        }
    }

    class FakeAPIClient:
        def get_daily_data(self, symbol):
            return sample_response

    def fake_save_raw_response(**kwargs):
        raise RuntimeError("Database connection failed")

    monkeypatch.setattr(pipeline, "APIClient", FakeAPIClient)

    monkeypatch.setattr(pipeline, "save_raw_response", fake_save_raw_response)

    with pytest.raises(RuntimeError, match="Database connection failed"):
        pipeline.ingest_stock_data("AAPL")
