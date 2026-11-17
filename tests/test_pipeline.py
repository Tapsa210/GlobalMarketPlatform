
import pytest

from ingestion import pipeline

SAMPLE_RESPONSE = {
    "Meta Data": {"2. Symbol": "AAPL"},
    "Time Series (Daily)": {
        "2026-11-04": {
            "1. open": "270.00",
            "2. high": "275.00",
            "3. low": "268.00",
            "4. close": "273.00",
            "5. volume": "1000000",
        },
        "2026-11-05": {
            "1. open": "273.00",
            "2. high": "278.00",
            "3. low": "271.00",
            "4. close": "276.00",
            "5. volume": "1200000",
        },
    },
}


def test_ingest_stock_data(monkeypatch):
    saved_data = {}
    sent_records = []
    producer_state = {"closed": False}

    class FakeAPIClient:
        def get_daily_data(self, symbol):
            assert symbol == "AAPL"
            return SAMPLE_RESPONSE

    class FakeProducer:
        def send_stock_record(self, record):
            sent_records.append(record)

        def close(self):
            producer_state["closed"] = True

    def fake_save_raw_response(symbol, source, response_data):
        saved_data["symbol"] = symbol
        saved_data["source"] = source
        saved_data["response_data"] = response_data
        return 42

    monkeypatch.setattr(pipeline, "APIClient", FakeAPIClient)
    monkeypatch.setattr(
        pipeline, "save_raw_response", fake_save_raw_response
    )
    monkeypatch.setattr(pipeline, "StockProducer", FakeProducer)

    record_id = pipeline.ingest_stock_data(" aapl ")

    assert record_id == 42
    assert saved_data["symbol"] == "AAPL"
    assert saved_data["source"] == "alpha_vantage"
    assert saved_data["response_data"] == SAMPLE_RESPONSE

    assert len(sent_records) == 2
    assert [record["symbol"] for record in sent_records] == ["AAPL", "AAPL"]
    assert producer_state["closed"] is True


def test_ingest_stock_data_api_failure(monkeypatch):
    def fake_get_daily_data(self, symbol):
        raise RuntimeError("API connection failed")

    def fail_if_saved(*args, **kwargs):
        pytest.fail("Data must not be saved when API fails")

    monkeypatch.setattr(
        pipeline.APIClient, "get_daily_data", fake_get_daily_data
    )
    monkeypatch.setattr(
        pipeline, "save_raw_response", fail_if_saved
    )

    with pytest.raises(RuntimeError, match="API connection failed"):
        pipeline.ingest_stock_data("AAPL")


def test_ingest_stock_data_database_failure(monkeypatch):
    class FakeAPIClient:
        def get_daily_data(self, symbol):
            return SAMPLE_RESPONSE

    def fake_save_raw_response(**kwargs):
        raise RuntimeError("Database connection failed")

    monkeypatch.setattr(pipeline, "APIClient", FakeAPIClient)
    monkeypatch.setattr(
        pipeline, "save_raw_response", fake_save_raw_response
    )

    with pytest.raises(RuntimeError, match="Database connection failed"):
        pipeline.ingest_stock_data("AAPL")


def test_ingest_stock_data_kafka_failure_closes_producer(monkeypatch):
    producer_state = {
        "sent": 0,
        "closed": False,
    }

    class FakeAPIClient:
        def get_daily_data(self, symbol):
            return SAMPLE_RESPONSE

    class FakeProducer:
        def send_stock_record(self, record):
            producer_state["sent"] += 1
            raise RuntimeError("Kafka connection failed")

        def close(self):
            producer_state["closed"] = True

    monkeypatch.setattr(pipeline, "APIClient", FakeAPIClient)
    monkeypatch.setattr(
        pipeline, "save_raw_response", lambda **kwargs: 42
    )
    monkeypatch.setattr(pipeline, "StockProducer", FakeProducer)

    with pytest.raises(RuntimeError, match="Kafka connection failed"):
        pipeline.ingest_stock_data("AAPL")

    assert producer_state["sent"] == 1
    assert producer_state["closed"] is True
