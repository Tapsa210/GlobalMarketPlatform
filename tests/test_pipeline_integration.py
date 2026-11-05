from database.repository import get_raw_response
from ingestion.pipeline import ingest_stock_data


def test_ingestion_pipeline_integration(monkeypatch):
    from ingestion import api_client

    sample_response = {
        "Meta Data": {
            "2. Symbol": "AAPL"
        },
        "Time Series (Daily)": {
            "2026-11-04": {
                "1. open": "270.00",
                "2. high": "275.00",
                "3. low": "268.00",
                "4. close": "273.00",
                "5. volume": "1000000"
            }
        }
    }

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return sample_response

    def fake_get(*args, **kwargs):
        return FakeResponse()

    monkeypatch.setattr(
        api_client.requests.Session,
        "get",
        fake_get
    )

    record_id = ingest_stock_data("AAPL")

    saved_record = get_raw_response(record_id)

    assert saved_record is not None
    assert saved_record["symbol"] == "AAPL"
    assert saved_record["source"] == "alpha_vantage"
    assert saved_record["response_data"] == sample_response