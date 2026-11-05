
from database.repository import get_raw_response, save_raw_response


def test_save_raw_response():
    sample_response = {
        "Meta Data": {
            "2. Symbol": "AAPL"
        },
        "Time Series (Daily)": {
            "2026-11-05": {
                "1. open": "270.00",
                "2. high": "275.00",
                "3. low": "268.00",
                "4. close": "272.30",
                "5. volume": "1000000"
            }
        }
    }

    record_id = save_raw_response(
        symbol="aapl",
        source="alpha_vantage",
        response_data=sample_response,
    )

    assert isinstance(record_id, int)
    assert record_id > 0
def test_get_raw_response():
    sample_response = {
        "Time Series (Daily)": {
            "2026-11-05": {
                "1. open": "270.00",
                "2. high": "275.00",
                "3. low": "268.00",
                "4. close": "272.30",
                "5. volume": "1000000",
            }
        }
    }

    record_id = save_raw_response(
        symbol="AAPL",
        source="alpha_vantage",
        response_data=sample_response,
    )

    result = get_raw_response(record_id)

    assert result is not None
    assert result["id"] == record_id
    assert result["symbol"] == "AAPL"
    assert result["source"] == "alpha_vantage"
    assert result["response_data"] == sample_response


def test_get_raw_response_not_found():
    result = get_raw_response(999999999)

    assert result is None
