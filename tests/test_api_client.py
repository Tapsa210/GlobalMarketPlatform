from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from unittest.mock import Mock, patch

import pytest
import requests

from ingestion.api_client import (
    APIClient,
    APIClientError,
    APIResponseError,
)


@patch("ingestion.api_client.load_settings")
def test_get_daily_data_success(mock_settings):
    mock_settings.return_value.alpha_vantage_api_key = "fake_test_key"

    client = APIClient()

    fake_response = Mock()

    fake_response.json.return_value = {
        "Meta Data": {"2. Symbol": "AAPL"},
        "Time Series (Daily)": {
            "2026-11-04": {
                "1. open": "270.50",
                "2. high": "275.00",
                "3. low": "268.00",
                "4. close": "273.50",
                "5. volume": "1500000",
            }
        },
    }

    fake_response.raise_for_status.return_value = None

    with patch.object(client.session, "get", return_value=fake_response) as mock_get:
        result = client.get_daily_data("AAPL")

    assert "Time Series (Daily)" in result
    assert "2026-11-04" in result["Time Series (Daily)"]

    mock_get.assert_called_once()


@patch("ingestion.api_client.load_settings")
def test_get_daily_data_rate_limit(mock_settings):
    mock_settings.return_value.alpha_vantage_api_key = "fake_test_key"

    client = APIClient()

    fake_response = Mock()
    fake_response.json.return_value = {"Note": "API rate limit exceeded."}
    fake_response.raise_for_status.return_value = None

    with (
        patch.object(client.session, "get", return_value=fake_response),
        pytest.raises(APIResponseError),
    ):
        client.get_daily_data("AAPL")


@patch("ingestion.api_client.load_settings")
def test_get_daily_data_http_error(mock_settings):
    mock_settings.return_value.alpha_vantage_api_key = "fake_test_key"

    client = APIClient()

    fake_response = Mock()
    fake_response.raise_for_status.side_effect = requests.HTTPError("500 Server Error")

    with (
        patch.object(client.session, "get", return_value=fake_response),
        pytest.raises(APIClientError),
    ):
        client.get_daily_data("AAPL")


@patch("ingestion.api_client.load_settings")
def test_get_daily_data_invalid_json(mock_settings):
    mock_settings.return_value.alpha_vantage_api_key = "fake_test_key"

    client = APIClient()

    fake_response = Mock()
    fake_response.raise_for_status.return_value = None
    fake_response.json.side_effect = ValueError("Invalid JSON")

    with (
        patch.object(client.session, "get", return_value=fake_response),
        pytest.raises(APIResponseError),
    ):
        client.get_daily_data("AAPL")


@patch("ingestion.api_client.load_settings")
def test_get_daily_data_timeout(mock_settings):
    mock_settings.return_value.alpha_vantage_api_key = "fake_test_key"

    client = APIClient()

    with (
        patch.object(
            client.session, "get", side_effect=requests.Timeout("Connection timed out")
        ),
        pytest.raises(APIClientError),
    ):
        client.get_daily_data("AAPL")


@patch("ingestion.api_client.load_settings")
def test_retry_configuration(mock_settings):
    mock_settings.return_value.alpha_vantage_api_key = "fake_test_key"

    client = APIClient()

    adapter = client.session.get_adapter("https://www.alphavantage.co")

    retry = adapter.max_retries

    assert retry.total == 3
    assert retry.connect == 3
    assert retry.read == 3
    assert retry.status == 3
    assert 503 in retry.status_forcelist
    assert 429 in retry.status_forcelist


@patch("ingestion.api_client.load_settings")
def test_retry_after_temporary_server_errors(mock_settings):
    mock_settings.return_value.alpha_vantage_api_key = "fake_test_key"

    class TemporaryServerErrorHandler(BaseHTTPRequestHandler):
        request_count = 0

        def do_GET(self):
            type(self).request_count += 1
            status_code = 503 if self.request_count < 3 else 200
            self.send_response(status_code)
            self.end_headers()

            if status_code == 200:
                self.wfile.write(
                    b'{"Time Series (Daily)": {"2026-11-04": {"1. close": "273.50"}}}'
                )

        def log_message(self, format, *args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), TemporaryServerErrorHandler)
    server_thread = Thread(target=server.serve_forever)
    server_thread.start()

    try:
        client = APIClient()
        base_url = f"http://127.0.0.1:{server.server_port}/query"
        with patch("ingestion.api_client.BASE_URL", base_url):
            result = client.get_daily_data("AAPL")
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join()

    assert "Time Series (Daily)" in result
    assert TemporaryServerErrorHandler.request_count == 3
