import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ingestion.config import load_settings

BASE_URL = "https://www.alphavantage.co/query"


class APIResponseError(Exception):
    """Raised when the API returns an error or invalid response."""


class APIClientError(Exception):
    """Raised when an API request fails due to network or timeout issues."""

class APIClient:
    # --- هنا ينحط الـ __init__ الجديد ---
    def __init__(self, timeout=30):
        settings = load_settings()

        self.api_key = settings.alpha_vantage_api_key
        self.timeout = timeout
        self.session = requests.Session()

        self._configure_retries()  # استدعاء دالة الـ Retry التي جهزناها

    def _configure_retries(self):
        """Configure retry strategy for the HTTP session."""
        retry_strategy = Retry(
            total=3,
            connect=3,
            read=3,
            status=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            respect_retry_after_header=True,
            raise_on_status=False,
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)

        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def get_daily_data(self, symbol: str) -> dict:
        # بقية الكود الخاص بجلب البيانات...
        url = BASE_URL
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": self.api_key,
        }

        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
        except requests.Timeout as e:
            raise APIClientError(f"Request timed out for symbol {symbol}") from e
        except requests.RequestException as e:
            raise APIClientError(f"HTTP request failed for symbol {symbol}: {e}") from e

        try:
            data = response.json()
        except ValueError as e:
            raise APIResponseError("Invalid JSON response from API") from e

        if "Note" in data:
            raise APIResponseError(f"API Rate Limit reached: {data['Note']}")

        if "Error Message" in data:
            raise APIResponseError(f"API Error: {data['Error Message']}")

        return data