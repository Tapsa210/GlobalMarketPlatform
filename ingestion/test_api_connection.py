from ingestion.api_client import APIClient, APIClientError, APIResponseError


def main():
    client = APIClient()

    try:
        data = client.get_daily_data("AAPL")

        time_series = data["Time Series (Daily)"]

        print("API connection successful!")
        print("Symbol:", data["Meta Data"]["2. Symbol"])
        print("Number of records:", len(time_series))

        latest_date = max(time_series.keys())
        latest_record = time_series[latest_date]

        print("Latest available date:", latest_date)
        print("Open:", latest_record["1. open"])
        print("High:", latest_record["2. high"])
        print("Low:", latest_record["3. low"])
        print("Close:", latest_record["4. close"])
        print("Volume:", latest_record["5. volume"])

    except (APIClientError, APIResponseError) as error:
        print("API request failed:", error)


if __name__ == "__main__":
    main()