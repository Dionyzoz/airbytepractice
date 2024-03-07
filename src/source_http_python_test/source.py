from typing import Any, Iterable, Mapping, Optional, Tuple, List, MutableMapping
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.auth.core import NoAuth
from airbyte_cdk.sources.streams.http.http import HttpStream
import requests
from datetime import datetime, timedelta


class ExchangeRates(HttpStream):
    url_base = "https://api.apilayer.com/exchangerates_data/"

    # Set this as a noop.
    primary_key = None
    cursor_field = "date"

    def __init__(self, config: Mapping[str, Any], start_date, **kwargs):
        super().__init__()
        self.base = config["base"]
        self.apikey = config["apikey"]
        self.start_date = start_date
        self._cursor_value = None

    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value.strftime("%Y-%m-%d")}
        else:
            return {self.cursor_field: self.start_date.strftime("%Y-%m-%d")}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = datetime.strptime(value[self.cursor_field], "%Y-%m-%d")

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        # The "/latest" path gives us the latest currency exchange rates
        return stream_slice['date']

    def request_headers(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Mapping[str, Any]:
        # The api requires that we include apikey as a header so we do that in this method
        return {"apikey": self.apikey}

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        # The api requires that we include the base currency as a query param so we do that in this method
        return {"base": self.base}

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        # The response is a simple JSON whose schema matches our stream's schema exactly,
        # so we just return a list containing the response
        return [response.json()]

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        # The API does not offer pagination,
        # so we return None to indicate there are no more pages in the response
        return None

    def _chunk_date_range(self, start_date: datetime) -> List[Mapping[str, Any]]:
        """
        Returns a list of each day between the start date and now.
        The return value is a list of dicts {'date': date_string}.
        """
        dates = []
        while start_date < datetime.now():
            dates.append({self.cursor_field: start_date.strftime("%Y-%m-%d")})
            start_date += timedelta(days=1)
        return dates

    def stream_slices(
        self,
        sync_mode,
        cursor_field: List[str] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        start_date = (
            datetime.strptime(stream_state[self.cursor_field], "%Y-%m-%d")
            if stream_state and self.cursor_field in stream_state
            else self.start_date
        )
        return self._chunk_date_range(start_date)

    def _chunk_date_range(self, start_date: datetime) -> List[Mapping[str, Any]]:
        """
        Returns a list of each day between the start date and now.
        The return value is a list of dicts {'date': date_string}.
        """
        dates = []
        while start_date < datetime.now():
            dates.append({self.cursor_field: start_date.strftime('%Y-%m-%d')})
            start_date += timedelta(days=1)
        return dates

    def stream_slices(self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None) -> Iterable[Optional[Mapping[str, Any]]]:
        start_date = datetime.strptime(stream_state[self.cursor_field], '%Y-%m-%d') if stream_state and self.cursor_field in stream_state else self.start_date
        return self._chunk_date_range(start_date) 


class SourceHttpPythonTest(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        accepted_currencies = {
            "USD",
            "JPY",
            "BGN",
            "CZK",
            "DKK",
        }  # assume these are the only allowed currencies
        input_currency = config["base"]
        if input_currency not in accepted_currencies:
            return (
                False,
                f"Input currency {input_currency} is invalid. Please input one of the following currencies: {accepted_currencies}",
            )
        else:
            return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        # NoAuth just means there is no authentication required for this API and is included for completeness.
        # Skip passing an authenticator if no authentication is required.
        # Other authenticators are available for API token-based auth and Oauth2.
        auth = NoAuth()
        start_date = datetime.strptime(config["start_date"], "%Y-%m-%d")
        return [ExchangeRates(authenticator=auth, config=config, start_date=start_date)]
