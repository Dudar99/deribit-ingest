"""
Class for working with deribit open API
"""
import datetime
from typing import List

from requests import get, Response
from deribit_api.data_classes import DeribitInstrument, InstrumentData


def _make_get_call(url, params=None):
    """
    Making GET api calls and validating response
    :param url:
    :param params:
    :return:
    """
    response: Response = get(url, params)
    if not response.status_code == 200:
        print(f"Error during API call to {url}. Status code: {response.status_code}")
        raise ValueError("API returned non success status code")
    return response.json()


class DeribitApiClient:
    PUBLIC_URL = "https://deribit.com/api/v2/public"

    def __init__(self):
        pass

    def get_all_instruments(self, currency: str, expired: bool = False, kind: str = None) -> List[DeribitInstrument]:
        """
        Get all instruments
        :param currency:
        :param expired: get non-expired instruements by default
        :param kind: get instruments  for this kind only if provided
        :return: list of DeribitInstrument objects
        """
        params = {
            "currency": currency,
            "expired": "true" if expired else "false"
        }
        if kind:
            params.update({"kind": kind})
        res = _make_get_call(self.PUBLIC_URL + "/get_instruments", params=params)

        return [DeribitInstrument(instrument["instrument_name"],
                                  instrument["kind"],
                                  instrument["instrument_id"],
                                  instrument["expiration_timestamp"])
                for instrument in res["result"]
                ]

    def get_instrument_data(self, start_timestamp: int, end_timestamp: int, instrument_name: str,
                            resolution="1D") -> InstrumentData:
        """
        Getting instrument history data using '/get_tradingview_chart_data' endpoint
        :param start_timestamp: milliseconds after 1970 year as start point
        :param end_timestamp: milliseconds after 1970 year as end point
        :param instrument_name: name of the instrument
        :param resolution: resolution parameter [ 3 5 10 15 30 60 120 180 360 720 1D ]

        :return: InstrumentData data class
        """
        params = {
            "start_timestamp": start_timestamp,
            "end_timestamp": end_timestamp,
            "instrument_name": instrument_name,
            "resolution": resolution
        }
        print(f"Getting data history for '{instrument_name}' instrument from {start_timestamp} to {end_timestamp}")
        data_history = _make_get_call(self.PUBLIC_URL + "/get_tradingview_chart_data", params=params)
        result = data_history["result"]

        return InstrumentData(result["ticks"], result["open"], result["close"], result["high"], result["low"])


if __name__ == '__main__':
    # for tests only
    d = DeribitApiClient()
    i = d.get_all_instruments("BTC", kind="future")[3]

    now = int(datetime.datetime.now().timestamp() * 1000)
    e = d.get_instrument_data(1648972800000, now, i.name)
