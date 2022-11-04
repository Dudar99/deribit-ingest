"""
DeribitIngetor class to work with Api and store data
"""
import csv
import boto3
import tempfile
from datetime import datetime


from deribit_api.api import DeribitApiClient
from deribit_api.data_classes import InstrumentData

INGEST_FOLDER = "ingestion"
DEFAULT_CURRENCY = "BTC"
BUCKET_NAME = "deribi-instruments"


class DeribitIngester:
    def __init__(self, instruments=None, start_timestamp=None, end_timestamp=None):
        """

        :param instruments: Optional instruments list to process
        :param start_timestamp: Optional start_timestamp for the earliest tick time
        :param end_timestamp:   Optional end_timestamp for the latest tick time
        """
        self._deribit_client: DeribitApiClient = DeribitApiClient()

        if not end_timestamp:
            end_timestamp = int(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp())

        if not instruments:
            self._instruments = self._deribit_client.get_all_instruments("BTC")

        self._start_timestamp = start_timestamp
        self._end_timestamp = end_timestamp
        self._s3resource = boto3.resource('s3')

    def _write_to_s3(self, path: str, data: InstrumentData):
        """
        Saving data to temporary file on local and uploading it to s3 which will be deleted after context manager
        :param path: target S3 path
        :param data: formatted data to be uploaded
        :return:
        """
        with tempfile.NamedTemporaryFile(mode="w") as f:
            write = csv.writer(f)
            write.writerows(data.df)
            f.flush()
            self._s3resource.Bucket(BUCKET_NAME).upload_file(f.name, path)

    def _get_last_timestamp_ingested(self, kind: str, instrument: str) -> int:
        """

        :param kind: kind of instrument
        :param instrument: instrument name
        :return:
        """
        last_file = sorted(self._s3resource.Bucket(BUCKET_NAME).objects.filter(
            Prefix=f"{INGEST_FOLDER}/kind={kind}/instrument={instrument}"), key=lambda x: x.key.split('/')[-1])
        if not last_file:
            return 0
        timestamp = last_file[0].key.split('/')[-1].replace(".csv", "")
        timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").replace(hour=0,
                                                                              minute=0,
                                                                              second=0,
                                                                              microsecond=0).timestamp()
        return int(timestamp)

    def ingest(self):
        """
        Going over instruments objects and ingesting data to S3
        Ingestion is idempotent process, it automatically determines whether it is first run or it is incremental run

        """
        for instrument in self._instruments:
            last_timestamp = self._get_last_timestamp_ingested(instrument.kind, instrument.name)
            if self._end_timestamp <= last_timestamp:
                print(f"Nothing to process for {instrument.name}."
                      f"Last processed time: {datetime.fromtimestamp(last_timestamp)}")
                continue
            path = f"{INGEST_FOLDER}/kind={instrument.kind}/instrument={instrument.name}/" \
                   f"{datetime.fromtimestamp(int(self._end_timestamp))}.csv"
            print(
                f"Staring ingestion process for {instrument};"
                f"start_time: {datetime.fromtimestamp(last_timestamp)};"
                f"end_time: {datetime.fromtimestamp(self._end_timestamp)}")
            data = self._deribit_client.get_instrument_data(last_timestamp * 1000,
                                                            self._end_timestamp * 1000,
                                                            instrument.name
                                                            )
            if data.ticks:
                self._write_to_s3(path, data)
            else:
                print(f"There is no data for such period.")
        print("Ingestion finished.")
