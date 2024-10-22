import datetime
import io
import re
from functools import cmp_to_key

import pandas as pd
from pandera.typing import DataFrame

from src.config import settings
from src.enums import Measurand
from src.services.load_data_exchange.common import SftpMixin, AbstractLoadDataRetriever, \
    SftpDownloadGenerationPrediction
from src.utils.dataframe_schemas import TimeSeriesSchema
from src.utils.timezone import TIMEZONE_BERLIN


TIMESTAMP_FORMAT = "%Y-%m-%d-%H-%M-%S"


def enercast_generation_file_name_match(file_name: str) -> re.Match:
    pattern = re.compile("(?P<asset_identifier>\d+)_.*_(?P<timestamp>\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}).csv")
    return re.fullmatch(pattern, file_name)


class EnercastSftpClient(SftpMixin):
    def __init__(self):
        self.username: str = settings.enercast_ftp_username
        self.password: str = settings.enercast_ftp_pass
        self.host: str = settings.enercast_ftp_host

    def download_generation_prediction(self, asset_identifier: str, start: datetime.datetime | None = None) -> list[io.BytesIO]:
        try:
            self._open_sftp()
            self._sftp.chdir("/forecasts")
            return self._download_relevant_files(asset_identifier, start)
        except Exception as exc:
            print(exc)
        finally:
            self._sftp.close()
            self._ssh.close()

    def _download_relevant_files(self, asset_identifier: str, start: datetime.datetime | None) -> list[io.BytesIO]:
        file_names: list[str] = []
        for file_name in self._sftp.listdir():
            match = enercast_generation_file_name_match(file_name)
            if match and match["asset_identifier"] == asset_identifier:
                if start and datetime.datetime.strptime(match["timestamp"], TIMESTAMP_FORMAT).astimezone(TIMEZONE_BERLIN) + datetime.timedelta(days=7) < start:
                    continue
                file_names.append(file_name)
                file_names.append(file_name)

        file_objs = []
        for file_name in file_names:
            file_obj = io.BytesIO()
            file_obj.name = file_name
            self._sftp.getfo(file_name, file_obj)
            file_obj.seek(0)
            file_objs.append(file_obj)
        return file_objs


class EnercastSftpDataRetriever(AbstractLoadDataRetriever):
    def __init__(self, sftp_client: SftpDownloadGenerationPrediction = EnercastSftpClient()):
        self.sftp_client = sftp_client

    def _get_data(
        self,
        asset_identifier: str,
        measurand: Measurand,
        start: datetime.datetime,
        end: datetime.datetime
    ) -> DataFrame[TimeSeriesSchema]:
        files = self.sftp_client.download_generation_prediction(asset_identifier, start=start)
        squashed_data = self._squash_files_data(files)
        if not start and not end:
            return squashed_data
        mask = (squashed_data.index >= start if start else True) & (squashed_data.index < end if end else True)
        return squashed_data[mask]

    def _squash_files_data(self, files: list[io.BytesIO]) -> DataFrame[TimeSeriesSchema]:
        sorted_files = sorted(files, key=cmp_to_key(self._compare_file_names), reverse=True)
        dfs = [self._csv_to_dataframe(file_obj) for file_obj in sorted_files]
        df = pd.concat(dfs, axis=0, ignore_index=True)
        df.rename(
            columns={"Timestamp (Europe/Berlin)": "datetime", df.columns[1]: "value"},
            inplace=True,
        )
        df.set_index("datetime", inplace=True)
        df = df[~df.index.duplicated(keep='first')]
        df = df.sort_index()
        return df

    def _csv_to_dataframe(self, file_obj):
        df = pd.read_csv(file_obj, sep=";", decimal=",", index_col=None, header=0)
        # apparently pandas < 2.0 has a bug in tz_localize with zoneinfo ojbects.
        # see https://stackoverflow.com/a/77827969/15077097
        # currently we are restricted to pandas 1.5 because of constraints from optinode dependency
        # therefore tz_localize here works with the string "Europe/Berlin" and then the timezone is changed to
        # the zoneinfo object to stay consistens with the rest of the codebase
        df["Timestamp (Europe/Berlin)"] = pd.to_datetime(df["Timestamp (Europe/Berlin)"]).dt.tz_localize("Europe/Berlin", ambiguous="infer").dt.tz_convert(TIMEZONE_BERLIN)
        return df

    @staticmethod
    def _compare_file_names(file_1: io.BytesIO, file_2: io.BytesIO):
        """
        compares the file names by the timestamp in the file name
        naming convention is <asset_name>_<timestamp>.csv
        timestamp is formatted as: %Y-%m-%d-%H-%M-%S
        """
        timestamp_1 = enercast_generation_file_name_match(file_1.name)["timestamp"]
        timestamp_1 = datetime.datetime.strptime(timestamp_1, TIMESTAMP_FORMAT)
        timestamp_2 = enercast_generation_file_name_match(file_2.name)["timestamp"]
        timestamp_2 = datetime.datetime.strptime(timestamp_2, TIMESTAMP_FORMAT)
        if timestamp_1 < timestamp_2:
            return -1
        if timestamp_1 > timestamp_2:
            return 1
        return 0


class EnercastApiDataRetriever(AbstractLoadDataRetriever):
    def __init__(self):
        self.host: str = ""

    def _get_data(self, asset_identifier: str, measurand: Measurand) -> pd.DataFrame:
        raise NotImplementedError
