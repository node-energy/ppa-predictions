import datetime
import io
import re
from functools import cmp_to_key

import pandas as pd
from pandera.typing import DataFrame

from src.config import settings
from src.enums import Measurand
from src.services.load_data_exchange.common import SftpMixin, AbstractLoadDataRetriever, \
    SftpDownloadGenerationPrediction, SftpUploadEigenverbrauch, AbstractLoadDataSender, SftpUploadResidualLong
from src.utils.dataframe_schemas import TimeSeriesSchema, IetLoadDataSchema
from src.utils.timezone import TIMEZONE_BERLIN


TIMEZONE_FILENAMES = TIMEZONE_BERLIN


class IetSftpClient(SftpMixin):
    def __init__(self):
        self.username: str = settings.iet_sftp_username
        self.password: str = settings.iet_sftp_pass
        self.host: str = settings.iet_sftp_host

    def download_generation_prediction(
        self,
        asset_identifier: str,
        start: datetime.datetime | None = None,
        end: datetime.datetime | None = None
    ) -> list[io.BytesIO]:
        try:
            self._open_sftp()
            self._sftp.chdir("/Erzeugungsprognose")
            return self._download_relevant_files(
                asset_identifier,
                start,
                end
            )
        except Exception as exc:
            print(exc)
        finally:
            self._sftp.close()
            self._ssh.close()

    def _download_relevant_files(
            self, asset_identifier: str, start: datetime.datetime | None, end: datetime.datetime | None
    ) -> list[io.BytesIO]:
        file_names: list[str] = []
        for file_name in self._sftp.listdir():
            match = iet_generation_file_name_match(file_name)
            if match and match["asset_id"] == asset_identifier and self._prognosis_date_overlaps_with_time_range(
                datetime.datetime.strptime(match["prognosis_date"], "%Y%m%d").date(), start, end
            ):
                file_names.append(file_name)

        file_objs = []
        for file_name in file_names:
            file_obj = io.BytesIO()
            file_obj.name = file_name
            self._sftp.getfo(file_name, file_obj)
            file_obj.seek(0)
            file_objs.append(file_obj)
        return file_objs

    def upload_eigenverbrauch(self, file_obj: io.BytesIO):
        file_path = f"/Eigenverbrauch/Anlagen/{file_obj.name}"
        self._upload_file(file_obj, file_path)

    def upload_residual_long(self, file_obj: io.BytesIO):
        file_path = f"/Ausspeisung/Anlagen/{file_obj.name}"
        self._upload_file(file_obj, file_path)

    def _upload_file(self, file_obj: io.BytesIO, path: str):
        if not settings.send_predictions_enabled:
            return
        try:
            self._open_sftp()
            self._sftp.putfo(file_obj, remotepath=path)
        except Exception as exc:
            print(exc)
        finally:
            self._sftp.close()
            self._ssh.close()

    def _prognosis_date_overlaps_with_time_range(
        self,
        prognosis_date: datetime.date,
        start: datetime.datetime | None,
        end: datetime.datetime | None
    ) -> bool:
        if start is None and end is None:
            return True

        start_included = True
        end_included = True
        if start:
            prog_end = datetime.datetime.combine(prognosis_date, datetime.time.max, tzinfo=TIMEZONE_FILENAMES)
            start_included = start <= prog_end
        if end:
            prog_start = datetime.datetime.combine(prognosis_date, datetime.time.min, tzinfo=TIMEZONE_FILENAMES)
            end_included = end >= prog_start
        return start_included and end_included


class IetSftpGenerationDataRetriever(AbstractLoadDataRetriever):
    def __init__(self, sftp_client: SftpDownloadGenerationPrediction = IetSftpClient()):
        self.sftp_client = sftp_client

    def _get_data(
        self,
        asset_identifier: str,
        measurand: Measurand,
        start: datetime.datetime | None,
        end: datetime.datetime | None
    ) -> DataFrame[TimeSeriesSchema]:
        files = self.sftp_client.download_generation_prediction(asset_identifier, start=start, end=end)
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
            columns={"utc_timestamp": "datetime", "power_mw": "value"},
            inplace=True,
        )
        df["value"] = df["value"].mul(1000)
        df.set_index("datetime", inplace=True)
        df.index = pd.to_datetime(df.index, utc=True, format="%d.%m.%Y %H:%M")
        df = df.tz_convert(TIMEZONE_BERLIN)
        df = df[~df.index.duplicated(keep='first')]
        df = df.sort_index()
        return df

    def _csv_to_dataframe(self, file_obj):
        df = pd.read_csv(file_obj, sep=";", decimal=",", index_col=None, header=0)
        return df

    @staticmethod
    def _compare_file_names(file_1: io.BytesIO, file_2: io.BytesIO) -> int:
        """
        compares the file names by the timestamp in the file name
        naming convention is <creation_timestamp>_erzeugerprognose_<asset_uuid>_<prognosis_date>.csv
        creation_timestamp is formatted as: %Y%m%d_%H%M
        prognosis_date is formatted as: %Y%m%d
        """
        format_ = "%Y%m%d_%H%M"

        timestamp_1 = iet_generation_file_name_match(file_1.name)["creation_timestamp"]
        timestamp_1 = datetime.datetime.strptime(timestamp_1, format_)
        timestamp_2 = iet_generation_file_name_match(file_2.name)["creation_timestamp"]
        timestamp_2 = datetime.datetime.strptime(timestamp_2, format_)
        if timestamp_1 < timestamp_2:
            return -1
        if timestamp_1 > timestamp_2:
            return 1
        return 0


def iet_generation_file_name_match(file_name: str) -> re.Match:
    pattern = re.compile(
        "(?P<creation_timestamp>20\d{6}_\d{4})_erzeugungsprognose_(?P<asset_id>[0-9A-Fa-f\-]*)_(?P<prognosis_date>20\d{6}).csv"
    )
    return re.fullmatch(pattern, file_name)


class IetSftpEigenverbrauchDataSender(AbstractLoadDataSender):
    def __init__(self, sftp_client: SftpUploadEigenverbrauch = IetSftpClient()):
        self.sftp_client = sftp_client

    def send_data(self, data: DataFrame[IetLoadDataSchema], prediction_date: datetime.date) -> None:
        file_obj = self._to_csv(data, prediction_date)
        return self.sftp_client.upload_eigenverbrauch(file_obj)

    def _to_csv(self, df: DataFrame[IetLoadDataSchema], prediction_date: datetime.date) -> io.BytesIO:
        file_obj = io.BytesIO()
        today = datetime.date.today().strftime('%Y%m%d')
        prediction_date_str = prediction_date.strftime('%Y%m%d')
        file_obj.name = f"{today}_eigenverbrauch_anlagen_{prediction_date_str}.csv"
        df.reset_index(inplace=True)
        df["#timestamp"] = df["#timestamp"].dt.strftime('%d.%m.%Y %H:%M:%S')
        df.to_csv(file_obj, sep=";", decimal=",", index=False)
        file_obj.seek(0)
        return file_obj


class IetSftpResidualLongDataSender(AbstractLoadDataSender):
    def __init__(self, sftp_client: SftpUploadResidualLong = IetSftpClient()):
        self.sftp_client = sftp_client

    def send_data(self, data: DataFrame[IetLoadDataSchema], prediction_date: datetime.date) -> None:
        file_obj = self._to_csv(data, prediction_date)
        return self.sftp_client.upload_residual_long(file_obj)

    def _to_csv(self, df: DataFrame[IetLoadDataSchema], prediction_date: datetime.date) -> io.BytesIO:
        file_obj = io.BytesIO()
        today = datetime.date.today().strftime('%Y%m%d')
        prediction_date_str = prediction_date.strftime('%Y%m%d')
        file_obj.name = f"{today}_ausspeisemengen_{prediction_date_str}.csv"
        df.reset_index(inplace=True)
        df["#timestamp"] = df["#timestamp"].dt.strftime('%d.%m.%Y %H:%M:%S')
        df.to_csv(file_obj, sep=";", decimal=",", index=False)
        file_obj.seek(0)
        return file_obj
