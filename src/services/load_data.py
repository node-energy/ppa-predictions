import abc
import datetime
import io
import os
import datetime as dt
import re
from dataclasses import dataclass
from functools import cmp_to_key
from typing import Collection, Callable, Type

import pandas as pd
import pandera
from pandera.typing import DataFrame
from paramiko import SSHClient, SFTPClient, AutoAddPolicy

from src.config import settings
from src.domain.model import Producer
from src.enums import Measurand, DataRetriever
from src.utils.dataframe_schema import TimeSeriesSchema
from src.utils.exceptions import NoMeteringOrMarketLocationFound, ConflictingEnergyData
from src.utils.timezone import TIMEZONE_BERLIN


class AbstractLoadDataRetriever(abc.ABC):
    @pandera.check_types
    def get_data(
        self, asset_identifier: str, measurand
    ) -> DataFrame[TimeSeriesSchema]:
        return self._get_data(asset_identifier, measurand)

    def _get_data(self, asset_identifier1: str, measurand: Measurand) -> DataFrame[TimeSeriesSchema]:
        raise NotImplementedError()


class APILoadDataRetriever(AbstractLoadDataRetriever):
    @pandera.check_types
    def get_data(self, asset_identifier: str, measurand: Measurand) -> DataFrame[TimeSeriesSchema]:
        return self._get_data(asset_identifier, measurand)

    def _get_data(self, asset_identifier: str, measurand: Measurand) -> DataFrame[TimeSeriesSchema]:
        raise NotImplementedError()


class AbstractSftpClient(abc.ABC):
    def get_relevant_files(self, asset_identifier: str) -> list[io.BytesIO]:
        raise NotImplementedError


class SftpMixin:
    def _open_sftp(self):
        self._ssh = SSHClient()
        self._ssh.set_missing_host_key_policy(AutoAddPolicy())  # todo: change to RejectPolicy
        self._ssh.connect(
            hostname=self.host,
            username=self.username,
            password=self.password,
            timeout=60,
        )
        self._sftp: SFTPClient = self._ssh.open_sftp()

    def _close_sftp(self):
        self._sftp.close()
        self._ssh.close()


class EnercastSftpClient(AbstractSftpClient, SftpMixin):
    def __init__(self):
        self.username: str = settings.enercast_ftp_username
        self.password: str = settings.enercast_ftp_pass
        self.host: str = settings.enercast_ftp_host

    def get_relevant_files(self, asset_identifier: str) -> list[io.BytesIO]:
        try:
            self._open_sftp()
            self._sftp.chdir("/forecasts")
            return self._download_relevant_files(asset_identifier)
        except Exception as exc:
            print(exc)
        finally:
            self._sftp.close()
            self._ssh.close()

    def _download_relevant_files(self, asset_identifier: str) -> list[io.BytesIO]:
        file_names: list[str] = []
        for file_name in self._sftp.listdir():
            if file_name.endswith(".csv") and file_name.startswith(
                    asset_identifier
            ):
                file_names.append(file_name)

        file_objs = []
        # todo this is pretty slow, we could think about only downloading files where the timestamp in the file name
        # indicates that it contains data for the relevant time period
        for file_name in file_names:
            file_obj = io.BytesIO()
            file_obj.name = file_name
            self._sftp.getfo(file_name, file_obj)
            file_obj.seek(0)
            file_objs.append(file_obj)
        return file_objs


class EnercastSftpDataRetriever(AbstractLoadDataRetriever):
    def __init__(self, sftp_client: AbstractSftpClient = EnercastSftpClient()):
        self.sftp_client = sftp_client

    def _get_data(self, asset_identifier: str, measurand: Measurand) -> DataFrame[TimeSeriesSchema]:
        files = self.sftp_client.get_relevant_files(asset_identifier)
        return self._squash_files_data(files)

    def _squash_files_data(self, files: list[io.BytesIO]) -> DataFrame[TimeSeriesSchema]:
        sorted_files = sorted(files, key=cmp_to_key(self._compare_file_names), reverse=True)
        dfs = [self._csv_to_dataframe(file_obj) for file_obj in sorted_files]
        df = pd.concat(dfs, axis=0, ignore_index=True)
        df.rename(
            columns={"Timestamp (Europe/Berlin)": "datetime", df.columns[1]: "value"},
            inplace=True,
        )
        df.set_index("datetime", inplace=True)
        df.index = pd.to_datetime(df.index)
        df = df.tz_localize(TIMEZONE_BERLIN)
        df = df[~df.index.duplicated(keep='first')]
        df = df.sort_index()
        return df

    def _csv_to_dataframe(self, file_obj):
        df = pd.read_csv(file_obj, sep=";", decimal=",", index_col=None, header=0)
        return df

    @staticmethod
    def _compare_file_names(file_1: io.BytesIO, file_2: io.BytesIO):
        """
        compares the file names by the timestamp in the file name
        naming convention is <asset_name>_<timestamp>.csv
        timestamp is formatted as: %Y-%m-%d-%H-%M-%S
        """
        pattern = re.compile(".*_(?P<timestamp>(\d{4})(-\d{2})(-\d{2})(-\d{2})-(\d{2})-(\d{2})).csv")
        format_ = "%Y-%m-%d-%H-%M-%S"
        timestamp_1 = re.fullmatch(pattern, file_1.name)["timestamp"]
        timestamp_1 = datetime.datetime.strptime(timestamp_1, format_)
        timestamp_2 = re.fullmatch(pattern, file_2.name)["timestamp"]
        timestamp_2 = datetime.datetime.strptime(timestamp_2, format_)
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


def iet_file_name_match(file_name: str) -> re.Match:
    pattern = re.compile(
        "(?P<creation_timestamp>20(?:\d{6})_(?:\d{4}))_erzeugungsprognose_(?P<asset_id>[0-9A-Fa-f\-]*)_(?P<prognosis_date>20(?:\d{6})).csv"
    )
    return re.fullmatch(pattern, file_name)


class IetSftpClient(AbstractSftpClient, SftpMixin):
    def __init__(self):
        self.username: str = settings.iet_sftp_username
        self.password: str = settings.iet_sftp_pass
        self.host: str = settings.iet_sftp_host

    def get_relevant_files(self, asset_identifier: str) -> list[io.BytesIO]:
        try:
            self._open_sftp()
            self._sftp.chdir("/Erzeugungsprognose")
            return self._download_relevant_files(asset_identifier)
        except Exception as exc:
            print(exc)
        finally:
            self._sftp.close()
            self._ssh.close()

    def _download_relevant_files(self, asset_identifier: str) -> list[io.BytesIO]:
        file_names: list[str] = []
        for file_name in self._sftp.listdir():
            match = iet_file_name_match(file_name)
            if match and match["asset_id"] == asset_identifier:
                file_names.append(file_name)

        file_objs = []
        # todo this is pretty slow, we could think about only downloading files where the timestamp in the file name
        # indicates that it contains data for the relevant time period
        for file_name in file_names:
            file_obj = io.BytesIO()
            file_obj.name = file_name
            self._sftp.getfo(file_name, file_obj)
            file_obj.seek(0)
            file_objs.append(file_obj)
        return file_objs


class IetSftpGenerationDataRetriever(AbstractLoadDataRetriever, SftpMixin):
    def __init__(self, sftp_client: AbstractSftpClient = IetSftpClient()):
        self.sftp_client = sftp_client

    def _get_data(
        self,
        asset_identifier: str,
        measurand: Measurand,
    ) -> DataFrame[TimeSeriesSchema]:
        files = self.sftp_client.get_relevant_files(asset_identifier)
        return self._squash_files_data(files)

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

        timestamp_1 = iet_file_name_match(file_1.name)["creation_timestamp"]
        timestamp_1 = datetime.datetime.strptime(timestamp_1, format_)
        timestamp_2 = iet_file_name_match(file_2.name)["creation_timestamp"]
        timestamp_2 = datetime.datetime.strptime(timestamp_2, format_)
        if timestamp_1 < timestamp_2:
            return -1
        if timestamp_1 > timestamp_2:
            return 1
        return 0


class OptinodeDataRetriever(AbstractLoadDataRetriever):  # TODO get rid of this
    def __init__(self):
        os.environ["SECRET_KEY"] = "topsecret"
        os.environ["DATABASE_URL"] = settings.optinode_db_connection_string
        os.environ["DJANGO_SETTINGS_MODULE"] = (
            "optinode.webserver.config.settings.package"
        )
        import django

        django.setup()

    def _get_data(self, asset_identifier: str, measurand: Measurand) -> DataFrame[TimeSeriesSchema]:
        start_date = dt.datetime.now(tz=TIMEZONE_BERLIN) - dt.timedelta(days=14)
        malo = self._get_market_location(asset_identifier, start_date, measurand)

        energy_data: pd.Series = malo.get_load_profile(
            start=start_date, measurand=measurand.value
        )
        energy_data = energy_data.tz_convert(TIMEZONE_BERLIN)
        energy_data.name = "value"
        energy_data.index.name = "datetime"
        return energy_data.to_frame()

    def _get_market_location(
        self, market_location_number: str, start_date: dt.datetime, measurand: Measurand
    ):
        from optinode.webserver.configurator.models import MeteringOrMarketLocation

        locations = MeteringOrMarketLocation.objects.filter(
            number=market_location_number,
             site__is_ppaaas=True
        )
        if not locations.exists():
            raise NoMeteringOrMarketLocationFound(market_location_number)

        if not self._all_locations_have_equal_energy_data(
            locations, start_date, measurand
        ):
            raise ConflictingEnergyData(market_location_number)
        return locations[0]

    @staticmethod
    def _all_locations_have_equal_energy_data(
        locations: Collection, start_date: dt.datetime, measurand: Measurand
    ) -> bool:
        if len(locations) == 1:
            return True
        energy_data = [
            loc.get_load_profile(
                start=start_date, measurand=measurand
            )
            for loc in locations
        ]
        return all([energy_data[0].equals(ed) for ed in energy_data[1:]])


@dataclass
class DataRetrieverConfig:
    data_retriever: Type[AbstractLoadDataRetriever]
    asset_identifier_func: Callable[[Producer], str]


DATA_RETRIEVER_MAP: dict[DataRetriever, DataRetrieverConfig] = {
    DataRetriever.ENERCAST_SFTP: DataRetrieverConfig(
        EnercastSftpDataRetriever,
        lambda producer: producer.market_location.number,
    ),
    DataRetriever.ENERCAST_API: DataRetrieverConfig(
        EnercastApiDataRetriever,
        lambda producer: producer.market_location.number,
    ),
    DataRetriever.IMPULS_ENERGY_TRADING_SFTP: DataRetrieverConfig(
        IetSftpGenerationDataRetriever,
        lambda producer: str(producer.id),
    )
}