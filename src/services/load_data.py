import abc
import datetime
import io
import os
import datetime as dt
import re
from functools import cmp_to_key
from typing import Collection

import pandas as pd
import pandera
from pandera.typing import DataFrame
from paramiko import SSHClient, SFTPClient, AutoAddPolicy

from src.config import settings
from src.enums import Measurand, DataRetriever
from src.utils.dataframe_schema import TimeSeriesSchema
from src.utils.exceptions import NoMeteringOrMarketLocationFound, ConflictingEnergyData
from src.utils.timezone import TIMEZONE_BERLIN


class AbstractLoadDataRetriever(abc.ABC):
    @pandera.check_types
    def get_data(
        self, market_location_number: str, measurand: Measurand = Measurand.POSITIVE
    ) -> DataFrame[TimeSeriesSchema]:
        return self._get_data(market_location_number, measurand)

    def _get_data(self, market_location_number: str, measurand: Measurand) -> DataFrame[TimeSeriesSchema]:
        raise NotImplementedError()


class APILoadDataRetriever(AbstractLoadDataRetriever):
    @pandera.check_types
    def get_data(self, market_location_number: str, measurand: Measurand = Measurand.POSITIVE) -> DataFrame[TimeSeriesSchema]:
        return self._get_data(market_location_number, measurand)

    def _get_data(self, market_location_number: str, measurand: Measurand) -> DataFrame[TimeSeriesSchema]:
        raise NotImplementedError()


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


class EnercastSftpDataRetriever(AbstractLoadDataRetriever):
    def __init__(self):
        self.username: str = settings.enercast_ftp_username
        self.password: str = settings.enercast_ftp_pass
        self.host: str = settings.enercast_ftp_host

    def _open_ftp(self):
        self._ssh = SSHClient()
        self._ssh.set_missing_host_key_policy(AutoAddPolicy())  # todo: change to RejectPolicy
        self._ssh.connect(
            hostname=self.host,
            username=self.username,
            password=self.password,
            timeout=60,
        )
        self._sftp: SFTPClient = self._ssh.open_sftp()

    def _csv_to_dataframe(self, file_obj):
        df = pd.read_csv(file_obj, sep=";", decimal=",", index_col=None, header=0)
        return df

    def _get_data(
        self, market_location_number: str, measurand: Measurand = Measurand.NEGATIVE
    ) -> DataFrame[TimeSeriesSchema]:
        try:
            self._open_ftp()
            self._sftp.chdir("/forecasts")
            file_names: list[str] = []
            for file_name in self._sftp.listdir():
                if file_name.endswith(".csv") and file_name.startswith(
                    market_location_number
                ):
                    file_names.append(file_name)

            file_names = sorted(file_names, key=cmp_to_key(self._compare_file_names), reverse=True)
            dfs = []
            # todo this is pretty slow, we could think about only downloading files where the timestamp in the file name
            # indicates that it contains data for the relevant time period
            for file_name in file_names:
                file_obj = io.BytesIO()
                self._sftp.getfo(file_name, file_obj)
                file_obj.seek(0)
                dfs.append(self._csv_to_dataframe(file_obj))
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

        except Exception as exc:
            print(exc)

        finally:
            self._sftp.close()
            self._ssh.close()

    @staticmethod
    def _compare_file_names(file_name_1, file_name_2):
        """
        compares the file names by the timestamp in the file name
        naming convention is <asset_name>_<timestamp>.csv
        timestamp is formatted as: %Y-%m-%d-%H-%M-%S
        """
        pattern = re.compile(".*_(?P<timestamp>(\d{4})(-\d{2})(-\d{2})(-\d{2})-(\d{2})-(\d{2})).csv")
        format = "%Y-%m-%d-%H-%M-%S"
        timestamp_1 = re.fullmatch(pattern, file_name_1)["timestamp"]
        timestamp_1 = datetime.datetime.strptime(timestamp_1, format)
        timestamp_2 = re.fullmatch(pattern, file_name_2)["timestamp"]
        timestamp_2 = datetime.datetime.strptime(timestamp_2, format)
        if timestamp_1 < timestamp_2:
            return -1
        if timestamp_1 > timestamp_2:
            return 1
        return 0


class EnercastApiDataRetriever(AbstractLoadDataRetriever):
    def __init__(self):
        self.host: str = ""

    def _get_data(
        self, market_location_number: str, measurand: Measurand = Measurand.NEGATIVE
    ) -> pd.DataFrame:
        raise NotImplementedError


class IetSftpGenerationDataRetriever(AbstractLoadDataRetriever, SftpMixin):
    def __init__(self):
        self.username: str = settings.iet_sftp_username
        self.password: str = settings.iet_sftp_pass
        self.host: str = settings.iet_sftp_host

    def _get_data(self, market_location_number: str, measurand: Measurand) -> DataFrame[TimeSeriesSchema]:
        raise NotImplementedError()
        # try:
        #     self._open_ftp()
        #     <get data here>
        # except Exception as exc:
        #     print(exc)
        # finally:
        #     self._close_sftp()


class OptinodeDataRetriever(AbstractLoadDataRetriever):  # TODO get rid of this
    def __init__(self):
        os.environ["SECRET_KEY"] = "topsecret"
        os.environ["DATABASE_URL"] = settings.optinode_db_connection_string
        os.environ["DJANGO_SETTINGS_MODULE"] = (
            "optinode.webserver.config.settings.package"
        )
        import django

        django.setup()

    def _get_data(
        self, market_location_number: str, measurand: Measurand = Measurand.POSITIVE
    ) -> DataFrame[TimeSeriesSchema]:
        start_date = dt.datetime.now(tz=TIMEZONE_BERLIN) - dt.timedelta(days=14)
        malo = self._get_market_location(market_location_number, start_date, measurand)

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


DATA_RETRIEVER_MAP = {
    DataRetriever.ENERCAST_SFTP: EnercastSftpDataRetriever,
    DataRetriever.ENERCAST_API: EnercastApiDataRetriever,
    DataRetriever.IMPULS_ENERGY_TRADING_SFTP: IetSftpGenerationDataRetriever,
}