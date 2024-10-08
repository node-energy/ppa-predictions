import abc
import datetime
import io
from typing import Protocol

import pandas as pd
import pandera
from pandera.typing import DataFrame
from paramiko import SSHClient, AutoAddPolicy, SFTPClient

from src.enums import Measurand
from src.utils.dataframe_schemas import TimeSeriesSchema


class AbstractLoadDataRetriever(abc.ABC):
    @pandera.check_types
    def get_data(
        self,
        asset_identifier: str,
        measurand: Measurand,
        start: datetime.datetime | None = None,
        end: datetime.datetime | None = None
    ) -> DataFrame[TimeSeriesSchema]:
        return self._get_data(asset_identifier, measurand, start, end)

    def _get_data(
        self,
        asset_identifier1: str,
        measurand: Measurand,
        start: datetime.datetime | None,
        end: datetime.datetime | None
    ) -> DataFrame[TimeSeriesSchema]:
        raise NotImplementedError()


class AbstractLoadDataSender(abc.ABC):
    @abc.abstractmethod
    def send_data(self, data: pd.DataFrame, **kwargs):
        ...


class APILoadDataRetriever(AbstractLoadDataRetriever):
    @pandera.check_types
    def get_data(self, asset_identifier: str, measurand: Measurand) -> DataFrame[TimeSeriesSchema]:
        return self._get_data(asset_identifier, measurand)

    def _get_data(self, asset_identifier: str, measurand: Measurand) -> DataFrame[TimeSeriesSchema]:
        raise NotImplementedError()


class AbstractSftpClient(abc.ABC):
    pass


class SftpClient(Protocol):
    def _open_sftp(self):
        ...

    def _close_sftp(self):
        ...


class SftpDownloadGenerationPrediction(SftpClient, Protocol):
    def download_generation_prediction(self, asset_identifier: str, **kwargs) -> list[io.BytesIO]:
        ...


class SftpUploadEigenverbrauch(SftpClient, Protocol):
    def upload_eigenverbrauch(self, file_obj: io.BytesIO):
        ...


class SftpUploadResidualLong(SftpClient, Protocol):
    def upload_residual_long(self, file_obj: io.BytesIO):
        ...


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
