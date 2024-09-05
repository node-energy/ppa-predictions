import io
from importlib import resources
from unittest import mock

import pandas as pd
import pytest
from pandas._testing import assert_frame_equal

from src.enums import Measurand
from src.services.load_data import OptinodeDataRetriever, EnercastSftpDataRetriever, AbstractSftpClient, \
    IetSftpGenerationDataRetriever
from src.utils.exceptions import ConflictingEnergyData, NoMeteringOrMarketLocationFound
from src.utils.timezone import TIMEZONE_BERLIN
from tests.integration import test_files


def read_expected_df(file_name):
    with resources.path(test_files, file_name) as path:
        df = pd.read_csv(path, sep=";", decimal=",", index_col=None, header=0)
    df.set_index("datetime", inplace=True)
    df.index = pd.to_datetime(df.index, format="%d.%m.%y %H:%M")
    df = df.tz_localize(TIMEZONE_BERLIN)
    return df


class FakeIetSftpClient(AbstractSftpClient):
    def get_relevant_files(self, asset_identifier: str) -> list[io.BytesIO]:
        file_objs = []
        if not asset_identifier == "fde5bb7e-b0a9-4dc5-ae9e-9ca109777cb7":
            return file_objs
        for file_name in [
            test_files.IET_PREDICTION_FROM_27_08_FOR_02_09,
            test_files.IET_PREDICTION_FROM_28_08_FOR_02_09,
            test_files.IET_PREDICTION_FROM_28_08_FOR_03_09
        ]:
            with resources.path(test_files, file_name) as path:
                with open(path, "rb") as f:
                    file_obj = io.BytesIO(f.read())
                    file_obj.name = file_name
                    file_objs.append(file_obj)
        return file_objs


class TestIetSftpGenerationDataRetriever:
    def test_load_data(self):
        data = IetSftpGenerationDataRetriever(
            sftp_client=FakeIetSftpClient()
        ).get_data(asset_identifier="fde5bb7e-b0a9-4dc5-ae9e-9ca109777cb7", measurand=Measurand.NEGATIVE)
        expected_df = read_expected_df(test_files.IET_MERGED_PREDICTIONS)
        assert_frame_equal(data, expected_df)


class FakeEnercastSftpClient(AbstractSftpClient):
    def get_relevant_files(self, asset_identifier: str) -> list[io.BytesIO]:
        file_objs = []
        if not asset_identifier == "50571705655":
            return file_objs
        for file_name in [
            test_files.ENERCAST_PREDICTIONS_FROM_28_08,
            test_files.ENERCAST_PREDICTIONS_FROM_29_08,
        ]:
            with resources.path(test_files, file_name) as path:
                with open(path, "rb") as f:
                    file_obj = io.BytesIO(f.read())
                    file_obj.name = file_name
                    file_objs.append(file_obj)
        return file_objs


class TestEnercastSftpDataRetriever:
    def test_load_data(self):
        data = EnercastSftpDataRetriever(
            sftp_client=FakeEnercastSftpClient()
        ).get_data(asset_identifier="50571705655", measurand=Measurand.NEGATIVE)
        expected_df = read_expected_df(test_files.ENERCAST_MERGED_PREDICTIONS)
        assert_frame_equal(data, expected_df)


class TestOptinodeDataRetriever:
    def test_load_data(self):
        series = pd.Series(
            index=pd.DatetimeIndex(
                data=pd.date_range(
                    start="2021-01-01T00:00:00", periods=5, freq="15min", tz=TIMEZONE_BERLIN
                ),
                name="datetime"
            ),
            data=[1, 2, 3, 4, 5],
            name="value",
        )

        data_retriever = OptinodeDataRetriever()

        with mock.patch(
            "optinode.webserver.configurator.models.MeteringOrMarketLocation.objects.filter"
        ) as mock_filter:
            mock_malo = mock.MagicMock()
            mock_malo.get_load_profile.return_value = series.copy()
            mock_filter.return_value = mock_filter
            mock_filter.__iter__.return_value = [mock_malo, mock_malo]
            mock_filter.__getitem__.return_value = mock_malo
            mock_filter.__len__.return_value = 2
            mock_filter.exists.return_value = True

            data = data_retriever.get_data(asset_identifier="12345", measurand=Measurand.POSITIVE)
        pd.testing.assert_frame_equal(data, series.to_frame())

    def test_load_data_conflicting_data(self):
        data_retriever = OptinodeDataRetriever()

        with mock.patch(
            "optinode.webserver.configurator.models.MeteringOrMarketLocation.objects.filter"
        ) as mock_filter:
            mock_malo_1 = mock.MagicMock()
            mock_malo_1.get_load_profile.return_value = pd.Series(
                {
                    "2021-01-01T00:00:00Z": 1,
                    "2021-01-01T00:15:00Z": 2,
                    "2021-01-01T00:30:00Z": 3,
                    "2021-01-01T00:45:00Z": 4,
                    "2021-01-01T01:00:00Z": 5,
                }
            )
            mock_malo_2 = mock.MagicMock()
            mock_malo_2.get_load_profile.return_value = pd.Series(
                {
                    "2021-01-01T00:00:00Z": 1,
                    "2021-01-01T00:15:00Z": 2,
                    "2021-01-01T00:30:00Z": 3,
                    "2021-01-01T00:45:00Z": 4,
                    "2021-01-01T01:00:00Z": 700,
                }
            )
            mock_filter.return_value = mock_filter
            mock_filter.__iter__.return_value = [mock_malo_1, mock_malo_2]
            mock_filter.__len__.return_value = 2
            mock_filter.exists.return_value = True

            with pytest.raises(ConflictingEnergyData):
                data_retriever.get_data(asset_identifier="12345", measurand=Measurand.POSITIVE)

    def test_load_data_no_location(self):
        data_retriever = OptinodeDataRetriever()

        with mock.patch(
            "optinode.webserver.configurator.models.MeteringOrMarketLocation.objects.filter"
        ) as mock_filter:
            mock_filter.return_value = mock_filter
            mock_filter.exists.return_value = False

            with pytest.raises(NoMeteringOrMarketLocationFound):
                data_retriever.get_data(asset_identifier="12345", measurand=Measurand.POSITIVE)
