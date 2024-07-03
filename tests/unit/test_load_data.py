from unittest import mock

import pandas as pd
import pytest

from src.services.load_data import OptinodeDataRetriever
from src.utils.exceptions import ConflictingEnergyData, NoMeteringOrMarketLocationFound


class TestOptinodeDataRetriever:
    def test_load_data(self):
        series = pd.Series(
            {
                "2021-01-01T00:00:00Z": 1,
                "2021-01-01T00:15:00Z": 2,
                "2021-01-01T00:30:00Z": 3,
                "2021-01-01T00:45:00Z": 4,
                "2021-01-01T01:00:00Z": 5,
            }
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

            data = data_retriever.get_data(market_location_number="12345")
        pd.testing.assert_series_equal(data, series, check_names=False)

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
                data_retriever.get_data(market_location_number="12345")

    def test_load_data_no_location(self):
        data_retriever = OptinodeDataRetriever()

        with mock.patch(
            "optinode.webserver.configurator.models.MeteringOrMarketLocation.objects.filter"
        ) as mock_filter:
            mock_filter.return_value = mock_filter
            mock_filter.exists.return_value = False

            with pytest.raises(NoMeteringOrMarketLocationFound):
                data_retriever.get_data(market_location_number="12345")
