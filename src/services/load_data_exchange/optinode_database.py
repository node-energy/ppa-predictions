import datetime
import datetime as dt
import os
from typing import Collection

import pandas as pd
from pandera.typing import DataFrame

from src.config import settings
from src.prognosis.enums import Measurand
from src.services.load_data_exchange.common import AbstractLoadDataRetriever
from src.utils.dataframe_schemas import TimeSeriesSchema
from src.utils.exceptions import NoMeteringOrMarketLocationFound, ConflictingEnergyData
from src.utils.timezone import TIMEZONE_BERLIN


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
        self,
        asset_identifier: str,
        measurand: Measurand,
        start: datetime.datetime | None = None,
        end: datetime.datetime | None = None
    ) -> DataFrame[TimeSeriesSchema]:
        if not start:
            start = dt.datetime.combine(dt.date.today(), dt.time.min, tzinfo=TIMEZONE_BERLIN) - dt.timedelta(days=90)
        malo = self._get_market_location(asset_identifier, start, measurand)

        energy_data: pd.Series = malo.get_load_profile(
            start=start, end=end, measurand=measurand.value
        )
        energy_data = energy_data.tz_convert(TIMEZONE_BERLIN)
        energy_data.name = "value"
        energy_data.index.name = "datetime"
        return DataFrame[TimeSeriesSchema](energy_data.to_frame())

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
