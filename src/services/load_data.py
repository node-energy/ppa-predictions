import abc
import os
import datetime as dt
from typing import Collection

import pandas as pd
from src.config import settings
from src.utils.exceptions import NoMeteringOrMarketLocationFound, ConflictingEnergyData
from src.utils.timezone import TIMEZONE_BERLIN

from optinode.webserver.configurator.enums import Measurand


class AbstractLoadDataRetriever(abc.ABC):
    def get_data(self, market_location_number: str) -> pd.DataFrame:
        raise NotImplementedError()


class APILoadDataRetriever(AbstractLoadDataRetriever):
    def get_data(self, market_location_number: str):
        pass


class OptinodeDataRetriever(AbstractLoadDataRetriever):  # TODO get rid of this
    def __init__(self):
        os.environ["SECRET_KEY"] = "topsecret"
        os.environ["DATABASE_URL"] = settings.optinode_db_connection_string
        os.environ[
            "DJANGO_SETTINGS_MODULE"
        ] = "optinode.webserver.config.settings.package"
        import django

        django.setup()

    def get_data(self, market_location_number: str) -> pd.DataFrame:
        start_date = dt.datetime.now(tz=TIMEZONE_BERLIN) - dt.timedelta(days=14)
        malo = self._get_market_location(market_location_number, start_date)

        energy_data: pd.Series = malo.get_load_profile(start=start_date, measurand=Measurand.POSITIVE)
        energy_data.name = "value"
        energy_data.index.name = "datetime"
        return energy_data.to_frame()

    def _get_market_location(self, market_location_number: str, start_date: dt.datetime):
        from optinode.webserver.configurator.models import MeteringOrMarketLocation

        locations = MeteringOrMarketLocation.objects.filter(
            number=market_location_number,
            #site__is_ppaaas=True
        )
        if not locations.exists():
            raise NoMeteringOrMarketLocationFound(market_location_number)

        if not self._all_locations_have_equal_energy_data(locations, start_date):
            raise ConflictingEnergyData(market_location_number)
        return locations[0]

    @staticmethod
    def _all_locations_have_equal_energy_data(locations: Collection, start_date: dt.datetime) -> bool:
        if len(locations) == 1:
            return True
        energy_data = [loc.get_load_profile(start=start_date, measurand=Measurand.POSITIVE) for loc in locations]
        return all([energy_data[0].equals(ed) for ed in energy_data[1:]])