import abc
import os
import datetime as dt
import pandas as pd
from src.config import settings


class AbstractLoadDataRetriever(abc.ABC):
    def get_data(self, malo: str):
        raise NotImplementedError()


class APILoadDataRetriever(AbstractLoadDataRetriever):
    def get_data(self, malo: str):
        pass


class OptinodeDataRetriever(AbstractLoadDataRetriever):  # TODO get rid of this
    def __init__(self):
        os.environ["SECRET_KEY"] = "topsecret"
        os.environ["DATABASE_URL"] = settings.optinode_db_connection_string
        os.environ[
            "DB_DJANGO_HOST"
        ] = "optinode-production-read-replica.postgres.database.azure.com"
        os.environ["DB_PORT"] = "5432"
        os.environ["DB_NAME"] = "optinode"
        os.environ["DB_USER"] = "Development"
        os.environ["DB_PASSWORD"] = ""
        os.environ["EMAIL_HOST"] = "smtp.office365.com"
        os.environ["EMAIL_HOST_USER"] = "horst.schlemmer@node.energy"
        os.environ["EMAIL_HOST_PASSWORD"] = "dummy"
        os.environ[
            "DJANGO_SETTINGS_MODULE"
        ] = "optinode.webserver.config.settings.package"
        import django

        django.setup()

    def get_data(self, malo: str):
        from optinode.webserver.configurator.models import Counter

        counter = Counter.objects.get(metering_or_market_location__number=malo)  # TODO malo is not unique?
        start_date = dt.datetime.now() - dt.timedelta(days=14)
        df_meter: pd.DataFrame = counter.get_as_load_profile(start=start_date)
        df_meter = df_meter.reset_index()
        df_meter.rename(columns={"index": "datetime", 0: "value"}, inplace=True)
        df_meter.set_index("datetime", inplace=True)
        df_meter = df_meter.tz_convert("Europe/Berlin")
        return df_meter
