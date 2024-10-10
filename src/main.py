import logging
import sys

from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.config import settings, scheduler
from src.infrastructure.message_bus import MessageBus
from src.infrastructure.unit_of_work import SqlAlchemyUnitOfWork
from src.services.load_data_exchange.optinode_database import OptinodeDataRetriever
from src.services.data_sender import DataSender
from src.prognosis import api as locations_api
from src.api.middleware import ApiKeyAuthMiddleware
from src.prognosis.domain import commands
from src.utils.timezone import TIMEZONE_BERLIN

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler(sys.stdout)
log_formatter = logging.Formatter(
    "%(asctime)s [%(processName)s: %(process)d] [%(threadName)s: %(thread)d] [%(levelname)s] %(name)s: %(message)s"
)
stream_handler.setFormatter(log_formatter)
logger.addHandler(stream_handler)


app = FastAPI(debug=True)


origins = [
    settings.cors_origin,
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(ApiKeyAuthMiddleware, api_key=settings.api_key)


app.include_router(locations_api.router)


@app.on_event("startup")
async def init_bus():
    bus = MessageBus()
    bus.setup(
        uow=SqlAlchemyUnitOfWork(),
        ldr=OptinodeDataRetriever(),
        dts=DataSender(),
    )


@app.get("/")
async def root():
    return {"message": "root"}


@scheduler.scheduled_job(CronTrigger.from_crontab(settings.update_cron, timezone=TIMEZONE_BERLIN))
def calculate_and_send_predictions_to_fahrplanmanagement():
    bus = MessageBus()
    bus.handle(commands.UpdatePredictAll())


@scheduler.scheduled_job(CronTrigger.from_crontab(settings.impuls_energy_trading_cron, timezone=TIMEZONE_BERLIN))
def send_data_to_impuls_energy_trading():
    bus = MessageBus()
    # this requires that the historical data was already retrieved
    # and the predictions were calculated on the same day
    bus.handle(commands.SendAllEigenverbrauchsPredictionsToImpuls())
    bus.handle(commands.SendAllResidualLongPredictionsToImpuls())