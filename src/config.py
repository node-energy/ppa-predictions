import logging

import sentry_sdk
from apscheduler.schedulers.background import BackgroundScheduler
from sentry_sdk.integrations.logging import LoggingIntegration
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    debug: bool = True
    db_connection_string: str
    sentry_dsn: str | None = None
    cors_origin: str = "https://localhost:3000"
    smtp_host: str = "smtp.office365.com"
    smtp_port: int = 587
    smtp_email: str = "kai.timofejew@node.energy"
    smtp_pass: str
    mail_recipient_cons: str = "verbrauchsprognosen@ppa-mailbox.node.energy"
    mail_recipient_prod: str = "erzeugungsprognosen@ppa-mailbox.node.energy"
    update_cron: str = "45 10 * * *"
    impuls_energy_trading_cron: str = "5 12 * * *"  # after 12 am local time to make sure we send the latest predictions that were respected for fahrplanmanagement
    send_predictions_enabled: bool = False
    api_key: str = "node"
    enercast_ftp_username: str = "node-energy"
    enercast_ftp_pass: str
    enercast_ftp_host: str = "transfer.enercast.de"
    iet_sftp_username: str = "nodeenergysftp.impuls"
    iet_sftp_pass: str
    iet_sftp_host: str = "nodeenergysftp.blob.core.windows.net"

    optinode_db_connection_string: str

    model_config = SettingsConfigDict(env_file="src/.env")


settings = Settings()


if settings.sentry_dsn is not None:
    sentry_sdk.init(
        dsn=settings.sentry_dsn,
        # Set traces_sample_rate to 1.0 to capture 100%
        # of transactions for tracing.
        traces_sample_rate=1.0,
        # Set profiles_sample_rate to 1.0 to profile 100%
        # of sampled transactions.
        # We recommend adjusting this value in production.
        profiles_sample_rate=1.0,
        integrations=[
            LoggingIntegration(
                level=logging.INFO,  # Capture info and above as breadcrumbs
                event_level=logging.ERROR,  # Send errors as events
            ),
        ]
    )


scheduler = BackgroundScheduler()
scheduler.start()
