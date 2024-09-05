import logging

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    debug: bool = True
    db_connection_string: str
    cors_origin: str = "https://localhost:3000"
    smtp_host: str = "smtp.office365.com"
    smtp_port: int = 587
    smtp_email: str = "kai.timofejew@node.energy"
    smtp_pass: str
    mail_recipient_cons: str = "verbrauchsprognosen@ppa-mailbox.node.energy"
    mail_recipient_prod: str = "erzeugungsprognosen@ppa-mailbox.node.energy"
    update_cron: str = "45 10 * * *"
    send_predictions_enabled: bool = False
    api_key: str = "node"
    enercast_ftp_username: str
    enercast_ftp_pass: str
    enercast_ftp_host: str = "transfer.enercast.de"
    iet_sftp_username: str
    iet_sftp_pass: str
    iet_sftp_username: str = "nodeenergysftp.impuls"

    optinode_db_connection_string: str

    model_config = SettingsConfigDict(env_file="src/.env")


settings = Settings()

sentry_sdk.init(
    dsn="https://446b15db143f9477706fbf13a4f6dbd9@o105024.ingest.us.sentry.io/4507814207815680",
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
