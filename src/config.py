from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    debug: bool = False
    db_connection_string: str
    cors_origin: str = "https://localhost:3000"
    smtp_host: str = "smtp.office365.com"
    smtp_port: int = 587
    smtp_email: str = "kai.timofejew@node.energy"
    smtp_pass: str
    recipient_consumption: str = "verbrauchsprognosen@ppa-mailbox.node.energy"
    recipient_production: str = "erzeugungsprognosen@ppa-mailbox.node.energy"
    update_cron: str = "20 16 * * *"
    send_predictions_enabled: bool = False
    api_key: str = "node"

    optinode_db_connection_string: str

    model_config = SettingsConfigDict(env_file="src/.env")


settings = Settings()
