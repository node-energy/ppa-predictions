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

    optinode_db_connection_string: str

    model_config = SettingsConfigDict(env_file="src/.env")


settings = Settings()
