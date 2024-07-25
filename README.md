# PPA Predictions

Calculation predictions from historic load profiles

## Requirements

- Docker
- Docker Compose

OR

- Python > 3.11 Environment ready

[!NOTE]
Currently Python 3.11 must be used due to the opti.node requirement.

## Build (Docker)

```export PIP_EXTRA_INDEX_URL="https://username:password@pypi.node.energy"```

### Production Image

```docker build -t ppa-prediction:v1 --target production-stage --secret id=PIP_EXTRA_INDEX_URL .```

### Test Image

```docker build -t ppa-prediction-test:v1 --target test-stage --secret id=PIP_EXTRA_INDEX_URL .```


## Test

```pytest```

## Run locally

```python -m uvicorn src.main:app --reload```

## DB Migrations

ppa-predictions uses alembic for Database Migrations

### Create a new migration from DB Model changes

```alembic revision --autogenerate -m "New field on Table X"```

### Apply migrations

```alembic upgrade head```

## Environment Variables

| Name                           | Description                                  | Default                                     | Example                          |
|--------------------------------|----------------------------------------------|---------------------------------------------|----------------------------------|
| DEBUG                          | Sets Debug Flag for FastAPI                  | True                                        |                                  |
| DB_CONNECTION_STRING*          | Database Connection String                   | -                                           | postgres://user:pw@host:port/db  |
| SMTP_HOST                      | SMTP Host for sending emails                 | smtp.office365.com                          |                                  |
| SMTP_PORT                      | SMTP Port for sending emails                 | 587                                         |                                  |
| SMTP_EMAIL                     | SMTP Email for sending emails                | -                                           |                                  |
| SMTP_PASS*                     | SMTP Pass for sending emails                 | -                                           |                                  |
| MAIL_RECIPIENT_CONS            | Send Consumption Prognosis to this mail      | verbrauchsprognosen@ppa-mailbox.node.energy |                                  |
| MAIL_RECIPIENT_PROD            | Send Production Prognosis to this mail       | erzeugungsprognosen@ppa-mailbox.node.energy |                                  |
| UPDATE_CRON                    | Cron String for prediction update job        | 45 10 * * *                                 |                                  |
| SEND_PREDICTIONS_ENABLED       | Send out emails                              | False                                       |                                  |
| API_KEY*                       | Secret API Key for API Auth                  | -                                           | topsecret                        |
| OPTINODE_DB_CONNECTION_STRING* | Connection String for opti.node read replica | -                                           | postgres://user:pw@host:port/db  |
*Secret
=======
## Access service on staging

```kubectl port-forward svc/ppa-predictions-service 8001:8001 -n ppa (--context optinode-production)```
