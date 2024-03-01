# PPA Predictions

Calculation predictions from historic load profiles

## Requirements

- Docker
- Docker Compose

OR

- Python > 3.12 Environment ready

## Build (Docker)


## Test

```pytest```

## Run locally

```python -m uvicorn src.main:app --reload```

## Access service on staging

```kubectl port-forward svc/ppa-predictions-service 8001:8001 -n ppa (--context optinode-staging)```
