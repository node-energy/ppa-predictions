FROM python:3.11.9-bookworm AS base

RUN pip install poetry==1.7.1

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /tmp

COPY pyproject.toml poetry.lock /tmp/

RUN poetry export -f requirements.txt --output requirements.txt --without-hashes

RUN poetry export -f requirements.txt --only dev --output requirements-dev.txt --without-hashes

#############
# Production Image
#############
FROM base AS production-stage

WORKDIR /code

COPY --from=base /tmp/requirements.txt /code/requirements.txt

RUN --mount=type=secret,id=PIP_EXTRA_INDEX_URL \
        export PIP_EXTRA_INDEX_URL=$(cat /run/secrets/PIP_EXTRA_INDEX_URL) \
        && pip install --no-cache-dir -r /code/requirements.txt

RUN pip install psycopg2-binary

COPY ./src /code/src
COPY ./alembic /code/alembic

COPY alembic.ini ./kubernetes/scripts/run_migrations.sh  /code

#############
# Testimage
#############
FROM production-stage AS test-stage

WORKDIR /code

COPY --from=base /tmp/requirements-dev.txt /code/requirements-dev.txt

RUN pip install --no-cache-dir -r /code/requirements-dev.txt
