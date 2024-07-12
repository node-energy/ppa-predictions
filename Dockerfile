FROM python:3.11.9-bookworm as requirements-stage

RUN pip install poetry==1.7.1

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /tmp

COPY pyproject.toml poetry.lock /tmp/

RUN poetry export -f requirements.txt --output requirements.txt --without-hashes

FROM python:3.11.9-bookworm

WORKDIR /code

COPY --from=requirements-stage /tmp/requirements.txt /code/requirements.txt

RUN --mount=type=secret,id=PYPI_EXTRA_INDEX_URL \
        export PYPI_EXTRA_INDEX_URL=$(cat /run/secrets/PYPI_EXTRA_INDEX_URL) \
        && pip install --no-cache-dir -r /code/requirements.txt --extra-index-url ${PYPI_EXTRA_INDEX_URL}

COPY ./src /code/src
