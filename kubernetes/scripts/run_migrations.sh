#! /bin/bash

set -euo pipefail

alembic revision --autogenerate -m "New field on Table X"
alembic upgrade head