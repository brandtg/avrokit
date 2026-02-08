# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: install env test test-coverage lint typecheck format build lock

install:
	poetry install --with dev --extras all

env:
	poetry env use python3.12

build:
	poetry build

test:
	poetry run pytest -n auto

test-coverage:
	poetry run pytest -n auto --cov=. --cov-report=term

lint:
	poetry run flake8 .

typecheck:
	poetry run mypy .

format:
	poetry run black .

lock:
	poetry lock

license:
	poetry run reuse annotate \
		--license Apache-2.0 \
		--copyright "Greg Brandt <brandt.greg@gmail.com>" \
		--skip-unrecognized \
		--recursive .
