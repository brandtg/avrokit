# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: install test test-coverage lint lint-fix typecheck format build publish lock check bump help

help: ## Display this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install all dependencies including extras and dev group
	uv sync --extra all --group dev

build: ## Build the package
	uv build

publish: ## Publish the package to PyPI
	uv publish

test: ## Run tests in parallel
	uv run pytest -n auto

test-coverage: ## Run tests with coverage report
	uv run pytest -n auto --cov=. --cov-report=term

lint: ## Run ruff linter
	uv run ruff check .

lint-fix: ## Run ruff linter and apply fixes
	uv run ruff check --fix .

typecheck: ## Run type checker
	uv run ty check

format: ## Format code with ruff
	uv run ruff format .

bump: ## Bump version: make bump part=patch|minor|major
	uv version --bump $(part)

lock: ## Update the lockfile
	uv lock

check: lint typecheck format test ## Run all checks (lint, typecheck, format, test)

license: ## Annotate files with REUSE license headers
	uv run reuse annotate \
		--license Apache-2.0 \
		--copyright "Greg Brandt <brandt.greg@gmail.com>" \
		--skip-unrecognized \
		--recursive .
