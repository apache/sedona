# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

.PHONY: help install install-dev test lint format type-check clean build
.DEFAULT_GOAL := help

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install the project
	uv sync

install-dev: ## Install the project with development dependencies
	uv sync --dev

lint: ## Run linting with ruff
	uv run ruff check src/ tests/

ruff-lint: ## Run linting with ruff
	uv run ruff check src/ tests/

ruff-fix: ## Run ruff with auto-fix
	uv run ruff check --fix src/ tests/

fix: ## Auto-fix linting issues with ruff
	uv run ruff check --fix src/ tests/

format: ## Format code with ruff
	uv run ruff format src/ tests/

ruff-format: ## Format code with ruff
	uv run ruff format src/ tests/

format-fix: ## Format code with ruff (same as format)
	uv run ruff format src/ tests/

format-check: ## Check formatting without modifying files
	uv run ruff format --check src/ tests/

type-check: ## Run type checking
	uv run mypy src/

check: lint type-check ## Run all checks (lint + type)
	uv run ruff format --check src/ tests/

clean: ## Clean build artifacts
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

build: ## Build the package
	uv build

# Development shortcuts
dev-setup: install-dev ## Set up development environment
	uv run pre-commit install
	@echo "Development environment ready!"

# Pre-commit hooks
pre-commit-install: ## Install pre-commit hooks
	uv run pre-commit install

pre-commit-run: ## Run pre-commit on all files
	uv run pre-commit run --all-files

run: ## Run the CLI (use with args like: make run ARGS="--help")
	uv run sedona-tester $(ARGS)

# Testing shortcuts
test-connection: ## Test Databricks connection
	uv run sedona-tester test-connection

list-runtimes: ## List available runtimes
	uv run sedona-tester list-runtimes

download-jars: ## Download JAR files
	uv run sedona-tester download-jars
