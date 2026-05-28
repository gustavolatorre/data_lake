.PHONY: up down restart logs test lint fmt clean help

## —— Docker ——————————————————————————————————————————
up: ## Start all services
	docker compose up --build -d

down: ## Stop all services and remove volumes
	docker compose down -v

restart: ## Restart all services
	docker compose restart

logs: ## Tail logs for all services
	docker compose logs -f

logs-airflow: ## Tail Airflow scheduler logs
	docker compose logs -f scheduler

logs-spark: ## Tail Spark master logs
	docker compose logs -f spark-master

## —— Quality ————————————————————————————————————————
test: ## Run tests with coverage
	uv run pytest --cov=src --cov-report=term-missing tests/

lint: ## Run ruff linter and mypy
	uv run ruff check src/ tests/ dags/
	uv run mypy src/

fmt: ## Auto-format code with ruff
	uv run ruff format src/ tests/ dags/
	uv run ruff check --fix src/ tests/ dags/

## —— Utilities ——————————————————————————————————————
clean: ## Remove generated files and caches
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null || true
	rm -rf .coverage htmlcov/

fernet-key: ## Generate a new Fernet key for Airflow
	@uv run python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

webserver-key: ## Generate a new Webserver Secret Key for Airflow
	@uv run python -c "import secrets; print(secrets.token_urlsafe(32))"

jwt-secret-key: ## Generate a new JWT Secret Key for Airflow Execution API
	@uv run python -c "import secrets; print(secrets.token_hex(32))"

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
