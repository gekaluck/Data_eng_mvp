# =============================================================================
# Data Engineering MVP — Convenience Commands
# =============================================================================
# Usage: make <target>
# Run `make help` to see all available targets.
# =============================================================================

.PHONY: up down restart ps logs logs-scheduler test test-schemas clean help

## Start all services in detached mode
up:
	docker compose up -d

## Stop all services
down:
	docker compose down

## Restart all services (rebuild picks up new pip packages)
restart:
	docker compose down && docker compose up -d

## Show running containers
ps:
	docker compose ps

## Tail logs for all services
logs:
	docker compose logs -f

## Tail scheduler logs (useful for DAG import errors)
logs-scheduler:
	docker compose logs -f airflow-scheduler

## Run all tests inside the scheduler container
test:
	docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests/ -v

## Run only schema tests (fast, no Airflow dependency)
test-schemas:
	docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests/test_schemas.py -v

## Stop services and remove volumes (WARNING: deletes all data)
clean:
	docker compose down -v

## Show this help message
help:
	@echo Available targets:
	@echo   up              - Start all services
	@echo   down            - Stop all services
	@echo   restart         - Restart all services
	@echo   ps              - Show running containers
	@echo   logs            - Tail all logs
	@echo   logs-scheduler  - Tail scheduler logs
	@echo   test            - Run all tests inside Docker
	@echo   test-schemas    - Run schema tests only
	@echo   clean           - Stop services and delete volumes
