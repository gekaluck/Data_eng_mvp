# =============================================================================
# Data Engineering MVP — Convenience Commands
# =============================================================================
# Usage: make <target>
# Run `make help` to see all available targets.
# =============================================================================

.PHONY: build up down restart ps logs logs-scheduler lab logs-lab test test-schemas test-dag clean help

## Build the custom Airflow image (required once after Dockerfile changes, M3+)
build:
	docker compose build

## Start all services in detached mode
up:
	docker compose up -d

## Stop all services
down:
	docker compose down

## Rebuild image and restart all services
restart:
	docker compose down && docker compose build && docker compose up -d

## Show running containers
ps:
	docker compose ps

## Tail logs for all services
logs:
	docker compose logs -f

## Tail scheduler logs (useful for DAG import errors)
logs-scheduler:
	docker compose logs -f airflow-scheduler

## Tail JupyterLab logs
logs-lab:
	docker compose logs -f jupyter-lab

## Open the local notebook browser service
lab:
	docker compose up -d jupyter-lab

## Run all tests inside the scheduler container
test:
	docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests/ -v

## Run only schema tests (fast, no Airflow dependency)
test-schemas:
	docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests/test_schemas.py -v

## Run only DAG integrity tests
test-dag:
	docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests/test_dag_integrity.py -v

## Stop services and remove volumes (WARNING: deletes all data)
clean:
	docker compose down -v

## Show this help message
help:
	@echo Available targets:
	@echo   build           - Build the custom Airflow image (run after Dockerfile changes)
	@echo   up              - Start all services
	@echo   down            - Stop all services
	@echo   restart         - Rebuild image and restart all services
	@echo   ps              - Show running containers
	@echo   logs            - Tail all logs
	@echo   logs-scheduler  - Tail scheduler logs
	@echo   logs-lab        - Tail JupyterLab logs
	@echo   lab             - Start the JupyterLab browser service
	@echo   test            - Run all tests inside Docker
	@echo   test-schemas    - Run schema tests only
	@echo   test-dag        - Run DAG integrity tests only
	@echo   clean           - Stop services and delete volumes
