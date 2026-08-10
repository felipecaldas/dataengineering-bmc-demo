SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

DATE ?= $(shell awk -F= '/^DEMO_TRADING_DATE=/{print $$2}' .env 2>/dev/null || echo 2026-08-14)
STORES ?= 1
ROWS ?= 400
SECONDS ?= 45
N ?= 15
COMPOSE := docker compose
TOOL := $(COMPOSE) run --rm toolbox
DBT := $(TOOL) dbt

.PHONY: help prepare up demo-ready down clean ps logs health kafka-topics airflow-stop airflow-start \
	seed simulate bronze silver dbt dbt-retry replen deliver \
	gate-eod gate-asn gate-ack run-airflow controlm-build controlm-deploy run-controlm \
	controlm-service wms-ack wms-never-ack wms-late wms-reject \
	fail-1 fail-2 fail-3 fail-4 fail-5 reset seed-sla-history test lint

help: ## Show the operator commands
	@awk 'BEGIN {FS = ":.*## "; printf "Retail DataOps demo\n\n"} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

prepare: ## Create local runtime folders and the .env file
	@test -f .env || cp .env.example .env
	@mkdir -p airflow/logs airflow/config runtime/asn runtime/outbound runtime/wms/ack runtime/wms/reject
	@chmod 0777 airflow/logs airflow/config runtime runtime/asn runtime/outbound runtime/wms runtime/wms/ack runtime/wms/reject

up: prepare ## Build and start the complete self-contained stack
	$(COMPOSE) up -d --build

demo-ready: up ## Start, validate, seed and prepare the standard presentation date
	$(MAKE) health
	$(MAKE) seed
	$(MAKE) simulate DATE=$(DATE)
	$(MAKE) reset DATE=$(DATE)
	@echo "Demo ready for $(DATE): Airflow http://localhost:8080, Redpanda Console http://localhost:8081"

down: ## Stop the demo while retaining its data
	$(COMPOSE) down --remove-orphans

clean: ## Remove the demo containers and named data volumes
	$(COMPOSE) down --volumes --remove-orphans

ps: ## Show container status
	$(COMPOSE) ps

logs: ## Follow logs from the stack
	$(COMPOSE) logs -f --tail=100

health: ## Check Kafka, Blob, Postgres, Databricks-local, WMS, Airflow and Control-M
	@./scripts/health.sh

kafka-topics: ## List Kafka topics through the containerised Redpanda CLI
	$(COMPOSE) exec redpanda rpk topic list --brokers redpanda:9092

airflow-stop: ## Stop only the Airflow control-plane containers
	$(COMPOSE) stop airflow-api-server airflow-scheduler airflow-dag-processor airflow-triggerer

airflow-start: ## Start only the Airflow control-plane containers
	$(COMPOSE) start airflow-api-server airflow-scheduler airflow-dag-processor airflow-triggerer

seed: ## Seed 325 stores, AU calendars, 2,000 products and 28 days of history
	$(TOOL) python -m demo.cli seed --history-days 28

simulate: ## Produce POS/EOD events and ASN for DATE
	$(TOOL) python -m demo.cli simulate --date $(DATE) --wait-ingest
	$(TOOL) python -m demo.cli generate-asn --date $(DATE)

gate-eod: ## Evaluate the store EOD completeness policy
	$(TOOL) python -m demo.cli gate-eod --date $(DATE)

gate-asn: ## Evaluate the supplier ASN arrival gate
	$(TOOL) python -m demo.cli gate-asn --date $(DATE)

gate-ack: ## Evaluate the WMS acknowledgement gate
	$(TOOL) python -m demo.cli gate-ack --date $(DATE)

wms-ack: ## Configure normal WMS acknowledgement behaviour
	$(TOOL) python -m demo.cli wms-mode ack --delay 2

wms-never-ack: ## Configure WMS to receive without acknowledging
	$(TOOL) python -m demo.cli wms-mode never_ack

wms-late: ## Configure WMS to acknowledge after at least 30 seconds
	$(TOOL) python -m demo.cli wms-mode late

wms-reject: ## Configure WMS to produce an explicit rejection file
	$(TOOL) python -m demo.cli wms-mode reject

bronze: ## Run idempotent bronze ingestion for DATE
	$(TOOL) python -m demo.cli databricks-run 440 --date $(DATE)

silver: ## Run idempotent silver conformance for DATE
	$(TOOL) python -m demo.cli databricks-run 441 --date $(DATE)

dbt: ## Build and test the ten-model gold graph for DATE
	$(DBT) build --project-dir dbt/kmart_retail --profiles-dir dbt/kmart_retail --vars "{trading_date: '$(DATE)'}"

dbt-retry: ## Resume the most recent failed dbt invocation
	$(DBT) retry --project-dir dbt/kmart_retail --profiles-dir dbt/kmart_retail --vars "{trading_date: '$(DATE)'}"

replen: ## Generate the replenishment order for DATE
	$(TOOL) python -m demo.cli databricks-run 447 --date $(DATE)

deliver: ## Deliver the order to WMS SFTP
	$(TOOL) python -m demo.cli deliver --date $(DATE)

run-airflow: ## Trigger Control Plane A for DATE
	$(COMPOSE) exec airflow-scheduler airflow dags trigger trade_close_to_replenishment --conf '{"trading_date":"$(DATE)"}'

controlm-build: ## Validate the workflow through Control-M Automation API
	ctm build controlm/workflows/trade_close_to_replenishment.json controlm/descriptors/dev.json -e $${CTM_ENV:-se-dev}

controlm-deploy: ## Deploy the development workflow (an explicit external change)
	ctm deploy controlm/workflows/trade_close_to_replenishment.json controlm/descriptors/dev.json -e $${CTM_ENV:-se-dev}

run-controlm: ## Order Control Plane B for DATE in the connected Control-M environment
	@./controlm/scripts/order_workflow.sh $(DATE)

controlm-service: ## Install/start the host Agent service (requires sudo)
	@./scripts/install_controlm_service.sh

fail-1: ## Withhold STORES EOD markers (use 1 and 8 to show both policies)
	@./failures/f1_late_store.sh $(STORES) $(DATE)

fail-2: ## Remove and suppress the supplier ASN
	@./failures/f2_no_asn.sh $(DATE)

fail-3: ## Emit an ASN with the unannounced carton_id column
	@./failures/f3_schema_drift.sh $(DATE)

fail-4: ## Inject ROWS negative stock positions after snapshotting
	@./failures/f4_phantom_stock.sh $(ROWS) $(DATE)

fail-5: ## Add SECONDS of silver-stage contention
	@./failures/f5_slow_cluster.sh $(SECONDS)

reset: ## Reverse every failure and return DATE to green
	@./failures/reset.sh $(DATE)

seed-sla-history: ## Order N successful Control-M runs for SLA history
	@./scripts/seed_sla_history.sh $(N) $(DATE)

lint: ## Validate Python, Compose, shell and JSON syntax without running services
	python3 -m compileall -q demo airflow/dags
	python3 -m json.tool controlm/workflows/trade_close_to_replenishment.json >/dev/null
	$(COMPOSE) config --quiet
	@for script in controlm/scripts/*.sh failures/*.sh scripts/*.sh; do bash -n "$$script"; done

test: lint ## Run local unit tests
	$(TOOL) python -m unittest discover -s tests -v
