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
CONTROL_M_RENDERED := runtime/controlm/trade_close_to_replenishment.json

.PHONY: help prepare up demo-ready down clean ps logs health kafka-topics eod-readiness-arm eod-readiness-status postgres-stop postgres-start airflow-stop airflow-start \
	seed simulate bronze silver dbt dbt-retry replen deliver \
	gate-eod gate-asn gate-ack run-airflow controlm-build controlm-deploy run-controlm \
	controlm-render controlm-dbt-provision controlm-dbt-trust controlm-service install-databricks-cli \
	databricks-azure-provision databricks-azure-export databricks-azure-sync \
	databricks-azure-replen dbt-cloud-databricks-provision \
	dbt-cloud-publish-controlm dbt-cloud-controlm-provision demo-controlm-azure \
	wms-ack wms-never-ack wms-late wms-reject \
	fail-1 fail-2 fail-3 fail-4 fail-5 reset seed-sla-history test lint

help: ## Show the operator commands
	@awk 'BEGIN {FS = ":.*## "; printf "Retail DataOps demo\n\n"} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

prepare: ## Create local runtime folders and the .env file
	@test -f .env || cp .env.example .env
	@chmod 0600 .env
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

eod-readiness-arm: ## Arm a fresh Kafka-backed EOD threshold generation for DATE
	$(TOOL) python -m demo.eod_readiness arm --date $(DATE)

eod-readiness-status: ## Show Kafka-backed EOD threshold state for DATE
	$(TOOL) python -m demo.eod_readiness status --date $(DATE)

postgres-stop: ## Stop local Postgres after the optional Azure silver sync
	$(COMPOSE) stop postgres

postgres-start: ## Start local Postgres and wait for its health check
	$(COMPOSE) up -d --wait postgres

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

controlm-render: ## Render dbt Cloud job IDs into the ignored Control-M definition
	@python3 controlm/build.py --output $(CONTROL_M_RENDERED)

controlm-build: controlm-render ## Validate the workflow through Control-M Automation API
	ctm build $(CONTROL_M_RENDERED) controlm/descriptors/dev.json -e $${CTM_ENV:-se-dev}

controlm-deploy: controlm-render ## Deploy the development workflow (an explicit external change)
	ctm deploy $(CONTROL_M_RENDERED) controlm/descriptors/dev.json -e $${CTM_ENV:-se-dev}

run-controlm: ## Order Control Plane B for DATE in the connected Control-M environment
	@./controlm/scripts/order_workflow.sh $(DATE)

controlm-service: ## Install/start the host Agent service (requires sudo)
	@./scripts/install_controlm_service.sh

install-databricks-cli: ## Install the pinned optional Azure Databricks CLI for this user
	@./scripts/install_databricks_cli.sh

databricks-azure-provision: ## Create/start the optional auto-terminating Azure cluster
	@python3 databricks/provision_cluster.py

databricks-azure-export: prepare ## Export the validated local silver snapshot for DATE
	$(TOOL) python -m demo.cli export-databricks-silver --date $(DATE)

databricks-azure-sync: databricks-azure-provision databricks-azure-export ## Load DATE into optional Azure Delta silver tables
	@python3 databricks/sync_silver.py --date $(DATE)

databricks-azure-replen: databricks-azure-provision ## Export tested Azure gold data into the WMS order contract
	@python3 databricks/export_replenishment.py --date $(DATE)
	$(TOOL) python -m demo.cli import-databricks-order --date $(DATE)

dbt-cloud-databricks-provision: databricks-azure-provision ## Create the dbt Cloud Databricks connection and development environment
	@python3 dbt/provision_cloud_databricks.py

dbt-cloud-publish-controlm: ## Publish only the dbt project to its dedicated deployment branch
	@./scripts/publish_dbt_controlm_branch.sh

dbt-cloud-controlm-provision: databricks-azure-provision ## Create the dbt Cloud deployment credential, environment and three jobs
	@python3 dbt/provision_controlm_jobs.py

controlm-dbt-provision: ## Store/test the dbt Cloud service token in a centralized Control-M profile
	@python3 controlm/provision_dbt.py

controlm-dbt-trust: ## Add the public dbt Cloud CA to the host Agent Application Integrator trust store
	@./scripts/configure_controlm_dbt_trust.sh

demo-controlm-azure: ## Arm, deploy, order, then publish the event-driven Kafka-to-Azure demo
	$(MAKE) up
	$(MAKE) health
	$(MAKE) seed
	$(MAKE) eod-readiness-arm DATE=$(DATE)
	$(MAKE) dbt-cloud-publish-controlm
	$(MAKE) dbt-cloud-controlm-provision
	$(MAKE) controlm-dbt-trust
	$(MAKE) controlm-dbt-provision
	$(MAKE) controlm-build
	$(MAKE) controlm-deploy
	$(MAKE) run-controlm DATE=$(DATE)
	$(MAKE) simulate DATE=$(DATE)

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
	python3 -m compileall -q demo airflow/dags databricks dbt/provision_cloud_databricks.py dbt/provision_controlm_jobs.py controlm
	python3 -m json.tool controlm/workflows/trade_close_to_replenishment.json >/dev/null
	$(COMPOSE) config --quiet
	@for script in controlm/scripts/*.sh failures/*.sh scripts/*.sh; do bash -n "$$script"; done

test: lint ## Run local unit tests
	$(TOOL) python -m unittest discover -s tests -v
