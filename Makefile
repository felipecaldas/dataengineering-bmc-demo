SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

DATE ?= $(shell awk -F= '/^DEMO_TRADING_DATE=/{print $$2}' .env 2>/dev/null || echo 2026-08-14)
STORES ?= 1
ROWS ?= 400
SECONDS ?= 45
N ?= 15
COMPOSE := docker compose
TOOL := $(COMPOSE) run --rm toolbox
CONTROL_M_RENDERED := runtime/controlm/trade_close_to_replenishment.json

.PHONY: help prepare up demo-ready down clean ps logs health controlm-health kafka-topics \
	eod-readiness-arm eod-readiness-status airflow-stop airflow-start seed simulate \
	stage-inputs databricks-ingest dbt-stage dbt-intermediate dbt-gold \
	databricks-export deliver gate-eod gate-asn gate-ack run-airflow \
	controlm-render controlm-build controlm-deploy run-controlm controlm-dbt-provision \
	controlm-dbt-trust controlm-service install-databricks-cli databricks-provision \
	dbt-cloud-connect dbt-cloud-publish dbt-cloud-provision demo-airflow demo-controlm \
	wms-ack wms-never-ack wms-late wms-reject fail-1 fail-2 fail-3 fail-4 fail-5 \
	reset seed-sla-history test lint

help: ## Show the operator commands
	@awk 'BEGIN {FS = ":.*## "; printf "Retail DataOps demo\n\n"} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-22s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

prepare: ## Create ignored runtime folders and the local .env file
	@test -f .env || cp .env.example .env
	@chmod 0600 .env
	@mkdir -p airflow/logs airflow/config runtime/asn runtime/outbound runtime/state runtime/wms/ack runtime/wms/reject runtime/databricks runtime/dbt_cloud runtime/controlm
	@chmod 0777 airflow/logs airflow/config runtime runtime/asn runtime/outbound runtime/wms runtime/wms/ack runtime/wms/reject
	@chmod 0777 runtime/state 2>/dev/null || true
	@for state in runtime/databricks/azure.json runtime/dbt_cloud/azure.json; do test ! -f "$$state" || chmod 0644 "$$state"; done

up: prepare ## Build and start Redpanda, WMS simulation and Airflow
	$(COMPOSE) up -d --build --remove-orphans

demo-ready: up ## Validate the configured cloud-backed presentation environment
	$(MAKE) health
	$(MAKE) seed DATE=$(DATE)
	$(MAKE) reset DATE=$(DATE)
	@echo "Demo ready for $(DATE): Airflow http://localhost:8080, Redpanda Console http://localhost:8081"

down: ## Stop the demo while retaining local state
	$(COMPOSE) down --remove-orphans

clean: ## Remove local containers and named data volumes (destructive)
	$(COMPOSE) down --volumes --remove-orphans

ps: ## Show container status
	$(COMPOSE) ps

logs: ## Follow logs from the local services
	$(COMPOSE) logs -f --tail=100

health: ## Check Kafka, Azure storage, WMS, readiness and Airflow
	@./scripts/health.sh

controlm-health: ## Check that the enrolled host Control-M Agent is running
	@./scripts/controlm_health.sh

kafka-topics: ## List Kafka topics through the containerised Redpanda CLI
	$(COMPOSE) exec redpanda rpk topic list --brokers redpanda:9092

eod-readiness-arm: ## Arm a fresh Kafka-backed EOD generation for DATE
	$(TOOL) python -m demo.eod_readiness arm --date $(DATE)

eod-readiness-status: ## Show Kafka-backed EOD threshold state for DATE
	$(TOOL) python -m demo.eod_readiness status --date $(DATE)

airflow-stop: ## Stop the Airflow control plane
	$(COMPOSE) stop airflow

airflow-start: ## Start the Airflow control plane
	$(COMPOSE) start airflow

seed: ## Upload reference, stock and 28 days of history for DATE to Azure
	$(TOOL) python -m demo.cli seed --date $(DATE) --history-days 28

simulate: ## Publish POS/EOD events and the supplier ASN for DATE
	$(TOOL) python -m demo.cli simulate --date $(DATE)
	$(TOOL) python -m demo.cli generate-asn --date $(DATE)

gate-eod: ## Evaluate the store EOD completeness policy
	$(TOOL) python -m demo.cli gate-eod --date $(DATE)

gate-asn: ## Evaluate the supplier ASN arrival gate
	$(TOOL) python -m demo.cli gate-asn --date $(DATE)

gate-ack: ## Evaluate the WMS acknowledgement gate
	$(TOOL) python -m demo.cli gate-ack --date $(DATE)

stage-inputs: ## Snapshot the active Kafka generation into Azure with a manifest
	$(TOOL) python -m demo.cli stage-inputs --date $(DATE)

databricks-ingest: ## Run the shared Azure Databricks Bronze ingest job for DATE
	@python3 databricks/run_job.py ingest --date $(DATE)

dbt-stage: ## Run the shared dbt Cloud staging job for DATE
	@python3 dbt/run_cloud_job.py stage --date $(DATE)

dbt-intermediate: ## Run the shared dbt Cloud intermediate job for DATE
	@python3 dbt/run_cloud_job.py intermediate --date $(DATE)

dbt-gold: ## Run the shared dbt Cloud Gold job for DATE
	@python3 dbt/run_cloud_job.py gold --date $(DATE)

databricks-export: ## Run the shared Azure Databricks replenishment export job
	@python3 databricks/run_job.py export --date $(DATE)

deliver: ## Deliver the Azure order object to WMS SFTP
	$(TOOL) python -m demo.cli deliver --date $(DATE)

run-airflow: ## Trigger Control Plane A for DATE
	$(COMPOSE) exec airflow airflow dags trigger trade_close_to_replenishment --conf '{"trading_date":"$(DATE)"}'

controlm-render: ## Render shared dbt Cloud job IDs into ignored Control-M JSON
	@python3 controlm/build.py --output $(CONTROL_M_RENDERED)

controlm-build: controlm-render ## Validate the workflow through Control-M Automation API
	ctm build $(CONTROL_M_RENDERED) controlm/descriptors/dev.json -e $${CTM_ENV:-se-dev}

controlm-deploy: controlm-render ## Deploy the workflow to the connected tenant (external change)
	ctm deploy $(CONTROL_M_RENDERED) controlm/descriptors/dev.json -e $${CTM_ENV:-se-dev}

run-controlm: ## Order Control Plane B for DATE in the connected tenant
	@./controlm/scripts/order_workflow.sh $(DATE)

controlm-service: ## Install/start the host Agent service (requires sudo)
	@./scripts/install_controlm_service.sh

install-databricks-cli: ## Install the pinned Azure Databricks CLI for this user
	@./scripts/install_databricks_cli.sh

databricks-provision: ## Create/update the Azure cluster and the two shared jobs
	@python3 databricks/provision_cluster.py
	@python3 databricks/provision_jobs.py

dbt-cloud-connect: databricks-provision ## Create/update the dbt Cloud Databricks connection
	@python3 dbt/provision_cloud_databricks.py

dbt-cloud-publish: ## Publish the dbt project to its shared deployment branch
	@./scripts/publish_dbt_branch.sh

dbt-cloud-provision: databricks-provision ## Create/update the three shared dbt Cloud jobs
	@python3 dbt/provision_jobs.py

controlm-dbt-provision: ## Store/test the dbt Cloud token in a Control-M profile
	@python3 controlm/provision_dbt.py

controlm-dbt-trust: ## Trust the public dbt Cloud CA in the host Agent plug-in
	@./scripts/configure_controlm_dbt_trust.sh

demo-airflow: ## Arm, trigger Airflow, then publish the DATE inputs
	$(MAKE) up
	$(MAKE) health
	$(MAKE) seed DATE=$(DATE)
	$(MAKE) eod-readiness-arm DATE=$(DATE)
	$(MAKE) run-airflow DATE=$(DATE)
	$(MAKE) simulate DATE=$(DATE)

demo-controlm: ## Arm, order Control-M, then publish the DATE inputs
	$(MAKE) up
	$(MAKE) health
	$(MAKE) controlm-health
	$(MAKE) seed DATE=$(DATE)
	$(MAKE) eod-readiness-arm DATE=$(DATE)
	$(MAKE) run-controlm DATE=$(DATE)
	$(MAKE) simulate DATE=$(DATE)

wms-ack: ## Configure normal WMS acknowledgement behaviour
	$(TOOL) python -m demo.cli wms-mode ack --delay 2

wms-never-ack: ## Configure WMS to receive without acknowledging
	$(TOOL) python -m demo.cli wms-mode never_ack

wms-late: ## Configure WMS to acknowledge after at least 30 seconds
	$(TOOL) python -m demo.cli wms-mode late

wms-reject: ## Configure WMS to produce an explicit rejection file
	$(TOOL) python -m demo.cli wms-mode reject

fail-1: ## Withhold STORES EOD markers from the next simulation
	@./failures/f1_late_store.sh $(STORES) $(DATE)

fail-2: ## Remove and suppress the supplier ASN for DATE
	@./failures/f2_no_asn.sh $(DATE)

fail-3: ## Publish an ASN with an unannounced carton_id column
	@./failures/f3_schema_drift.sh $(DATE)

fail-4: ## Inject ROWS negative stock positions after snapshotting
	@./failures/f4_phantom_stock.sh $(ROWS) $(DATE)

fail-5: ## Add SECONDS of Azure Databricks ingest delay
	@./failures/f5_slow_cluster.sh $(SECONDS)

reset: ## Reverse every failure and return DATE inputs to green
	@./failures/reset.sh $(DATE)

seed-sla-history: ## Order N successful Control-M runs for SLA history
	@./scripts/seed_sla_history.sh $(N) $(DATE)

lint: ## Validate Python, Compose, shell and JSON syntax without cloud changes
	python3 -m compileall -q demo airflow/dags databricks dbt controlm
	python3 -m json.tool controlm/workflows/trade_close_to_replenishment.json >/dev/null
	$(COMPOSE) config --quiet
	@for script in controlm/scripts/*.sh failures/*.sh scripts/*.sh; do bash -n "$$script"; done

test: lint ## Run local contract tests in the toolbox image
	$(COMPOSE) run --rm --build toolbox python -m unittest discover -s tests -v
