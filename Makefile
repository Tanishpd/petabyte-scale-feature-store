# The Makefile is the interface. Anything a human might run twice lives here,
# and the docs cite `make x` rather than raw docker/pip commands.
#
# macOS ships GNU Make 3.81 — keep to portable syntax (no .RECIPEPREFIX, no
# .ONESHELL). Recipe lines are TAB-indented.

COMPOSE      := docker compose -f docker/docker-compose.yml
VENV         := .venv
PY           := $(VENV)/bin/python
PIP          := $(VENV)/bin/pip
HOST_PYTHON  ?= python3.11

TOPIC        ?= transactions
PARTITIONS   ?= 6
RETENTION_MS ?= 3600000
EPS          ?= 5000

# `make up UI=1` also starts kafka-ui.
ifdef UI
PROFILES := --profile ui
else
PROFILES :=
endif

.DEFAULT_GOAL := help
.PHONY: help bootstrap build up down restart ps logs health topics \
        produce submit apply read bench test smoke clean nuke

help:  ## Show this help
	@echo "Petabyte-Scale Feature Store — available targets:"
	@echo
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | sort \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "Vars: EPS=$(EPS) TOPIC=$(TOPIC) PARTITIONS=$(PARTITIONS)  |  UI=1 adds kafka-ui"

# ---------------------------------------------------------------- Phase 0 ---

bootstrap:  ## Create the host venv (Python 3.11) and install host deps
	@command -v $(HOST_PYTHON) >/dev/null 2>&1 || { \
	  echo "ERROR: $(HOST_PYTHON) not found. See docs/DECISIONS.md ADR-001."; exit 1; }
	$(HOST_PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip setuptools wheel
	$(PIP) install -r requirements-host.txt
	@echo
	@$(PY) -c "import feast, kafka, redis, pandas; print('host venv OK — feast', feast.__version__)"

# ---------------------------------------------------------------- Phase 1 ---

build:  ## Build the custom Flink image (slow on arm64 — compiles PyFlink, see ADR-009)
	@echo ">>> Building petafs/flink:2.2.1. On Apple Silicon this compiles the"
	@echo ">>> PyFlink sdist from source and takes a while. See docs/DECISIONS.md ADR-009."
	$(COMPOSE) build jobmanager

up:  ## Start the stack and wait until healthy
	@docker info >/dev/null 2>&1 || { echo "ERROR: Docker daemon not running. Start Docker Desktop."; exit 1; }
	$(COMPOSE) $(PROFILES) up -d
	@$(MAKE) --no-print-directory health

down:  ## Stop the stack (volumes preserved)
	$(COMPOSE) $(PROFILES) down

restart: down up  ## Stop then start

ps:  ## Container status
	$(COMPOSE) ps

logs:  ## Tail logs — `make logs S=taskmanager` for one service
	$(COMPOSE) logs -f --tail=100 $(S)

health:  ## Report health of each service
	@echo "--- container health ---"
	@$(COMPOSE) ps --format 'table {{.Service}}\t{{.Status}}'
	@echo
	@printf "kafka   (host listener 29092) : "; \
	  (nc -z localhost 29092 2>/dev/null && echo reachable) || echo "NOT reachable"
	@printf "redis   (6379)               : "; \
	  (redis-cli -p 6379 ping 2>/dev/null | grep -q PONG && echo PONG) || echo "no response"
	@printf "flink   (ui 8081)            : "; \
	  (curl -fs http://localhost:8081/overview >/dev/null 2>&1 && echo up) || echo "not up"

topics:  ## Create the transactions topic with retention caps
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server localhost:9092 --create --if-not-exists \
	  --topic $(TOPIC) --partitions $(PARTITIONS) --replication-factor 1 \
	  --config retention.ms=$(RETENTION_MS) --config retention.bytes=2147483648
	@echo "--- topics ---"
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server localhost:9092 --describe --topic $(TOPIC)

# --------------------------------------------------------- Phases 2 and on ---
# Defined so the interface is visible, but honest about not existing yet.

produce:  ## [Phase 2] Stream mock transactions into Kafka
	@test -f src/producer/generate_stream.py \
	  || { echo "Not built yet — Phase 2 in PLAN.md."; exit 1; }
	$(PY) src/producer/generate_stream.py --eps $(EPS) --topic $(TOPIC)

apply:  ## [Phase 3] Register feature definitions with Feast
	@test -f src/feast_repo/feature_store.yaml \
	  || { echo "Not built yet — Phase 3 in PLAN.md."; exit 1; }
	cd src/feast_repo && ../../$(VENV)/bin/feast apply

submit:  ## [Phase 4-6] Submit the PyFlink job into the cluster
	@test -f src/flink/streaming_job.py \
	  || { echo "Not built yet — Phase 4 in PLAN.md."; exit 1; }
	$(COMPOSE) exec jobmanager flink run -py /opt/project/src/flink/streaming_job.py

read:  ## [Phase 7] Read features back the way a model would
	@test -f src/serving/online_read.py \
	  || { echo "Not built yet — Phase 7 in PLAN.md."; exit 1; }
	$(PY) src/serving/online_read.py

bench:  ## [Phase 7] p50/p95/p99 online read latency
	@test -f src/serving/benchmark.py \
	  || { echo "Not built yet — Phase 7 in PLAN.md."; exit 1; }
	$(PY) src/serving/benchmark.py

test:  ## Run pytest (includes the Feast codec round-trip)
	$(VENV)/bin/pytest -q tests/

smoke:  ## End-to-end: produce -> flink -> redis -> feast read
	@test -f scripts/smoke_test.sh \
	  || { echo "Not built yet — Phase 6 in PLAN.md."; exit 1; }
	bash scripts/smoke_test.sh

# ------------------------------------------------------------------ misc ---

clean:  ## Remove the stack and its volumes (keeps images)
	$(COMPOSE) $(PROFILES) down -v

nuke: clean  ## clean + drop the built Flink image and the host venv
	-docker image rm petafs/flink:2.2.1
	rm -rf $(VENV)
