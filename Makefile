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
VERIFY_TOPIC ?= transactions_verify

# Host-side Redis port. 6380 by default so the stack coexists with a system or
# Homebrew redis on 6379. Inside the compose network it is always redis:6379.
REDIS_HOST_PORT ?= 6380
export REDIS_HOST_PORT

# `make up UI=1` also starts kafka-ui.
ifdef UI
PROFILES := --profile ui
else
PROFILES :=
endif

.DEFAULT_GOAL := help
.PHONY: help bootstrap build up down restart ps logs health topics sample tail-topic topic-count \
        produce submit apply seed registry read bench test smoke clean nuke \
        verify-seed verify-windows bench-embed

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
	@printf "redis   ($(REDIS_HOST_PORT))               : "; \
	  (redis-cli -p $(REDIS_HOST_PORT) ping 2>/dev/null | grep -q PONG && echo PONG) || echo "no response"
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

produce:  ## Stream mock transactions into Kafka (EPS=n, DURATION=s)
	$(PY) src/producer/generate_stream.py --eps $(EPS) --topic $(TOPIC) \
	  $(if $(DURATION),--duration $(DURATION),)

sample:  ## Print a few generated events without touching Kafka
	$(PY) src/producer/generate_stream.py --dry-run

tail-topic:  ## Show N recent events as JSON (N=5 default)
	@# --from-beginning is required: the console consumer defaults to the LATEST
	@# offset and would block forever when no producer is running. -T avoids a
	@# TTY that hangs in non-interactive shells. timeout-ms bounds an empty topic.
	@$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
	  --bootstrap-server localhost:9092 --topic $(TOPIC) --from-beginning \
	  --max-messages $(or $(N),5) --timeout-ms 10000 2>/dev/null \
	  || echo "(topic empty or fewer than $(or $(N),5) records — run 'make produce')"

topic-count:  ## Total records currently in the topic
	@$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
	  --bootstrap-server localhost:9092 --topic $(TOPIC) \
	  | awk -F: '{s+=$$3} END {print "records:", s+0}'

seed:  ## Generate the offline parquet backing the FeatureView's batch source
	$(PY) scripts/seed_offline.py $(if $(HOURS),--hours $(HOURS),) $(if $(USERS),--users $(USERS),)

apply:  ## Register feature definitions with Feast
	@test -f src/feast_repo/data/user_txn_stats_10m.parquet \
	  || { echo "No batch source yet — run 'make seed' first."; exit 1; }
	cd src/feast_repo && ../../$(VENV)/bin/feast apply

registry:  ## Show what is registered
	@cd src/feast_repo && ../../$(VENV)/bin/feast feature-views list \
	  && echo && ../../$(VENV)/bin/feast entities list

submit:  ## Submit the PyFlink job (FROM_START=1 to replay, JOB_TOPIC=x)
	@test -f src/flink/streaming_job.py \
	  || { echo "Not built yet — Phase 4 in PLAN.md."; exit 1; }
	$(COMPOSE) exec -T jobmanager flink run -d -py /opt/project/src/flink/streaming_job.py \
	  --no-wait --sink $(or $(SINK),redis) \
	  $(if $(FROM_START),--startup-mode earliest-offset,) $(if $(JOB_TOPIC),--topic $(JOB_TOPIC),)

read:  ## Read features back the way a model would (USERS="1 2 3")
	$(PY) src/serving/online_read.py $(if $(USERS),--users $(USERS),)

bench:  ## p50/p95/p99 online read latency + Redis footprint
	$(PY) src/serving/benchmark.py $(if $(ITERS),--iterations $(ITERS),)

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

# ---------------------------------------------------------------- Phase 4 ---

verify-seed:  ## Backfill a small, ordered history into the verification topic
	@$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server localhost:9092 --create --if-not-exists \
	  --topic $(VERIFY_TOPIC) --partitions 3 --replication-factor 1 \
	  --config retention.ms=86400000 >/dev/null   # 24h: must outlive a work session
	$(PY) src/producer/generate_stream.py --topic $(VERIFY_TOPIC) \
	  --backfill $(or $(SPAN),1200) --eps $(or $(VEPS),40) --users $(or $(VUSERS),150) \
	  --late-rate 0 --seed 42

verify-windows:  ## Prove Flink's aggregates match a pandas recomputation
	$(COMPOSE) exec -T jobmanager python /opt/project/src/flink/verify_windows.py \
	  --topic $(VERIFY_TOPIC)

bench-embed:  ## Measure embedding throughput and prove batching pays
	$(COMPOSE) exec -T jobmanager python /opt/project/src/flink/bench_embeddings.py \
	  --topic $(VERIFY_TOPIC) --parallelism $(or $(PAR),2)
