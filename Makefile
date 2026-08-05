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
        secrets secrets-rotate \
        produce submit apply seed registry read bench test smoke clean nuke \
        verify-seed verify-windows bench-embed similar \
        prod-up prod-down prod-clean prod-ps prod-submit prod-drill prod-tls \
        mh-up mh-down mh-clean mh-ps mh-health mh-tls mh-topics mh-produce mh-submit mh-drill \
        mh-ha-up mh-ha-down mh-ha-clean mh-ha-ps mh-ha-health mh-ha-topics mh-ha-produce \
        mh-ha-submit mh-ha-drill mh-ha-acls

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

alerts-test:  ## Prove each alert rule FIRES on its condition (no stack needed)
	@# "rules loaded" is not evidence an alert works — the same configured-vs-
	@# enforced gap this repo probes for everywhere else. promtool replays
	@# synthetic series through the real rules file and checks both directions:
	@# fires on the condition, silent below the threshold.
	docker run --rm -v "$(PWD)/docker/observability:/obs:ro" -w /obs \
	  --entrypoint promtool prom/prometheus:v3.2.1 test rules alerts_test.yml

alerts-verify:  ## Assert every metric the alerts watch EXISTS (needs a running stack)
	@# The half promtool cannot cover: a rule over a misspelled metric passes
	@# every unit test and then never fires, because an empty query result is
	@# not an alert. Needs Prometheus up with the pipeline submitted.
	bash scripts/alerts_verify.sh

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

similar:  ## Find behaviourally similar users (USER=1 K=5) — RediSearch KNN
	$(PY) src/serving/similar_users.py $(if $(USER),--user $(USER),) $(if $(K),--k $(K),)

# ---------------------------------------------------- production topology ---
# Separate from the dev stack. See docs/RUNBOOK.md.

PROD_COMPOSE := docker compose -f docker/docker-compose.prod.yml
PROD_KAFKA   := kafka1:9092,kafka2:9092,kafka3:9092

prod-up:  ## Start the production topology (3 brokers, MinIO, Redis HA)
	@echo ">>> ~10 GiB of RAM. Stop the dev stack first: make down"
	$(PROD_COMPOSE) up -d
	@echo ">>> Flink UI http://localhost:8081   MinIO console http://localhost:9001"

prod-down:  ## Stop the production topology (volumes kept)
	$(PROD_COMPOSE) down

prod-clean:  ## Stop it and drop the volumes
	$(PROD_COMPOSE) down -v

prod-ps:  ## Production container status
	$(PROD_COMPOSE) ps

prod-topics:  ## Create `transactions` at RF=3 on the production topology
	@# `auto.create.topics.enable=false` here, deliberately — a topic created by
	@# accident gets the broker defaults, not RF=3, which quietly undoes the
	@# whole point of three brokers. The cost is that producing before this runs
	@# reports `0 eps` and a handful of client errors rather than "no such
	@# topic", so it is worth a target rather than a line in a doc.
	@#
	@# retention.bytes/segment.bytes are a LAPTOP CONCESSION, not production
	@# sizing — override them on a real box. A Kafka broker's resident size is
	@# dominated by mmap'd log segments rather than by KAFKA_HEAP_OPTS, so an
	@# uncapped topic grows the brokers until they crowd out everything else on
	@# the machine; that is what starved Flink into "Could not create actor
	@# system" on the HA topology, an error naming nothing about memory.
	@#   make prod-topics RETENTION_BYTES=10737418240 SEGMENT_BYTES=1073741824
	$(PROD_COMPOSE) exec -T kafka1 sh -c 'printf "security.protocol=SASL_SSL\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$${KAFKA_USER:-petafs}\" password=\"%s\";\nssl.truststore.location=/tls/kafka.truststore.jks\nssl.truststore.password=%s\n" "$$(cat /run/secrets/kafka_password)" "$$(cat /run/secrets/tls_storepass)" > /tmp/t.properties; \
	  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
	    --topic $(TOPIC) --partitions $(or $(PARTITIONS),6) --replication-factor 3 \
	    --config retention.bytes=$(or $(RETENTION_BYTES),268435456) \
	    --config segment.bytes=$(or $(SEGMENT_BYTES),67108864) \
	    --command-config /tmp/t.properties && \
	  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe \
	    --topic $(TOPIC) --command-config /tmp/t.properties | head -1'

prod-submit:  ## Submit the pipeline against the production topology
	$(PROD_COMPOSE) exec -T jobmanager flink run -d -py /opt/project/src/flink/streaming_job.py \
	  --no-wait --sink redis --bootstrap $(PROD_KAFKA) --topic $(TOPIC) \
	  $(if $(FROM_START),--startup-mode earliest-offset,)

prod-drill:  ## Run the failure drills (kill a broker, kill the redis master)
	bash scripts/failure_drills.sh

prod-tls:  ## Generate the local CA and service certs for the prod topology
	bash docker/tls/generate-certs.sh

# ------------------------------------------------------- multi-host topology ---
# Three Compose projects, three isolated networks, one per simulated host.
# Separate projects are the whole point: they get separate networks, so there is
# no Compose DNS between nodes and every address has to be routable — the same
# constraint as three real machines. See docs/MULTIHOST.md.

MH_COMPOSE := docker compose -f docker/docker-compose.multihost.yml --env-file docker/multihost/sim.env
MH_A       := $(MH_COMPOSE) -p petafs-a --profile a
MH_B       := $(MH_COMPOSE) -p petafs-b --profile b
MH_C       := $(MH_COMPOSE) -p petafs-c --profile c
# BROKER listeners — what in-cluster clients (Flink) dial.
MH_KAFKA   := host.docker.internal:19092,host.docker.internal:19192,host.docker.internal:19292
# HOST listeners — what scripts on this Mac dial, because macOS does not
# resolve host.docker.internal (verified). Same shape as Invariant 4.
MH_KAFKA_HOST := localhost:19094,localhost:19194,localhost:19294

mh-up:  ## Start the three-node topology (three isolated Compose projects)
	@docker info >/dev/null 2>&1 || { echo "ERROR: Docker daemon not running."; exit 1; }
	@test -f docker/tls/out/redis-node.crt || { echo "No certs — run 'make mh-tls' first."; exit 1; }
	@echo ">>> brokers first: the JobManager gates on kafka1 being healthy, and"
	@echo ">>> kafka1 cannot be healthy until the KRaft quorum has all three."
	$(MH_A) up -d kafka1 minio
	$(MH_B) up -d kafka2
	$(MH_C) up -d kafka3
	$(MH_A) up -d
	$(MH_B) up -d
	$(MH_C) up -d
	@$(MAKE) --no-print-directory mh-health

secrets:  ## Generate the credential files the authenticated topologies mount
	@bash scripts/gen_secrets.sh

secrets-rotate:  ## Regenerate ALL credentials (drops stacks + volumes; certs too)
	@echo ">>> This invalidates running stacks and the JKS keystores."
	rm -f docker/secrets/*
	@bash scripts/gen_secrets.sh
	@$(MAKE) --no-print-directory mh-tls
	@echo ">>> Redis persists its password into its config file on first boot,"
	@echo ">>> so drop the volumes too: make mh-ha-clean"

mh-tls:  ## Generate certs with the routable node names in their SANs
	NODE_SANS=$(or $(NODE_SANS),host.docker.internal) bash docker/tls/generate-certs.sh

mh-down:  ## Stop all three nodes (volumes kept)
	-$(MH_A) down
	-$(MH_B) down
	-$(MH_C) down

mh-clean:  ## Stop all three nodes and drop their volumes
	-$(MH_A) down -v
	-$(MH_B) down -v
	-$(MH_C) down -v

mh-ps:  ## Status of all three nodes
	@for n in a b c; do echo "--- node $$n ---"; \
	  docker compose -f docker/docker-compose.multihost.yml --env-file docker/multihost/sim.env \
	    -p petafs-$$n --profile $$n ps --format 'table {{.Service}}\t{{.Status}}' 2>/dev/null; echo; done

mh-health:  ## Cluster-formation summary across the three nodes
	@bash scripts/multihost_health.sh

mh-topics:  ## Create the transactions topic at RF=3 across the three nodes
	@bash scripts/multihost_health.sh --create-topic $(TOPIC) $(PARTITIONS)

mh-produce:  ## Produce into the multi-host cluster from this Mac (HOST listeners)
	KAFKA_SECURITY_PROTOCOL=SASL_SSL KAFKA_SASL_MECHANISM=PLAIN \
	KAFKA_USER=petafs KAFKA_PASSWORD=petafs-dev-secret \
	KAFKA_SSL_CAFILE=docker/tls/out/ca.crt \
	$(PY) src/producer/generate_stream.py --bootstrap $(MH_KAFKA_HOST) \
	  --topic $(TOPIC) --eps $(or $(EPS),400) $(if $(DURATION),--duration $(DURATION),) \
	  --acks all --users $(or $(USERS),400)

mh-submit:  ## Submit the pipeline against the multi-host topology
	$(MH_A) exec -T jobmanager flink run -d -py /opt/project/src/flink/streaming_job.py \
	  --no-wait --sink redis --bootstrap $(MH_KAFKA) --topic $(TOPIC) \
	  $(if $(FROM_START),--startup-mode earliest-offset,)

mh-drill:  ## Kill an entire node and assert what survived
	bash scripts/multihost_drill.sh

# ------------------------------------------- multi-host + JobManager HA ---
# Adds a ZooKeeper ensemble and a standby JobManager. Separate project names
# from the mh-* targets: the two topologies must never run at once on this
# machine — together they need ~9 GiB and Docker has 7.75 GiB.

MHHA_COMPOSE := docker compose -f docker/docker-compose.multihost-ha.yml --env-file docker/multihost/sim-ha.env
MHHA_A       := $(MHHA_COMPOSE) -p petafs-ha-a --profile a
MHHA_B       := $(MHHA_COMPOSE) -p petafs-ha-b --profile b
MHHA_C       := $(MHHA_COMPOSE) -p petafs-ha-c --profile c
MHHA_KAFKA   := host.docker.internal:19092,host.docker.internal:19192,host.docker.internal:19292

mh-ha-up:  ## Start the three-node HA topology (ZooKeeper + standby JobManager)
	@docker info >/dev/null 2>&1 || { echo "ERROR: Docker daemon not running."; exit 1; }
	@test -f docker/tls/out/redis-node.crt || { echo "No certs — run 'make mh-tls' first."; exit 1; }
	@docker ps --format '{{.Names}}' | grep -q '^petafs-mh-' \
	  && { echo "ERROR: the non-HA multi-host stack is up. 'make mh-down' first — both do not fit in 7.75 GiB."; exit 1; } || true
	@echo ">>> brokers, ZooKeeper and the object store first: all three form"
	@echo ">>> quorums, and nothing that gates on them can start until they have."
	$(MHHA_A) up -d kafka1 zookeeper-1 minio-a
	$(MHHA_B) up -d kafka2 zookeeper-2 minio-b
	$(MHHA_C) up -d kafka3 zookeeper-3 minio-c
	$(MHHA_A) up -d
	$(MHHA_B) up -d
	$(MHHA_C) up -d
	@$(MAKE) --no-print-directory mh-ha-health

mh-ha-down:  ## Stop the HA topology (volumes kept)
	-$(MHHA_A) down
	-$(MHHA_B) down
	-$(MHHA_C) down

mh-ha-clean:  ## Stop the HA topology and drop its volumes
	-$(MHHA_A) down -v
	-$(MHHA_B) down -v
	-$(MHHA_C) down -v
	@# `down -v` leaves the volumes behind if any container was mid-restart when
	@# it ran, and it reports success either way. The stale Kafka logs then
	@# survive into the next run, which looks like "the topic config I just set
	@# was ignored". Sweep explicitly and verify.
	@# Containers first: a killed-but-not-removed container still holds its
	@# volume, so removing volumes alone fails with "volume is in use".
	-@docker ps -aq --filter name=petafs-mhha- | xargs -r docker rm -f >/dev/null 2>&1
	-@docker volume ls -q --filter name=petafs-ha- | xargs -r docker volume rm -f >/dev/null 2>&1
	@n=$$(docker volume ls -q --filter name=petafs-ha- | wc -l | tr -d ' '); \
	  if [ "$$n" -eq 0 ]; then echo "all HA volumes removed"; \
	  else echo "WARNING: $$n HA volume(s) still present:"; docker volume ls -q --filter name=petafs-ha-; fi

mh-ha-ps:  ## Status of all three HA nodes
	@for n in a b c; do echo "--- node $$n ---"; \
	  docker compose -f docker/docker-compose.multihost-ha.yml --env-file docker/multihost/sim-ha.env \
	    -p petafs-ha-$$n --profile $$n ps --format 'table {{.Service}}\t{{.Status}}' 2>/dev/null; echo; done

mh-ha-health:  ## ZooKeeper quorum, JobManager leadership, cluster formation
	@bash scripts/multihost_ha_health.sh

mh-ha-topics:  ## Create the transactions topic at RF=3 on the HA topology
	@bash scripts/multihost_ha_health.sh --create-topic $(TOPIC) $(PARTITIONS)

mh-ha-acls:  ## Apply least-privilege Kafka ACLs (deny-by-default is already on)
	@bash scripts/multihost_ha_health.sh --apply-acls $(TOPIC) petafs-flink

mh-ha-produce:  ## Produce into the HA topology from this Mac (HOST listeners)
	@test -f docker/secrets/kafka_producer_password || { echo "No secrets — run 'make secrets'."; exit 1; }
	@# KAFKA_PASSWORD_FILE, not KAFKA_PASSWORD: the value never enters this
	@# process's environment, so it cannot leak via `ps -e` or a crash dump.
	@# contracts/secrets.py resolves it.
	KAFKA_SECURITY_PROTOCOL=SASL_SSL KAFKA_SASL_MECHANISM=PLAIN \
	KAFKA_USER=petafs-producer KAFKA_PASSWORD_FILE=docker/secrets/kafka_producer_password \
	KAFKA_SSL_CAFILE=docker/tls/out/ca.crt \
	$(PY) src/producer/generate_stream.py --bootstrap localhost:19094,localhost:19194,localhost:19294 \
	  --topic $(TOPIC) --eps $(or $(EPS),400) $(if $(DURATION),--duration $(DURATION),) \
	  --acks all --users $(or $(USERS),400)

mh-ha-submit:  ## Submit the pipeline against the HA topology
	@# --on-bad-record skip, unlike the dev stack. streaming_job.py argues the
	@# case in full: a schema mismatch must be LOUD in development, and in
	@# production one malformed record must not stop the pipeline. This topology
	@# is the production one, and the difference is not academic — a single
	@# unparseable byte on the topic put the job in a permanent restart loop
	@# (it re-reads the same offset from its checkpoint and dies again), which
	@# on this box took the load average past 160. `skip` counts what it drops.
	$(MHHA_B) exec -T jobmanager flink run -d -py /opt/project/src/flink/streaming_job.py \
	  --no-wait --sink redis --bootstrap $(MHHA_KAFKA) --topic $(TOPIC) \
	  --on-bad-record $(or $(ON_BAD_RECORD),skip) \
	  $(if $(FROM_START),--startup-mode earliest-offset,)

mh-ha-drill:  ## Kill the LEADING JobManager's whole node; assert automatic recovery
	bash scripts/multihost_ha_drill.sh
