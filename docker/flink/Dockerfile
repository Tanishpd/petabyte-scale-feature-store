# Flink 2.2.1 + PyFlink + sentence-transformers + Feast's Redis codec.
#
# Base is eclipse-temurin:17-jre-noble (Ubuntu 24.04). Two consequences drive
# this file, both documented in ../../docs/DECISIONS.md:
#
#   ADR-001  noble has NO python3.11 package — only 3.12. The container runs
#            3.12 while the host venv runs 3.11. Only the apache-flink version
#            must match the cluster; the interpreter version need not.
#   ADR-009  apache-flink has never shipped a Linux aarch64 wheel. On Apple
#            Silicon pip falls back to the sdist, which ships pre-generated .c
#            files and therefore still compiles C extensions. The base is
#            JRE-only, so build-essential + python3.12-dev are MANDATORY.
#            Expect a slow first build. Do not "fix" this by changing the
#            Flink version — no version has an aarch64 wheel.

FROM flink:2.2.1-scala_2.12-java17

USER root

# --- system deps -----------------------------------------------------------
# build-essential + python3.12-dev exist solely to compile the PyFlink sdist.
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3.12 \
        python3.12-dev \
        python3.12-venv \
        build-essential \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# --- Kafka SQL connector ---------------------------------------------------
# Version suffix is <connector>-<flink-minor> and must match the cluster (ADR-002).
# Placed before the pip layer: small, stable, and rarely invalidated.
ARG KAFKA_CONNECTOR_VERSION=5.0.0-2.2
RUN curl -fSL -o /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
        "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
    && ls -l /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar

# --- Python environment ----------------------------------------------------
# A venv rather than --break-system-packages: Ubuntu 24.04 marks the system
# interpreter externally-managed (PEP 668), and PyFlink resolves `python` from
# PATH, so putting the venv first is all the wiring needed.
ENV VIRTUAL_ENV=/opt/venv
RUN python3.12 -m venv "$VIRTUAL_ENV"
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# The slow layer: compiles apache-flink's C extensions on arm64 (ADR-009).
COPY requirements-flink.txt /tmp/requirements-flink.txt
RUN pip install --no-cache-dir -r /tmp/requirements-flink.txt

# --- bake the embedding model ----------------------------------------------
# Invariant 6: a UDF must never reach the network on first call — that turns a
# cold start into a task-manager timeout. Download at build time, then go offline.
ENV HF_HOME=/opt/models
ARG EMBEDDING_MODEL=sentence-transformers/all-MiniLM-L6-v2
RUN python -c "\
from sentence_transformers import SentenceTransformer; \
m = SentenceTransformer('${EMBEDDING_MODEL}'); \
v = m.encode(['warm the cache and prove it loads']); \
print('baked', '${EMBEDDING_MODEL}', 'dim=', v.shape[-1])"
ENV HF_HUB_OFFLINE=1
ENV TRANSFORMERS_OFFLINE=1

# --- optional plugins ------------------------------------------------------
# Flink ships these in /opt/flink/opt/ but only LOADS plugins from
# /opt/flink/plugins/<name>/. Copying is the documented activation step; without
# it `s3://` checkpoint paths fail with "Could not find a file system
# implementation for scheme 's3'".
#
# Only the presto S3 filesystem, not hadoop: both register the `s3://` scheme
# and installing both makes the resolution ambiguous. Presto is the one Flink
# recommends for checkpointing.
RUN mkdir -p /opt/flink/plugins/s3-fs-presto \
    && cp /opt/flink/opt/flink-s3-fs-presto-*.jar /opt/flink/plugins/s3-fs-presto/ \
    && ls /opt/flink/plugins/s3-fs-presto/

# --- checkpoint storage ----------------------------------------------------
# The compose file mounts a named volume here. Docker seeds a fresh named
# volume's ownership from the image's directory, so this mkdir+chown is what
# makes the volume writable by the `flink` user. Without it the JobManager
# fails at job start with:
#   FlinkRuntimeException: Failed to create checkpoint storage at checkpoint
#   coordinator side ... Failed to create directory for shared state
RUN mkdir -p /flink-state/checkpoints /flink-state/savepoints \
    && chown -R flink:flink /flink-state

# Flink launches Python workers as `python`; make sure it is the venv one.
ENV PYFLINK_PYTHON=/opt/venv/bin/python

# Sanity: fail the build rather than at job-submission time.
RUN python -c "import pyflink, feast, sentence_transformers, redis; print('pyflink', pyflink.__version__)" \
    && python -c "from feast.infra.online_stores.helpers import _redis_key, _mmh3; print('feast redis codec importable')"

USER flink
