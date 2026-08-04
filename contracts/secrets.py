"""Read a credential from a file when one is offered, from the environment otherwise.

`SECRET_FILE` beats `SECRET`, which beats the default. That ordering is the
Docker convention (`_FILE`-suffixed variables), and it is what lets the same
code run against `docker compose secrets:`, a Kubernetes secret projected as a
volume, or a Vault agent writing to a tmpfs — without any of them knowing about
each other.

WHY FILES BEAT ENVIRONMENT VARIABLES FOR THIS
---------------------------------------------
An environment variable holding a password is readable by anything that can ask
the orchestrator about the container:

    docker inspect <container> | grep PASSWORD        # plaintext
    docker compose config                              # plaintext
    cat /proc/1/environ                                # plaintext, any process

and it lands in `docker compose config` output, which is exactly the thing
people paste into issues. A file mounted at /run/secrets is visible to the
processes in that container and to nothing else — not to `docker inspect`, not
to the compose file, and not to version control.

WHAT THIS DOES NOT DO
---------------------
It does not rotate, expire, audit, or seal anything. The file still sits on a
host filesystem, and any process inside the container can read it. That is a
real improvement over environment variables and it is not a secret manager; a
production deployment points the same `_FILE` variables at whatever its
platform projects (K8s secret, Vault agent, SOPS-decrypted tmpfs).

Stdlib only, like the rest of `contracts/` — the Python 3.11 host venv and the
3.12 Flink container both import it.
"""

from __future__ import annotations

import os

__all__ = ["read_secret"]


def read_secret(name: str, default: str | None = None) -> str | None:
    """Return the credential `name`, preferring `${name}_FILE`.

    >>> read_secret("REDIS_PASSWORD")            # reads REDIS_PASSWORD_FILE if set
    """
    path = os.environ.get(f"{name}_FILE", "").strip()
    if path:
        try:
            with open(path, encoding="utf-8") as fh:
                # Trailing newline is near-universal in generated secret files
                # and is NOT part of the password. Redis and Kafka would both
                # accept the value and then reject every client that sent the
                # stripped one, which reads as "wrong password" on the client
                # and logs nothing useful on the server.
                return fh.read().strip("\r\n")
        except OSError as exc:
            # Loud, because the alternative is falling back to a dev default
            # and starting a service whose credentials silently do not match
            # the ones every client was given.
            raise RuntimeError(
                f"{name}_FILE points at {path!r} which could not be read: {exc}"
            ) from exc

    value = os.environ.get(name)
    return value if value not in (None, "") else default
