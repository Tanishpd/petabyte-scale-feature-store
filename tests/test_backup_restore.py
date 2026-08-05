"""A backup you have never restored is not a backup.

These run the real scripts against a real archive — no mocks — because every
interesting failure here is in the parts a mock would replace: the manifest
format, the checksum comparison, and the refusal to write when verification
fails. Restoring a corrupt registry does not raise; Feast opens it happily and
then addresses the wrong Redis keys, so reads return None with no error
anywhere (Invariant 1). The verification step is the whole product.

No Docker, no Flink, no Feast — so this runs in CI alongside the rest.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

BACKUP = ROOT / "scripts" / "backup.sh"
RESTORE = ROOT / "scripts" / "restore.sh"
DATA = ROOT / "src" / "feast_repo" / "data"


def _run(script: Path, *args: str, env_extra: dict | None = None, cwd: Path | None = None):
    env = dict(os.environ)
    env.update(env_extra or {})
    return subprocess.run(
        ["bash", str(script), *args],
        capture_output=True, text=True, env=env, cwd=str(cwd or ROOT),
    )


@pytest.fixture
def sandbox(tmp_path):
    """A throwaway copy of the repo layout, so a test never touches real state.

    The scripts resolve paths from their own location, so the sandbox has to be
    a real tree with the scripts in it rather than a cwd override.
    """
    for sub in ("scripts", "src/feast_repo/data", "backups"):
        (tmp_path / sub).mkdir(parents=True, exist_ok=True)
    for s in (BACKUP, RESTORE):
        shutil.copy(s, tmp_path / "scripts" / s.name)
    (tmp_path / "src/feast_repo/data/registry.db").write_bytes(b"REGISTRY-v1-content")
    (tmp_path / "src/feast_repo/data/user_txn_stats_10m.parquet").write_bytes(b"PARQUET" * 100)
    (tmp_path / "src/feast_repo/feature_store.yaml").write_text("project: petafs\n")
    return tmp_path


def _archive_in(sandbox: Path) -> Path:
    archives = sorted((sandbox / "backups").glob("*.tar.gz"))
    assert archives, "backup produced no archive"
    return archives[-1]


# --- the happy path -------------------------------------------------------

def test_backup_creates_an_archive_with_a_manifest(sandbox):
    r = _run(sandbox / "scripts/backup.sh", cwd=sandbox)
    assert r.returncode == 0, r.stderr
    archive = _archive_in(sandbox)
    listing = subprocess.run(["tar", "-tzf", str(archive)],
                             capture_output=True, text=True).stdout
    assert "MANIFEST" in listing
    assert "registry.db" in listing


def test_round_trip_restores_the_exact_bytes(sandbox):
    """The registry is a few KB and load-bearing; a byte-for-byte round trip is
    the claim that matters."""
    original = (sandbox / "src/feast_repo/data/registry.db").read_bytes()
    assert _run(sandbox / "scripts/backup.sh", cwd=sandbox).returncode == 0
    archive = _archive_in(sandbox)

    # Simulate the disaster: the registry is gone.
    (sandbox / "src/feast_repo/data/registry.db").unlink()

    r = _run(sandbox / "scripts/restore.sh", str(archive), cwd=sandbox)
    assert r.returncode == 0, r.stderr
    assert (sandbox / "src/feast_repo/data/registry.db").read_bytes() == original


def test_restore_preserves_the_parquet_too(sandbox):
    original = (sandbox / "src/feast_repo/data/user_txn_stats_10m.parquet").read_bytes()
    assert _run(sandbox / "scripts/backup.sh", cwd=sandbox).returncode == 0
    archive = _archive_in(sandbox)
    shutil.rmtree(sandbox / "src/feast_repo/data")
    assert _run(sandbox / "scripts/restore.sh", str(archive), cwd=sandbox).returncode == 0
    assert (sandbox / "src/feast_repo/data/user_txn_stats_10m.parquet").read_bytes() == original


# --- the part that actually protects you ----------------------------------

def test_a_corrupted_archive_is_refused_and_nothing_is_written(sandbox):
    """THE test. A restore that silently writes a corrupt registry is worse
    than no restore at all: Feast opens it, the hashes come out wrong, and every
    online read returns None with nothing logged."""
    assert _run(sandbox / "scripts/backup.sh", cwd=sandbox).returncode == 0
    archive = _archive_in(sandbox)

    # Rewrite one file inside the archive, leaving the manifest untouched.
    work = sandbox / "tamper"
    work.mkdir()
    subprocess.run(["tar", "-xzf", str(archive), "-C", str(work)], check=True)
    inner = next(work.iterdir())
    (inner / "feast" / "registry.db").write_bytes(b"CORRUPTED")
    archive.unlink()
    subprocess.run(["tar", "-czf", str(archive), "-C", str(work), inner.name], check=True)

    good = (sandbox / "src/feast_repo/data/registry.db").read_bytes()
    r = _run(sandbox / "scripts/restore.sh", str(archive), cwd=sandbox)
    assert r.returncode != 0, "a corrupt archive must not restore"
    assert "CORRUPT" in r.stdout or "CORRUPT" in r.stderr
    # and the good state is untouched
    assert (sandbox / "src/feast_repo/data/registry.db").read_bytes() == good


def test_an_archive_without_a_manifest_is_refused(sandbox):
    """Refuse anything this script did not write, rather than guessing at the
    layout of a tarball someone hand-rolled."""
    bogus = sandbox / "backups" / "not-ours.tar.gz"
    junk = sandbox / "junk"
    (junk / "stuff").mkdir(parents=True)
    (junk / "stuff" / "x").write_text("hello")
    subprocess.run(["tar", "-czf", str(bogus), "-C", str(junk), "stuff"], check=True)
    r = _run(sandbox / "scripts/restore.sh", str(bogus), cwd=sandbox)
    assert r.returncode != 0
    assert "MANIFEST" in r.stderr


def test_restore_moves_existing_state_aside_rather_than_clobbering(sandbox):
    """A restore of the wrong archive must itself be reversible."""
    assert _run(sandbox / "scripts/backup.sh", cwd=sandbox).returncode == 0
    archive = _archive_in(sandbox)
    (sandbox / "src/feast_repo/data/registry.db").write_bytes(b"NEWER-STATE")
    assert _run(sandbox / "scripts/restore.sh", str(archive), cwd=sandbox).returncode == 0
    kept = list((sandbox / "src/feast_repo").glob("data.pre-restore.*"))
    assert kept, "the previous state should have been preserved"
    assert (kept[0] / "registry.db").read_bytes() == b"NEWER-STATE"


# --- what must NOT be in the backup ---------------------------------------

def test_secrets_are_excluded_by_default(sandbox):
    """Credentials next to the data turn one stolen file into a full
    compromise. Opt in explicitly or not at all."""
    (sandbox / "docker" / "secrets").mkdir(parents=True)
    (sandbox / "docker" / "secrets" / "redis_password").write_text("hunter2")
    assert _run(sandbox / "scripts/backup.sh", cwd=sandbox).returncode == 0
    listing = subprocess.run(["tar", "-tzf", str(_archive_in(sandbox))],
                             capture_output=True, text=True).stdout
    assert "secrets/" not in listing


def test_secrets_are_included_only_when_asked(sandbox):
    (sandbox / "docker" / "secrets").mkdir(parents=True)
    (sandbox / "docker" / "secrets" / "redis_password").write_text("hunter2")
    assert _run(sandbox / "scripts/backup.sh", env_extra={"INCLUDE_SECRETS": "1"},
                cwd=sandbox).returncode == 0
    listing = subprocess.run(["tar", "-tzf", str(_archive_in(sandbox))],
                             capture_output=True, text=True).stdout
    assert "secrets/redis_password" in listing


def test_backup_refuses_when_there_is_no_registry(sandbox):
    """Silently producing an empty archive is how you discover at restore time
    that there was nothing to restore."""
    (sandbox / "src/feast_repo/data/registry.db").unlink()
    r = _run(sandbox / "scripts/backup.sh", cwd=sandbox)
    assert r.returncode != 0
    assert "registry" in r.stderr.lower()
