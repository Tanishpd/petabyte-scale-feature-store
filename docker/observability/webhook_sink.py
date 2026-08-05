#!/usr/bin/env python3
"""Minimal Alertmanager webhook receiver, so alert DELIVERY can be asserted.

This exists to make one claim checkable: an alert that fires actually arrives
somewhere. Before it, `alerts.yml` was evaluated correctly by Prometheus and
then dropped on the floor — there was no `alerting:` block, no Alertmanager, and
no receiver. Every drill in this repo proves a property by observing it from
outside, and this is the piece that lets the alerting path be observed the same
way instead of being asserted from configuration.

It is deliberately stdlib-only and tiny: it runs in the stock `python:3.12-slim`
image with no build step, and there is nothing in it that can fail in an
interesting way. In a real deployment this is replaced by Slack/PagerDuty and
nothing else about the topology changes.

Endpoints:
    POST /alert    receive an Alertmanager notification; append it to the log
    GET  /alerts   the alertnames received so far, newest first, as JSON
    GET  /healthz  liveness

State is in memory on purpose. A restart forgetting past alerts is correct for
a delivery probe: it should only ever assert on alerts that arrived *since* it
started, never on a stale record from a previous run.
"""
from __future__ import annotations

import json
import threading
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = 9099
_LOCK = threading.Lock()
# Bounded: a firing Watchdog repeats every couple of minutes forever, so an
# unbounded list is a slow leak in a container meant to run for days.
_RECEIVED: deque[dict] = deque(maxlen=500)


class Handler(BaseHTTPRequestHandler):
    def _json(self, code: int, payload) -> None:
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:  # noqa: N802  (stdlib naming)
        if self.path.rstrip("/") != "/alert":
            self._json(404, {"error": "not found"})
            return
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            # Record it anyway. A malformed notification is itself a finding,
            # and swallowing it would make the sink lie by omission.
            payload = {"_unparseable": raw.decode("utf-8", "replace")[:400]}

        entries = []
        for alert in payload.get("alerts", []) or [{}]:
            labels = alert.get("labels", {})
            entries.append({
                "alertname": labels.get("alertname", "?"),
                "severity": labels.get("severity", "?"),
                "status": alert.get("status", payload.get("status", "?")),
                "summary": (alert.get("annotations") or {}).get("summary", ""),
            })
        with _LOCK:
            _RECEIVED.extend(entries)
        for e in entries:
            print(f"ALERT {e['status']:8} {e['alertname']:24} {e['summary']}", flush=True)
        self._json(200, {"received": len(entries)})

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.rstrip("/") or "/"
        if path == "/healthz":
            self._json(200, {"ok": True})
        elif path == "/alerts":
            with _LOCK:
                self._json(200, list(reversed(_RECEIVED)))
        else:
            self._json(404, {"error": "not found"})

    def log_message(self, *_args) -> None:
        """Silence per-request logging; the ALERT lines above are the signal."""


if __name__ == "__main__":
    print(f"alert-sink listening on :{PORT}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
