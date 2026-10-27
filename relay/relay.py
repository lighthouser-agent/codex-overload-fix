#!/usr/bin/env python3
"""Local retry relay for Codex /responses.

Listens on http://127.0.0.1:<port> and forwards to
https://chatgpt.com/backend-api/codex, retrying when the upstream answers
with a server-overload ("Selected model is at capacity") response instead of
handing that failure straight to Codex. scripts/cchost.py passes --upstream
for CCHost.

Everything is byte-transparent: request bodies (including zstd-compressed
ones) and auth headers are forwarded untouched. Only the very start of the
SSE stream is inspected to decide whether the attempt succeeded.
"""

import argparse
import contextlib
import gzip
import http.client
import json
import os
import random
import select
import socket
import socketserver
import sys
import threading
import time
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

from sse import LateOverload, SSETail, StreamAttempt

HOP_BY_HOP = {
    "connection",
    "proxy-connection",
    "keep-alive",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
    "accept-encoding",
}

LOG_LOCK = threading.Lock()
LOG_SIZE = [0]
LOG_MAX = 8 * 1024 * 1024


def log(msg):
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}"
    with LOG_LOCK:
        with contextlib.suppress(OSError, ValueError):
            print(line, flush=True)
        if ARGS.log:
            try:
                if LOG_SIZE[0] > LOG_MAX:
                    os.replace(ARGS.log, ARGS.log + ".1")
                    LOG_SIZE[0] = 0
                with open(ARGS.log, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
                LOG_SIZE[0] += len(line) + 1
            except OSError:
                # The CCHost service discards stdout, so stderr is the only record left.
                with contextlib.suppress(OSError, ValueError):
                    print(line, file=sys.stderr, flush=True)


ARGS = None


def upstream_connection(timeout):
    host = ARGS.upstream_host
    port = ARGS.upstream_port
    if ARGS.proxy:
        phost, pport = ARGS.proxy
        conn = http.client.HTTPSConnection(phost, pport, timeout=timeout)
        conn.set_tunnel(host, port)
        return conn
    if ARGS.upstream_scheme == "http":
        return http.client.HTTPConnection(host, port, timeout=timeout)
    return http.client.HTTPSConnection(host, port, timeout=timeout)


def read_request_body(handler):
    te = (handler.headers.get("Transfer-Encoding") or "").lower()
    if "chunked" in te:
        chunks = []
        while True:
            line = handler.rfile.readline(65536)
            if not line:
                break
            size = int(line.split(b";")[0].strip() or b"0", 16)
            if size == 0:
                handler.rfile.readline(65536)
                break
            chunks.append(handler.rfile.read(size))
            handler.rfile.readline(65536)
        return b"".join(chunks)
    length = int(handler.headers.get("Content-Length") or 0)
    return handler.rfile.read(length) if length else b""


def clean_headers(headers):
    blocked = set(HOP_BY_HOP)
    for key, value in headers.items():
        if key.lower() == "connection":
            blocked.update(part.strip().lower() for part in value.split(","))
    return {key: value for key, value in headers.items() if key.lower() not in blocked}


def forward_headers(handler):
    out = clean_headers(handler.headers)
    out["Accept-Encoding"] = "identity"
    return out


def analyze(handler, body):
    """One-line size breakdown of a /responses body. Sizes only, no content."""
    if len(body) > 64 << 20:
        return f" | analyze-skipped({len(body)}B too large)"
    enc = (handler.headers.get("Content-Encoding") or "").lower()
    try:
        if enc in ("", "identity"):
            raw = body
        elif enc == "gzip":
            raw = gzip.decompress(body)
        elif enc == "deflate":
            raw = zlib.decompress(body)
        elif enc == "zstd":
            try:
                from compression import zstd
                raw = zstd.decompress(body)
            except Exception:
                import zstandard
                raw = zstandard.ZstdDecompressor().decompress(body, max_output_size=64 << 20)
        else:
            return f" enc={enc} analysis-skipped"
        doc = json.loads(raw)
    except Exception as exc:  # noqa: BLE001
        return f" analyze-failed({exc.__class__.__name__})"

    parts = []
    if doc.get("model"):
        parts.append(f"model={doc['model']}")
    _r = doc.get("reasoning") or {}
    if isinstance(_r, dict) and _r.get("effort"):
        parts.append(f"effort={_r['effort']}")
    if doc.get("service_tier"):
        parts.append(f"tier={doc['service_tier']}")
    if isinstance(doc.get("instructions"), str):
        parts.append(f"instructions={len(doc['instructions'])}c")
    tools = doc.get("tools") or []
    if tools:
        tb = len(json.dumps(tools, ensure_ascii=False).encode())
        parts.append(f"tools={tb}B/{len(tools)}")
    items = doc.get("input") or []
    if isinstance(items, list):
        by = {}
        for it in items:
            role = it.get("type") or "?"
            if role == "message":
                role = "msg:" + (it.get("role") or "?")
            b = len(json.dumps(it, ensure_ascii=False).encode())
            by[role] = by.get(role, 0) + b
        top = ",".join(f"{k}={v}" for k, v in sorted(by.items(), key=lambda kv: -kv[1])[:4])
        parts.append(f"input={len(json.dumps(items, ensure_ascii=False).encode())}B/{len(items)}it({top})")
    if doc.get("prompt_cache_key"):
        parts.append("cache_key=yes")
    return " | " + (" ".join(parts) if parts else "empty")


def request_summary(handler, body):
    """Upstream's analyze() line, guarded so it can neither block forwarding nor forge log lines."""
    try:
        summary = analyze(handler, body)
    except Exception as exc:  # noqa: BLE001 - e.g. a JSON body that is not an object
        return f" analyze-failed({exc.__class__.__name__})"
    return "".join(char if char.isprintable() else "?" for char in summary)[:2000]


class Handler(BaseHTTPRequestHandler):
    """Forward requests; retry only confirmed overload before observable output."""

    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass

    def _proxy(self):
        if self.command == "GET" and self.path == "/__codex_retry/health":
            body = json.dumps({"service": "cchost-codex-retry", "upstream": ARGS.upstream}).encode()
            self._send_simple(200, {"Content-Type": "application/json"}, body)
            return
        try:
            body = read_request_body(self)
        except (OSError, ValueError):
            self._send_simple(400, {"Content-Type": "text/plain"}, b"relay: invalid request body\n")
            return
        headers = forward_headers(self)
        path = ARGS.upstream_prefix + self.path
        if self.command == "POST" and self.path.split("?")[0].endswith("/responses"):
            self._retry_responses(path, headers, body)
        else:
            self._passthrough(path, headers, body)

    def _retry_responses(self, path, headers, body):
        port = self.client_address[1]
        log(f"[{port}] {self.command} {self.path} ({len(body)}B){request_summary(self, body)}")
        deadline = time.monotonic() + ARGS.total_budget
        for number in range(1, ARGS.max_attempts + 1):
            if self._client_gone():
                return
            commit_by = min(deadline, time.monotonic() + ARGS.commit_deadline)
            with StreamAttempt(upstream_connection, ARGS.timeout, commit_by, self._client_gone) as attempt:
                result = attempt.run(path, headers, body)
                for line in result.events:
                    log(f"[{port}]   {line}")
                if attempt.abort_reason == "client_closed":
                    log(f"[{port}] client cancelled")
                    return
                if result.mode == "stream":
                    log(f"[{port}] attempt {number}: ok, streaming ({result.reason}) [{result.detail}]")
                    self._stream_back(result, attempt.response, port)
                    return
            if result.mode != "retry":
                log(f"[{port}] attempt {number}: upstream {result.status}, passthrough")
                self._send_simple(result.status, result.headers, result.body)
                return
            log(f"[{port}] attempt {number}: retryable ({result.reason}) [{result.detail}]")
            delay = min(ARGS.backoff_max, ARGS.backoff_base * 1.6 ** min(number - 1, 30))
            delay = max(result.delay, delay * random.uniform(0.7, 1.3))
            if number == ARGS.max_attempts or time.monotonic() + delay >= deadline:
                log(f"[{port}] giving up after {number} attempts")
                self._send_simple(result.status, result.headers, result.body)
                return
            if not self._wait_retry(delay):
                log(f"[{port}] client cancelled during backoff")
                return

    def _wait_retry(self, delay):
        until = time.monotonic() + delay
        while time.monotonic() < until:
            if self._client_gone():
                return False
            time.sleep(min(0.05, max(0, until - time.monotonic())))
        return not self._client_gone()

    def _client_gone(self):
        try:
            readable, _, _ = select.select([self.connection], [], [], 0)
            return bool(readable) and self.connection.recv(1, socket.MSG_PEEK) == b""
        except (OSError, ValueError):
            return True

    def _passthrough(self, path, headers, body):
        started = time.time()
        conn = upstream_connection(ARGS.timeout)
        try:
            conn.request(self.command, path, body=body or None, headers=headers)
            resp = conn.getresponse()
            data = resp.read()
            self._send_simple(resp.status, dict(resp.getheaders()), data)
            log(f"[{self.client_address[1]}] {self.command} {path} -> {resp.status} "
                f"{len(data)}B in {time.time() - started:.1f}s")
        except Exception as exc:  # noqa: BLE001
            log(f"[{self.client_address[1]}] {self.command} {path} -> error {exc!r}")
            self._send_simple(502, {"Content-Type": "text/plain"}, f"relay: {exc}\n".encode())
        finally:
            conn.close()

    def _stream_back(self, result, response, port):
        tail = SSETail()
        tail.feed(result.body)
        late = None if ARGS.forward_late_overload else LateOverload(result.body)
        try:
            self._send_headers(result.status, result.headers)
            self.wfile.write(result.body)
            self.wfile.flush()
            while True:
                chunk = response.read1(65536)
                if not chunk:
                    break
                if late and late.check(chunk):
                    log(f"[{port}] mid-stream overload after commit -> dropping connection for client retry")
                    break
                self.wfile.write(chunk)
                self.wfile.flush()
                tail.feed(chunk)
        except (OSError, http.client.HTTPException):
            # The relay itself never replays a request after commit.
            log(f"[{port}] committed stream closed")
        log(f"[{port}] stream ended: {tail.events} events, last [{tail.last}]"
            + (f", failure [{tail.failure}]" if tail.failure else ""))

    def _send_headers(self, status, headers, length=None):
        self.send_response(status)
        for key, value in clean_headers(headers).items():
            self.send_header(key, value)
        if length is not None:
            self.send_header("Content-Length", str(length))
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True

    def _send_simple(self, status, headers, data):
        try:
            self._send_headers(status, headers, len(data))
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            pass

    do_GET = _proxy
    do_POST = _proxy
    do_PUT = _proxy
    do_DELETE = _proxy
    do_PATCH = _proxy


class LoopbackServer(ThreadingHTTPServer):
    """Bind without HTTPServer's getfqdn, a reverse DNS lookup that can stall startup."""

    daemon_threads = True

    def server_bind(self):
        socketserver.TCPServer.server_bind(self)
        self.server_name, self.server_port = self.server_address[:2]


def parse_upstream(value):
    parsed = urlsplit(value if "://" in value else "https://" + value)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError("upstream must be an HTTP(S) URL")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("upstream must not contain credentials, query or fragment")
    if parsed.scheme == "http" and parsed.hostname not in ("127.0.0.1", "localhost", "::1"):
        raise ValueError("non-local upstream requires HTTPS")
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    return parsed.scheme, parsed.hostname, port, parsed.path.rstrip("/")


def main():
    global ARGS
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=8787)
    p.add_argument("--upstream", default="chatgpt.com/backend-api/codex")
    p.add_argument("--proxy", default="", help="e.g. http://127.0.0.1:7897")
    p.add_argument("--max-attempts", type=int, default=30)
    p.add_argument("--total-budget", type=float, default=300.0)
    p.add_argument("--backoff-base", type=float, default=1.0)
    p.add_argument("--backoff-max", type=float, default=8.0)
    p.add_argument("--commit-deadline", type=float, default=120.0,
                   help="max seconds to hold the stream head while deciding to retry")
    p.add_argument("--timeout", type=float, default=600.0)
    p.add_argument("--log", default="")
    p.add_argument("--forward-late-overload", action="store_true",
                   help="forward an overload that arrives after streaming starts instead of dropping "
                        "the connection; for clients whose stream retries are off")
    ARGS = p.parse_args()
    positive = (ARGS.max_attempts, ARGS.total_budget, ARGS.commit_deadline, ARGS.timeout)
    if not 0 < ARGS.port < 65536 or min(positive) <= 0 or min(ARGS.backoff_base, ARGS.backoff_max) < 0:
        p.error("invalid port, retry count or timeout")

    if ARGS.log and os.path.exists(ARGS.log):
        try:
            LOG_SIZE[0] = os.path.getsize(ARGS.log)
        except OSError:
            LOG_SIZE[0] = 0
    ARGS.upstream_scheme, ARGS.upstream_host, ARGS.upstream_port, ARGS.upstream_prefix = (
        parse_upstream(ARGS.upstream)
    )
    if ARGS.proxy:
        proxy = urlsplit(ARGS.proxy)
        if proxy.scheme != "http" or not proxy.hostname or proxy.username or proxy.password:
            p.error("proxy must be an HTTP URL without credentials")
        ARGS.proxy = (proxy.hostname, proxy.port or 80)

    target = ARGS.upstream if "://" in ARGS.upstream else "https://" + ARGS.upstream
    log(f"relay listening on 127.0.0.1:{ARGS.port} -> {target} via {ARGS.proxy}")
    srv = LoopbackServer(("127.0.0.1", ARGS.port), Handler)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    sys.exit(main())
