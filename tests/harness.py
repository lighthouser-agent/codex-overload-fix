"""Real loopback HTTP fixtures; never contact a model provider."""

import http.client
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent


def event(kind, **values):
    return b"data: " + json.dumps({"type": kind, **values}).encode() + b"\n\n"


OVERLOAD = event("error", error={"code": "server_is_overloaded", "message": "at capacity"})
SUCCESS = event("response.output_text.delta", delta="private-output") + event("response.completed")


class FakeUpstream(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *unused):
        pass

    def do_POST(self):
        server = self.server
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        with server.lock:
            server.requests.append((self.path, dict(self.headers), body, time.monotonic()))
            reply = server.replies[min(len(server.requests) - 1, len(server.replies) - 1)]
        status, headers, chunks = reply
        self.send_response(status)
        for key, value in headers.items():
            self.send_header(key, value)
        self.send_header("Connection", "close")
        self.end_headers()
        server.started.set()
        try:
            for chunk in chunks:
                if chunk is None:
                    self.connection.settimeout(4)
                    if self.connection.recv(1) == b"":
                        server.disconnected.set()
                    break
                self.wfile.write(chunk)
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            server.disconnected.set()
        self.close_connection = True


def sse(*chunks):
    return 200, {"Content-Type": "text/event-stream"}, chunks


class Relay:
    """Own exactly the server and subprocess created for one test."""

    def __init__(self, replies, *extra, upstream=True):
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), FakeUpstream)
        self.server.daemon_threads = True
        self.server.replies = replies
        self.server.requests = []
        self.server.lock = threading.Lock()
        self.server.started = threading.Event()
        self.server.disconnected = threading.Event()
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.directory = tempfile.TemporaryDirectory()
        self.output = Path(self.directory.name) / "relay-output.txt"
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            self.port = probe.getsockname()[1]
        target = ["--upstream", f"http://127.0.0.1:{self.server.server_port}/v1"] if upstream else []
        argv = [sys.executable, str(ROOT / "relay/relay.py"), "--port", str(self.port), *target,
                "--max-attempts", "3", "--backoff-base", "0.01", "--backoff-max", "0.01",
                "--total-budget", "3", "--timeout", "3", *extra]
        with self.output.open("wb") as output:
            self.process = subprocess.Popen(argv, stdout=output, stderr=subprocess.STDOUT)
        self._ready()

    def _ready(self):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", self.port), 0.1):
                    return
            except OSError:
                if self.process.poll() is not None:
                    raise RuntimeError(self.output.read_text())
                time.sleep(0.02)
        raise TimeoutError("relay did not listen")

    def request(self, body=b'{"model":"test-model","stream":true}', path="/responses", headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        supplied = {"Content-Type": "application/json", "Authorization": "Bearer dummy-secret"}
        supplied.update(headers or {})
        connection.request("POST", path, body=body, headers=supplied)
        response = connection.getresponse()
        result = response.status, dict(response.getheaders()), response.read()
        connection.close()
        return result

    def raw_request(self):
        client = socket.create_connection(("127.0.0.1", self.port), 2)
        client.sendall(b"POST /responses HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\n\r\n{}")
        return client

    def __enter__(self):
        return self

    def __exit__(self, *unused):
        if self.process.poll() is None:
            if os.name == "posix":
                command = subprocess.check_output(["ps", "-p", str(self.process.pid), "-o", "command="], text=True)
                assert str(ROOT / "relay/relay.py") in command
            self.process.terminate()
            self.process.wait(timeout=5)
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.directory.cleanup()
