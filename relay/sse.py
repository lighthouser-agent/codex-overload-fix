"""Inspect only the uncommitted SSE head; never replay observable output."""

import email.utils
import http.client
import json
import re
import socket
import threading
import time
from dataclasses import dataclass, field


OVERLOAD_CODES = {
    "server_overloaded", "server_is_overloaded", "service_unavailable_error",
    "flex_unavailable", "queue_full",
}
# response.metadata carries ids only; CCHost sends it right before an in-stream overload.
SETUP_EVENTS = {"response.created", "response.in_progress", "response.queued", "response.metadata", "ping"}
QUOTA_MARKERS = (
    "quota", "billing", "credit", "balance", "invalid_api_key", "authentication",
    "unauthorized", "余额", "额度", "欠费",
)
TOKEN = re.compile(r"[A-Za-z0-9_.:-]{1,64}")


def token(value):
    return value if isinstance(value, str) and TOKEN.fullmatch(value) else "?"


def keys(value):
    return ",".join(sorted(token(key) for key in value)[:16])


def describe(payload):
    """Log-safe event shape: type, field names and error codes, never values or text."""
    try:
        event = json.loads(payload)
    except (ValueError, TypeError):
        return f"non_json:{len(payload)}B"
    if not isinstance(event, dict):
        return "non_object"
    parts = ["type=" + token(event.get("type")), "keys=" + keys(event)]
    response, error, item = event.get("response"), event.get("error"), event.get("item")
    if isinstance(response, dict):
        parts.append("status=" + token(response.get("status")))
        error = response.get("error") or error
    if isinstance(error, dict):
        parts.append(f"error={token(error.get('code'))}/{token(error.get('type'))}:{keys(error)}")
    elif error is not None:
        parts.append("error=" + token(error))
    if isinstance(item, dict):
        parts.append(f"item={token(item.get('type'))}:{keys(item)}")
    return " ".join(parts)


def is_overload(error):
    if not isinstance(error, dict):
        return False
    values = [str(error.get(key) or "").lower() for key in ("code", "type", "message")]
    if any(marker in value for marker in QUOTA_MARKERS for value in values):
        return False
    return any(value in OVERLOAD_CODES for value in values[:2]) or any(
        marker in values[2] for marker in ("at capacity", "overloaded")
    )


def empty_message_item(event):
    """Only a known empty assistant message is safe to buffer as setup."""
    item = event.get("item")
    return (event.get("type") == "response.output_item.added" and isinstance(item, dict)
            and item.get("type") == "message" and item.get("role") == "assistant"
            and item.get("content") == []
            and item.get("phase") in (None, "commentary", "final_answer")
            and set(item) <= {"id", "type", "role", "status", "content", "phase"})


def commit_reason(event):
    """Return fixed categories only; upstream strings may contain private content."""
    kind = event.get("type")
    if kind == "response.output_item.added":
        item = event.get("item")
        item_type = item.get("type") if isinstance(item, dict) else None
        return {"message": "message_item", "reasoning": "reasoning_item",
                "function_call": "tool_item", "custom_tool_call": "tool_item"}.get(item_type, "unknown_item")
    return {"response.output_text.delta": "text_output",
            "response.content_part.added": "content_part",
            "response.reasoning_summary_text.delta": "reasoning_output",
            "response.function_call_arguments.delta": "tool_output"}.get(kind, "unknown_event")


def classify_event(payload, metadata=None):
    """Unknown events commit conservatively, including every tool/reasoning event."""
    try:
        event = json.loads(payload)
    except (ValueError, TypeError):
        return "commit"
    if not isinstance(event, dict):
        return "commit"
    kind = event.get("type")
    response = event.get("response")
    error = response.get("error") if isinstance(response, dict) else None
    error = error or event.get("error")
    if kind == "error" and not isinstance(error, dict):
        error = event
    if kind is None and isinstance(error, dict) and set(event) <= {"error", "sequence_number"}:
        kind = "error"
    if kind in ("error", "response.error", "response.failed"):
        if metadata is not None:
            metadata["error_event"] = True
        return "retry" if is_overload(error) else "commit"
    if kind in SETUP_EVENTS or empty_message_item(event):
        return None
    if metadata is not None:
        metadata["commit_reason"] = commit_reason(event)
    return "commit"


class SSEHead:
    """Frame SSE across arbitrary TCP chunks, including multiline data fields."""

    def __init__(self):
        self.buffer = b""
        self.data = []
        self.error_event = False
        self.commit_reason = "unknown_event"
        self.detail = ""

    def payloads(self, chunk):
        self.buffer += chunk
        while b"\n" in self.buffer:
            line, self.buffer = self.buffer.split(b"\n", 1)
            line = line.rstrip(b"\r")
            if line.startswith(b"data:"):
                self.data.append(line[5:].lstrip(b" "))
            elif not line and self.data:
                payload = b"\n".join(self.data)
                self.data = []
                yield payload

    def feed(self, chunk):
        for payload in self.payloads(chunk):
            metadata = {}
            verdict = classify_event(payload, metadata)
            self.error_event = metadata.get("error_event", False)
            self.commit_reason = metadata.get("commit_reason", "unknown_event")
            if verdict:
                self.detail = describe(payload)
                return verdict
        return None


class SSETail(SSEHead):
    """Follow forwarded bytes to log how a committed stream ended; never alters them."""

    FAILURE_MARKERS = ("error", "fail", "incomplete")
    # Stop tracking rather than buffer an unbounded non-SSE body or line.
    BUFFER_LIMIT = 4 * 1024 * 1024

    def __init__(self):
        super().__init__()
        self.events = 0
        self.last = ""
        self.failure = ""
        self.stopped = False

    def feed(self, chunk):
        if self.stopped:
            return None
        if len(self.buffer) + sum(map(len, self.data)) + len(chunk) > self.BUFFER_LIMIT:
            self.stopped, self.buffer, self.data = True, b"", []
            self.last = "untracked"
            return None
        try:
            for payload in self.payloads(chunk):
                self.events += 1
                self.last = describe(payload)
                if not self.failure and any(marker in self.last for marker in self.FAILURE_MARKERS):
                    self.failure = f"#{self.events} {self.last}"
        except Exception:  # diagnostics must never break a committed stream
            self.stopped, self.buffer, self.data = True, b"", []
            self.last = "untracked"
        return None


def retry_after(headers):
    value = headers.get("Retry-After", "")
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            return max(0.0, email.utils.parsedate_to_datetime(value).timestamp() - time.time())
        except (ValueError, TypeError, OverflowError):
            return 0.0


def possible_sse_head(data):
    """CCHost can label SSE as plain text; inspect syntax without replaying other bodies."""
    head = data.lstrip(b"\r\n")
    fields = (b"data:", b"event:", b"id:", b"retry:", b":")
    return any(head.startswith(field) or field.startswith(head) for field in fields)


@dataclass
class Result:
    mode: str
    status: int
    headers: dict = field(default_factory=dict)
    body: bytes = b""
    reason: str = ""
    delay: float = 0.0
    detail: str = ""


class StreamAttempt:
    """Own one upstream connection and abort blocked reads when its client leaves."""

    def __init__(self, connect, timeout, deadline, client_gone):
        self.conn = connect(min(timeout, max(0.1, deadline - time.monotonic())))
        self.timeout = timeout
        self.deadline = deadline
        self.client_gone = client_gone
        self.response = None
        self.sock = None
        self.committed = False
        self.abort_reason = None
        self.done = threading.Event()
        self.watcher = threading.Thread(target=self._watch, daemon=True)

    def __enter__(self):
        self.watcher.start()
        return self

    def __exit__(self, *unused):
        self.done.set()
        if self.response:
            self.response.close()
        self.conn.close()
        self.watcher.join(timeout=0.2)

    def _watch(self):
        while not self.done.wait(0.05):
            if self.client_gone():
                self.abort_reason = "client_closed"
            elif not self.committed and time.monotonic() >= self.deadline:
                self.abort_reason = "deadline"
            if self.abort_reason:
                sock = self.sock or self.conn.sock
                if sock:
                    try:
                        sock.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                # A connection still being established is checked again next tick.
                if sock:
                    return

    def run(self, path, headers, body):
        try:
            try:
                self.conn.connect()
            except (OSError, http.client.HTTPException) as exc:
                if self.abort_reason or time.monotonic() >= self.deadline:
                    raise
                return Result("retry", 502, {"Content-Type": "text/plain"},
                              b"relay: upstream connection unavailable\n", "connect_" + type(exc).__name__)
            self.sock = self.conn.sock
            if self.abort_reason:
                raise OSError(self.abort_reason)
            self.conn.request("POST", path, body=body, headers=headers)
            self.response = self.conn.getresponse()
            headers = dict(self.response.getheaders())
            if self.response.status != 200:
                return self._http_error(headers)
            ctype = self.response.getheader("Content-Type", "").lower()
            encoding = self.response.getheader("Content-Encoding", "identity").lower()
            sniff = not ctype or "text/plain" in ctype
            if ("text/event-stream" not in ctype and not sniff) or encoding not in ("", "identity"):
                self._commit()
                return Result("passthrough", 200, headers, self.response.read())
            return self._read_head(headers, sniff)
        except (OSError, ValueError, http.client.HTTPException) as exc:
            # An ambiguous transport failure is not proof of overload; do not replay it.
            timed_out = self.abort_reason == "deadline" or time.monotonic() >= self.deadline
            status = 504 if timed_out else 502
            reason = self.abort_reason or type(exc).__name__
            return Result("passthrough", status, {"Content-Type": "text/plain"},
                          b"relay: upstream transport interrupted\n", reason)

    def _commit(self):
        self.committed = True
        if self.sock:
            self.sock.settimeout(self.timeout)

    def _http_error(self, headers):
        body = self.response.read()
        status = self.response.status
        try:
            document = json.loads(body)
            error = document.get("error", document) if isinstance(document, dict) else {}
        except ValueError:
            error = {}
        denied = any(marker in str(error).lower() for marker in QUOTA_MARKERS)
        transient = status in (500, 502, 503, 504) and not denied
        limited = status == 429 and is_overload(error)
        mode = "retry" if transient or limited else "passthrough"
        return Result(mode, status, headers, body, f"http_{status}", retry_after(self.response.headers))

    def _read_head(self, headers, sniff=False):
        prelude = bytearray()
        parser = SSEHead()
        while True:
            chunk = self.response.read1(65536)
            if not chunk:
                if self.abort_reason:
                    raise OSError(self.abort_reason)
                # Preserve an incomplete upstream stream; never infer overload from EOF.
                return Result("passthrough", 200, headers, bytes(prelude), "early_eof")
            prelude.extend(chunk)
            if sniff and not possible_sse_head(prelude):
                self._commit()
                return Result("stream", 200, headers, bytes(prelude), "non_sse_body")
            verdict = parser.feed(chunk)
            if verdict == "retry":
                return Result("retry", 200, headers, bytes(prelude), "sse_overload", detail=parser.detail)
            if verdict == "commit" or len(prelude) >= 262144:
                self._commit()
                reason = "error_event_passthrough" if parser.error_event else parser.commit_reason
                return Result("stream", 200, headers, bytes(prelude), reason, detail=parser.detail)
