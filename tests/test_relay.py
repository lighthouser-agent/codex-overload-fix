import gzip
import json
import sys
import time
import unittest
from unittest.mock import Mock, patch

from tests.harness import OVERLOAD, ROOT, SUCCESS, Relay, event, sse

sys.path.insert(0, str(ROOT / "relay"))
from sse import SSEHead, SSETail, StreamAttempt, classify_event, describe


class FramingTests(unittest.TestCase):
    def test_every_chunk_boundary_recognizes_overload(self):
        for boundary in range(len(OVERLOAD)):
            parser = SSEHead()
            first = parser.feed(OVERLOAD[:boundary])
            second = parser.feed(OVERLOAD[boundary:])
            self.assertIn("retry", (first, second))

    def test_multiline_and_crlf(self):
        data = b'data: {"type":"error",\r\ndata: "error":{"code":"server_is_overloaded"}}\r\n\r\n'
        self.assertEqual(SSEHead().feed(data), "retry")

    def test_unknown_event_and_bad_json_commit(self):
        for payload in (b"[]", b"null", b"[DONE]", b"invalid", b'{"type":"new-tool-event"}'):
            self.assertEqual(classify_event(payload), "commit")

    def test_only_known_empty_assistant_message_can_remain_uncommitted(self):
        item = {"type": "message", "role": "assistant", "content": [], "status": "in_progress"}
        self.assertIsNone(classify_event(json.dumps({"type": "response.output_item.added", "item": item})))
        for changed in ({"content": [{"type": "output_text", "text": "output"}]},
                        {"type": "function_call", "arguments": "{}"},
                        {"type": "reasoning"}, {"unknown_field": "output"}):
            with self.subTest(changed=changed):
                payload = {"type": "response.output_item.added", "item": {**item, **changed}}
                self.assertEqual(classify_event(json.dumps(payload)), "commit")

    def test_tail_diagnostics_never_raise_or_grow_unbounded(self):
        for body in (b"x" * (SSETail.BUFFER_LIMIT + 1), b"data: invalid\n\n"):
            with self.subTest(size=len(body)):
                tail = SSETail()
                tail.feed(body)
                self.assertLessEqual(len(tail.buffer), SSETail.BUFFER_LIMIT)
        with patch("sse.describe", side_effect=RecursionError):
            tail = SSETail()
            tail.feed(SUCCESS)
            self.assertEqual((tail.stopped, tail.last), (True, "untracked"))
        self.assertEqual(describe(b'{"type":"x y","secret value":"private"}'), "type=? keys=?,type")

    def test_connect_failure_can_retry_without_sending_request_bytes(self):
        connection = Mock()
        connection.connect.side_effect = ConnectionRefusedError()
        with StreamAttempt(lambda timeout: connection, 1, time.monotonic() + 1, lambda: False) as attempt:
            result = attempt.run("/responses", {}, b"{}")
        self.assertEqual((result.mode, result.status), ("retry", 502))
        connection.request.assert_not_called()


class RelayTests(unittest.TestCase):
    def test_cchost_plain_text_sse_overload_is_retried(self):
        for headers in ({"Content-Type": "text/plain; charset=utf-8"}, {}):
            with self.subTest(headers=headers), Relay([(200, headers, [OVERLOAD]), sse(SUCCESS)]) as relay:
                self.assertEqual(relay.request()[2], SUCCESS)
                self.assertEqual(len(relay.server.requests), 2)

    def test_fragmented_plain_text_sse_still_stops_replay_after_output(self):
        headers = {"Content-Type": "text/plain; charset=utf-8"}
        for kind in ("response.output_text.delta", "response.reasoning_summary_text.delta",
                     "response.function_call_arguments.delta"):
            body = event(kind, delta="private-output") + OVERLOAD
            chunks = [bytes([byte]) for byte in body]
            with self.subTest(kind=kind), Relay([(200, headers, chunks)]) as relay:
                self.assertEqual(relay.request()[2], body)
                self.assertEqual(len(relay.server.requests), 1)

    def test_plain_text_json_body_uses_normal_read_timeout(self):
        def chunks():
            yield b'{"ok":'
            time.sleep(0.25)
            yield b'true}'
        with Relay([(200, {"Content-Type": "text/plain"}, chunks())], "--commit-deadline", "0.1") as relay:
            self.assertEqual(relay.request()[::2], (200, b'{"ok":true}'))
            self.assertEqual(len(relay.server.requests), 1)

    def test_flat_and_typeless_overload_events_retry(self):
        for values in ({"code": "server_overloaded", "message": "Selected model is at capacity."},
                       {"error": {"code": "server_is_overloaded"}}):
            raw = ({"type": "error", **values} if "code" in values else values)
            body = b"data: " + json.dumps(raw).encode() + b"\n\n"
            with self.subTest(raw=raw), Relay([sse(body), sse(SUCCESS)]) as relay:
                self.assertEqual(relay.request()[2], SUCCESS)
                self.assertEqual(len(relay.server.requests), 2)

    def test_empty_message_setup_does_not_hide_following_overload(self):
        initial = (event("response.created") + event("response.in_progress")
                   + event("response.output_item.added", item={"type": "message", "role": "assistant", "content": []}))
        with Relay([sse(initial, OVERLOAD), sse(SUCCESS)]) as relay:
            self.assertEqual(relay.request()[2], SUCCESS)
            self.assertEqual(len(relay.server.requests), 2)

    def test_cchost_empty_message_with_phase_retries_following_overload(self):
        for phase in ("commentary", "final_answer", None):
            item = {"id": "msg_fixture", "type": "message", "role": "assistant",
                    "status": "in_progress", "content": [], "phase": phase}
            initial = event("response.created") + event("response.output_item.added", item=item)
            reply = 200, {"Content-Type": "text/plain; charset=utf-8"}, [initial, OVERLOAD]
            with self.subTest(phase=phase), Relay([reply, sse(SUCCESS)]) as relay:
                self.assertEqual(relay.request()[2], SUCCESS)
                self.assertEqual(len(relay.server.requests), 2)

    def test_phase_never_makes_nonempty_or_unknown_message_replayable(self):
        item = {"type": "message", "role": "assistant", "content": [], "phase": "final_answer"}
        for changed in ({"content": [{"type": "output_text", "text": "private-output"}]},
                        {"phase": "future-phase"}, {"unknown": "private-output"}):
            first = event("response.output_item.added", item={**item, **changed})
            with self.subTest(changed=changed), Relay([sse(first, OVERLOAD)]) as relay:
                self.assertEqual(relay.request()[2], first + OVERLOAD)
                self.assertEqual(len(relay.server.requests), 1)

    def test_overload_retry_preserves_body_and_auth_and_redacts_logs(self):
        body = gzip.compress(b'{"model":"test-model","private":"secret-prompt"}')
        with Relay([sse(OVERLOAD), sse(OVERLOAD), sse(SUCCESS)]) as relay:
            status, _, data = relay.request(body, headers={"Content-Encoding": "gzip"})
            self.assertEqual((status, data), (200, SUCCESS))
            self.assertEqual(len(relay.server.requests), 3)
            for path, headers, forwarded, _ in relay.server.requests:
                self.assertEqual(path, "/v1/responses")
                self.assertEqual(headers["Authorization"], "Bearer dummy-secret")
                self.assertEqual(headers["Content-Encoding"], "gzip")
                self.assertEqual(body, forwarded)
            logs = relay.output.read_text()
            for secret in ("dummy-secret", "private-output", "secret-prompt"):
                self.assertNotIn(secret, logs)

    def test_exhausted_http_error_preserves_status_headers_and_body(self):
        reply = 503, {"Content-Type": "application/json", "Retry-After": "0", "X-Request-ID": "upstream-id"}, [b'{"error":"busy"}']
        with Relay([reply]) as relay:
            status, headers, body = relay.request()
            self.assertEqual(status, 503)
            self.assertEqual(headers["X-Request-ID"], "upstream-id")
            self.assertEqual(body, reply[2][0])
            self.assertEqual(len(relay.server.requests), 3)

    def test_quota_auth_and_unknown_rate_limit_pass_once(self):
        for status, code in ((429, "insufficient_quota"), (401, "invalid_api_key"),
                             (403, "forbidden"), (429, "unknown"), (503, "insufficient_balance")):
            body = json.dumps({"error": {"code": code}}).encode()
            with self.subTest(status=status, code=code), Relay([(status, {"Content-Type": "application/json"}, [body])]) as relay:
                self.assertEqual(relay.request()[::2], (status, body))
                self.assertEqual(len(relay.server.requests), 1)

    def test_sse_quota_passes_once_even_when_message_mentions_overload(self):
        body = event("error", error={"code": "insufficient_quota", "message": "overloaded"})
        with Relay([sse(body)]) as relay:
            self.assertEqual(relay.request()[2], body)
            self.assertEqual(len(relay.server.requests), 1)

    def test_flat_quota_error_does_not_retry(self):
        body = event("error", code="insufficient_quota", message="at capacity")
        with Relay([sse(body)]) as relay:
            self.assertEqual(relay.request()[2], body)
            self.assertEqual(len(relay.server.requests), 1)

    def test_never_replays_text_reasoning_or_tools(self):
        kinds = ("response.output_text.delta", "response.reasoning_summary_text.delta",
                 "response.output_item.added", "response.function_call_arguments.delta")
        for kind in kinds:
            first = event(kind, delta="private-output")
            with self.subTest(kind=kind), Relay([sse(first, OVERLOAD)]) as relay:
                self.assertEqual(relay.request()[2], first + OVERLOAD)
                self.assertEqual(len(relay.server.requests), 1)

    def test_retry_after_larger_than_budget_does_not_retry(self):
        reply = 503, {"Retry-After": "60"}, [b"busy"]
        with Relay([reply], "--total-budget", "0.2") as relay:
            self.assertEqual(relay.request()[0], 503)
            self.assertEqual(len(relay.server.requests), 1)

    def test_quick_cancel_interrupts_blocked_upstream(self):
        with Relay([sse(event("response.created"), None)]) as relay:
            client = relay.raw_request()
            self.assertTrue(relay.server.started.wait(2))
            started = time.monotonic()
            client.close()
            self.assertTrue(relay.server.disconnected.wait(1.5))
            self.assertLess(time.monotonic() - started, 1.5)
            self.assertEqual(len(relay.server.requests), 1)

    def test_cancel_during_backoff_does_not_send_another_attempt(self):
        with Relay([sse(OVERLOAD)], "--backoff-base", "1", "--backoff-max", "1") as relay:
            client = relay.raw_request()
            self.assertTrue(relay.server.started.wait(2))
            client.close()
            time.sleep(0.25)
            self.assertEqual(len(relay.server.requests), 1)

    def test_commit_deadline_bounds_a_silent_upstream(self):
        with Relay([sse(event("response.created"), None)], "--commit-deadline", "0.15") as relay:
            started = time.monotonic()
            self.assertEqual(relay.request()[0], 504)
            self.assertLess(time.monotonic() - started, 1.5)
            self.assertEqual(len(relay.server.requests), 1)

    def test_json_success_is_transparent(self):
        with Relay([(200, {"Content-Type": "application/json"}, [b'{"ok":true}'])]) as relay:
            self.assertEqual(relay.request()[::2], (200, b'{"ok":true}'))

    def test_json_body_can_outlive_sse_commit_deadline(self):
        def chunks():
            yield b'{"ok":'
            time.sleep(0.25)
            yield b'true}'
        with Relay([(200, {"Content-Type": "application/json"}, chunks())], "--commit-deadline", "0.1") as relay:
            self.assertEqual(relay.request()[::2], (200, b'{"ok":true}'))
            self.assertEqual(len(relay.server.requests), 1)

    def test_other_endpoint_is_never_retried(self):
        with Relay([(503, {}, [b"busy"])]) as relay:
            self.assertEqual(relay.request(path="/responses/compact")[0], 503)
            self.assertEqual(len(relay.server.requests), 1)

    def test_fragmented_stream_and_exhausted_overload_are_transparent(self):
        with Relay([sse(*(bytes([byte]) for byte in OVERLOAD))]) as relay:
            self.assertEqual(relay.request()[::2], (200, OVERLOAD))
            self.assertEqual(len(relay.server.requests), 3)

    def test_unknown_transport_failure_is_not_replayed(self):
        initial = event("response.created")
        with Relay([sse(initial)]) as relay:
            self.assertEqual(relay.request()[2], initial)
            self.assertEqual(len(relay.server.requests), 1)


class CCHostCaptureTests(unittest.TestCase):
    """Stream shapes observed from the real CCHost upstream."""

    def test_cchost_metadata_event_retries_following_overload(self):
        # Shape captured from a real CCHost overload on 2026-10-27 18:51.
        error = {"code": "server_is_overloaded", "type": "service_unavailable_error",
                 "message": "private-error-text", "param": None}
        failed = (event("response.created", response={"status": "in_progress"}, sequence_number=0)
                  + event("response.metadata", metadata={"private": "private-metadata"},
                          response_id="resp_fixture", sequence_number=1)
                  + event("error", error=error, sequence_number=2)
                  + event("response.failed", response={"status": "failed", "error": error},
                          sequence_number=3))
        reply = 200, {"Content-Type": "text/plain; charset=utf-8"}, [failed]
        with Relay([reply, sse(SUCCESS)]) as relay:
            self.assertEqual(relay.request()[2], SUCCESS)
            self.assertEqual(len(relay.server.requests), 2)
            logs = relay.output.read_text()
            for secret in ("private-metadata", "private-error-text", "resp_fixture", "private-output"):
                self.assertNotIn(secret, logs)

    def test_committed_stream_end_is_logged_without_content(self):
        error = {"code": "server_is_overloaded", "message": "private-error-text"}
        first = event("response.output_text.delta", delta="private-output")
        late = event("response.failed", response={"status": "failed", "error": error})
        with Relay([sse(first, late)]) as relay:
            self.assertEqual(relay.request()[2], first + late)
            logs = relay.output.read_text()
            self.assertIn("failure [#2 type=response.failed", logs)
            for secret in ("private-error-text", "private-output"):
                self.assertNotIn(secret, logs)


if __name__ == "__main__":
    unittest.main()
