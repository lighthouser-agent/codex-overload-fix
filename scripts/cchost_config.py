"""Surgical, reversible edits of the existing CCHost provider. Python 3.11+."""

import copy
import json
import os
import re
import tempfile
import time
import tomllib
from pathlib import Path
from urllib.parse import urlsplit


FIELDS = ("base_url", "supports_websockets", "stream_max_retries", "request_max_retries")


def load(path):
    text = path.read_bytes().decode("utf-8")
    return text, tomllib.loads(text)


def discover(explicit=None):
    if explicit:
        path = Path(explicit).expanduser().resolve(strict=True)
        if not path.is_file():
            raise ValueError("config must be a file")
        return path
    if os.environ.get("CODEX_HOME"):
        return discover(str(Path(os.environ["CODEX_HOME"]) / "config.toml"))
    candidates = []
    for directory in (".codex-cli", ".codex"):
        path = Path.home() / directory / "config.toml"
        if path.is_file() and (load(path)[1].get("model_provider") == "cchost" or state_path(path).exists()):
            candidates.append(path.resolve())
    candidates = list(dict.fromkeys(candidates))
    if len(candidates) != 1:
        raise ValueError("cannot identify one CCHost config; pass --config /absolute/path/config.toml")
    return candidates[0]


def state_path(config):
    return config.with_name(config.name + ".cchost-retry.json")


def provider(document, require_selected=True):
    if require_selected and document.get("model_provider") != "cchost":
        raise ValueError("selected config must already use model_provider = cchost")
    result = document.get("model_providers", {}).get("cchost")
    if not isinstance(result, dict) or result.get("wire_api", "responses") != "responses":
        raise ValueError("existing CCHost Responses provider required")
    return result


def validate_upstream(value):
    url = urlsplit(value)
    if (url.scheme != "https" or url.hostname != "cchost.ai" or url.username
            or url.password or url.query or url.fragment or url.path.rstrip("/") != "/v1"
            or url.port not in (None, 443)):
        raise ValueError("CCHost upstream must be https://cchost.ai/v1")
    return "https://cchost.ai/v1"


def managed_values(port):
    return {"base_url": f"http://127.0.0.1:{port}", "supports_websockets": False,
            "stream_max_retries": 0, "request_max_retries": 0}


def _provider_header(line):
    if not re.match(r"^\s*\[[^\[].*\]\s*(?:#.*)?$", line):
        return False
    try:
        probe = tomllib.loads(line + "\n__relay_probe__ = true\n")
        return probe.get("model_providers", {}).get("cchost", {}).get("__relay_probe__") is True
    except tomllib.TOMLDecodeError:
        return False


def rewrite(text, values, require_selected=True):
    original = tomllib.loads(text)
    provider(original, require_selected)
    lines = text.splitlines(keepends=True)
    starts = [index for index, line in enumerate(lines) if _provider_header(line)]
    if len(starts) != 1:
        raise ValueError("one explicit [model_providers.cchost] table required")
    start = starts[0] + 1
    end = next((i for i in range(start, len(lines)) if lines[i].lstrip().startswith("[")), len(lines))
    newline = "\r\n" if "\r\n" in text else "\n"
    section = [line for line in lines[start:end]
               if not re.match(r"^\s*(" + "|".join(FIELDS) + r")\s*=", line)]
    if section and not section[-1].endswith("\n"):
        section[-1] += newline
    for key, value in values.items():
        if value["present"]:
            section.append(f"{key} = {json.dumps(value['value'])}{newline}")
    candidate = "".join(lines[:start] + section + lines[end:])
    expected = copy.deepcopy(original)
    target = provider(expected, require_selected)
    for key, value in values.items():
        target.pop(key, None)
        if value["present"]:
            target[key] = value["value"]
    if tomllib.loads(candidate) != expected:
        raise ValueError("config edit changed unrelated settings; no write performed")
    return candidate


def atomic_write(path, content):
    descriptor, temporary = tempfile.mkstemp(prefix=".cchost-retry-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def plan(config, port):
    text, document = load(config)
    current = provider(document)
    managed = managed_values(port)
    state_file = state_path(config)
    if state_file.exists():
        state = json.loads(state_file.read_text())
        if state["config"] != str(config) or state["managed"] != managed:
            raise ValueError("existing installation differs; uninstall it before changing the port")
        if any(current.get(key) != value for key, value in managed.items()):
            raise ValueError("managed config fields changed; review them before reinstalling")
        return text, text, state
    upstream = validate_upstream(current.get("base_url", ""))
    values = {key: {"present": True, "value": value} for key, value in managed.items()}
    candidate = rewrite(text, values)
    state = {"config": str(config), "port": port, "upstream": upstream, "managed": managed,
             "original": {key: {"present": key in current, "value": current.get(key)} for key in FIELDS},
             "backup": str(config.with_name(config.name + f".cchost-retry-{time.time_ns()}.bak"))}
    return text, candidate, state


def apply(config, original, candidate, state):
    if config.read_bytes() != original.encode("utf-8"):
        raise ValueError("config changed during installation; no write performed")
    if original == candidate:
        # A reinstall leaves the config alone but may switch between manual and service mode.
        if state_path(config).exists():
            atomic_write(state_path(config), (json.dumps(state, indent=2) + "\n").encode())
        return
    atomic_write(Path(state["backup"]), original.encode("utf-8"))
    atomic_write(state_path(config), (json.dumps(state, indent=2) + "\n").encode())
    try:
        if config.read_bytes() != original.encode("utf-8"):
            raise ValueError("config changed during installation; no write performed")
        atomic_write(config, candidate.encode("utf-8"))
    except (OSError, ValueError):
        state_path(config).unlink()
        raise


def restore(config, keep_state=False):
    file = state_path(config)
    if not file.exists():
        raise ValueError("no managed CCHost relay installation found")
    state = json.loads(file.read_text())
    text, document = load(config)
    current = provider(document, require_selected=False)
    if state["config"] != str(config):
        raise ValueError("installation belongs to another config")
    for key, value in state["managed"].items():
        original = state["original"][key]
        is_original = ((key in current) == original["present"] and current.get(key) == original["value"])
        if current.get(key) != value and not is_original:
            raise ValueError(f"managed field {key} changed; leaving config and service intact")
    restored = rewrite(text, state["original"], require_selected=False)
    if config.read_bytes() != text.encode("utf-8"):
        raise ValueError("config changed during restoration; no write performed")
    atomic_write(config, restored.encode("utf-8"))
    if not keep_state:
        file.unlink()
    return state
