"""Native per-user service management without process-name matching."""

import json
import os
import platform
import plistlib
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

from cchost_config import atomic_write, validate_upstream


LABEL = "com.cchost.codex-retry"
SERVICE = "cchost-codex-retry.service"
ROOT = Path(__file__).resolve().parent.parent


def paths():
    if platform.system() == "Darwin":
        return (Path.home() / "Library/LaunchAgents" / (LABEL + ".plist"),
                Path.home() / "Library/Application Support/cchost-codex-retry")
    if platform.system() == "Linux":
        config = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
        state = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local/state"))
        return config / "systemd/user" / SERVICE, state / "cchost-codex-retry"
    raise ValueError("automatic startup supports macOS/Linux; use --manual on other platforms")


def run(command, required=True):
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=30)
    except subprocess.TimeoutExpired as exc:
        raise ValueError(f"service command timed out: {command[0]} {command[1]}") from exc
    if required and result.returncode:
        raise ValueError(f"service command failed: {command[0]} {command[1]}")
    return result.returncode == 0


def health(port, upstream):
    # Ignore shell proxy settings for a loopback health probe.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(f"http://127.0.0.1:{port}/__codex_retry/health", timeout=1) as response:
            data = json.load(response)
        actual = data.get("upstream", "")
        actual = actual if "://" in actual else "https://" + actual
        return (data.get("service") == "cchost-codex-retry"
                and validate_upstream(actual) == validate_upstream(upstream))
    except (OSError, ValueError, AttributeError, TypeError):  # URLError is an OSError
        return False


def command(port, upstream, state, proxy):
    argv = [sys.executable, str(ROOT / "relay/relay.py"), "--port", str(port),
            "--upstream", upstream, "--log", str(state / "relay.log"),
            "--max-attempts", "10", "--total-budget", "300", "--commit-deadline", "300",
            # Codex retries are off under CCHost, so a dropped stream would only fail as "stream disconnected".
            "--forward-late-overload"]
    if proxy:
        argv.extend(["--proxy", proxy])
    return argv


def service_definition(port, upstream, proxy):
    file, state = paths()
    argv = command(port, upstream, state, proxy)
    if platform.system() == "Darwin":
        data = {"Label": LABEL, "ProgramArguments": argv, "RunAtLoad": True,
                "KeepAlive": True, "ThrottleInterval": 10, "ProcessType": "Background",
                # The relay also prints every --log line to stdout; keep only errors here.
                "WorkingDirectory": str(state), "StandardOutPath": os.devnull,
                "StandardErrorPath": str(state / "service.log")}
        return file, state, plistlib.dumps(data)
    quoted = " ".join('"' + arg.replace('\\', '\\\\').replace('"', '\\"').replace('%', '%%').replace('$', '$$') + '"' for arg in argv)
    unit = ("[Unit]\nDescription=CCHost Codex retry relay\n\n[Service]\nType=simple\n"
            f"ExecStart={quoted}\nStandardOutput=null\nRestart=always\nRestartSec=10\n\n"
            "[Install]\nWantedBy=default.target\n")
    return file, state, unit.encode()


def domain():
    return f"gui/{os.getuid()}"


def stop_launchd():
    target = f"{domain()}/{LABEL}"
    run(["launchctl", "bootout", target], required=False)
    # bootout can return before launchd drops the job, and bootstrap fails until it does.
    deadline = time.monotonic() + 10
    while run(["launchctl", "print", target], required=False):
        if time.monotonic() >= deadline:
            raise ValueError("launchd service is still running")
        time.sleep(0.2)


def activate(file):
    if platform.system() == "Darwin":
        stop_launchd()
        run(["launchctl", "bootstrap", domain(), str(file)])
    else:
        run(["systemctl", "--user", "daemon-reload"])
        run(["systemctl", "--user", "enable", SERVICE])
        run(["systemctl", "--user", "restart", SERVICE])


def snapshot():
    file, _ = paths()
    return file.read_bytes() if file.exists() else None


def rollback(previous):
    if previous is None:
        uninstall()
    else:
        file, _ = paths()
        atomic_write(file, previous)
        activate(file)


def wait_healthy(port, upstream, state):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if health(port, upstream):
            return
        time.sleep(0.1)
    raise ValueError(f"relay failed health check; config unchanged; inspect logs in {state}")


def install(port, upstream, proxy=""):
    previous = snapshot()
    file, state, definition = service_definition(port, upstream, proxy)
    file.parent.mkdir(parents=True, exist_ok=True)
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        atomic_write(file, definition)
        activate(file)
        wait_healthy(port, upstream, state)
    except (OSError, ValueError) as exc:
        try:
            rollback(previous)
        except (OSError, ValueError) as cleanup:
            raise ValueError(f"{exc}; service rollback failed: {cleanup}") from exc
        raise


def uninstall():
    file, _ = paths()
    if platform.system() == "Darwin":
        stop_launchd()
    else:
        run(["systemctl", "--user", "disable", "--now", SERVICE], required=False)
        if run(["systemctl", "--user", "is-active", "--quiet", SERVICE], required=False):
            raise ValueError("systemd service is still running")
    if file.exists():
        file.unlink()
    if platform.system() == "Linux":
        run(["systemctl", "--user", "daemon-reload"])
