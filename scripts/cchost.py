#!/usr/bin/env python3
"""Install, inspect or remove CCHost retry without changing credentials."""

import argparse
import json
import socket
import sys
from urllib.parse import urlsplit

if sys.version_info < (3, 11):
    sys.exit("CCHost installer requires Python 3.11+")

import cchost_config as config
import cchost_service as service


def install(args, path):
    original, candidate, state = config.plan(path, args.port)
    if args.proxy:
        proxy = urlsplit(args.proxy)
        if proxy.scheme != "http" or not proxy.hostname or proxy.username or proxy.password:
            raise ValueError("--proxy must be an HTTP URL without credentials")
    if args.dry_run:
        print(f"配置：{path}\n上游：{state['upstream']}\n本地端口：{args.port}")
        print("保留cchost与认证；关闭WebSocket；由中继统一负责重试。")
        return
    if not args.manual:
        with socket.socket() as probe:
            occupied = probe.connect_ex(("127.0.0.1", args.port)) == 0
        previous_service = service.snapshot()
        # A healthy relay with no managed service is a manual one; the service could not bind the port.
        if occupied and (previous_service is None or not service.health(args.port, state["upstream"])):
            raise ValueError("port held by a process this installer does not manage; "
                             "stop it, choose --port or use --manual")
        service.install(args.port, state["upstream"], args.proxy)
    elif not service.health(args.port, state["upstream"]):
        raise ValueError("start relay/relay.py first; refusing to point Codex at an unavailable relay")
    # Persist the mode on every install so uninstall knows whether a service is ours to stop.
    state["manual"] = args.manual
    try:
        config.apply(path, original, candidate, state)
    except (OSError, ValueError) as exc:
        if not args.manual:
            try:
                service.rollback(previous_service)
            except (OSError, ValueError) as cleanup:
                raise ValueError(f"{exc}; service rollback failed: {cleanup}") from exc
        raise
    print(f"已接入CCHost中继：{path}\n备份：{state['backup']}\n重启使用此配置的Codex客户端后生效。")


def status(path):
    state_file = config.state_path(path)
    if not state_file.exists():
        print(f"未接入：{path}")
        return
    state = json.loads(state_file.read_text())
    current = config.provider(config.load(path)[1])
    matches = all(current.get(key) == value for key, value in state["managed"].items())
    healthy = service.health(state["port"], state["upstream"])
    print(f"配置：{path}\n配置匹配：{matches}\n中继健康：{healthy}")
    if not matches or not healthy:
        raise ValueError("relay installation needs attention")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("install", "uninstall", "status"))
    parser.add_argument("--config", help="existing CCHost config.toml")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--proxy", default="")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--manual", action="store_true", help="manage config only; relay must already run")
    args = parser.parse_args()
    if not 0 < args.port < 65536:
        parser.error("invalid port")
    try:
        path = config.discover(args.config)
        if args.dry_run and args.action != "install":
            raise ValueError("--dry-run applies only to install")
        if args.action == "install":
            install(args, path)
        elif args.action == "status":
            status(path)
        else:
            state = config.restore(path, keep_state=True)
            if not args.manual and not state.get("manual", False):
                service.uninstall()
            config.state_path(path).unlink()
            print(f"已恢复CCHost直连；保留备份：{state['backup']}\n重启Codex客户端后生效。")
    except (OSError, ValueError, KeyError) as exc:
        parser.exit(1, f"错误：{exc}\n")


if __name__ == "__main__":
    # A Windows pipe using cp1252 cannot encode the Chinese messages; escape them instead of crashing.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="backslashreplace")
    main()
