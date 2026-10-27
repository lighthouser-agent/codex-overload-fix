import argparse
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from tests.harness import ROOT

sys.path.insert(0, str(ROOT / "scripts"))
import cchost
import cchost_config as config
import cchost_service as service
from tests.test_config import CONFIG


class ServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.file = self.root / "relay.plist"
        self.paths = patch.object(service, "paths", return_value=(self.file, self.root / "logs"))
        self.paths.start()

    def tearDown(self):
        self.paths.stop()
        self.directory.cleanup()

    def test_failed_fresh_service_install_removes_registered_service(self):
        def command(argv, required=True):
            return "print" not in argv
        with patch.object(service, "activate"), patch.object(service, "run", side_effect=command), \
                patch.object(service, "wait_healthy", side_effect=ValueError("unhealthy")), \
                patch.object(service.platform, "system", return_value="Darwin"):
            with self.assertRaisesRegex(ValueError, "unhealthy"):
                service.install(8787, "https://cchost.ai/v1")
        self.assertFalse(self.file.exists())

    def test_failed_service_upgrade_restores_previous_definition(self):
        self.file.write_bytes(b"previous definition")
        with patch.object(service, "activate") as activate, \
                patch.object(service, "wait_healthy", side_effect=ValueError("unhealthy")), \
                patch.object(service.platform, "system", return_value="Darwin"):
            with self.assertRaisesRegex(ValueError, "unhealthy"):
                service.install(8787, "https://cchost.ai/v1")
        self.assertEqual(self.file.read_bytes(), b"previous definition")
        self.assertEqual(activate.call_count, 2)

    def test_config_failure_restores_previous_service(self):
        path = self.root / "config.toml"
        path.write_text(CONFIG)
        args = argparse.Namespace(port=8787, proxy="", dry_run=False, manual=False)
        with patch.object(service, "install"), patch.object(service, "health", return_value=True), \
                patch.object(config, "apply", side_effect=ValueError("concurrent edit")), \
                patch.object(service, "snapshot", return_value=b"old"), patch.object(service, "rollback") as rollback:
            with self.assertRaisesRegex(ValueError, "concurrent edit"):
                cchost.install(args, path)
        rollback.assert_called_once_with(b"old")
        self.assertEqual(path.read_text(), CONFIG)

    def test_service_command_timeout_becomes_actionable_error(self):
        with patch.object(service.subprocess, "run", side_effect=subprocess.TimeoutExpired("systemctl", 30)):
            with self.assertRaisesRegex(ValueError, "timed out"):
                service.run(["systemctl", "--user", "restart"])

    def test_health_accepts_equivalent_upstream_urls(self):
        for upstream in ("https://cchost.ai/v1/", "cchost.ai/v1", "https://cchost.ai:443/v1"):
            payload = {"service": "cchost-codex-retry", "upstream": upstream}
            opener = Mock()
            opener.open.return_value = io.BytesIO(json.dumps(payload).encode())
            with self.subTest(upstream=upstream), patch.object(service.urllib.request, "build_opener", return_value=opener):
                self.assertTrue(service.health(8787, "https://cchost.ai/v1"))

    def test_discovery_finds_managed_config_after_switching_provider(self):
        path = self.root / ".codex" / "config.toml"
        path.parent.mkdir()
        path.write_text(CONFIG)
        original, candidate, state = config.plan(path.resolve(), 8787)
        config.apply(path.resolve(), original, candidate, state)
        path.write_text(path.read_text().replace('model_provider = "cchost"', 'model_provider = "openai"'))
        with patch.dict("os.environ", {"CODEX_HOME": ""}), patch.object(config.Path, "home", return_value=self.root):
            self.assertEqual(config.discover(), path.resolve())
