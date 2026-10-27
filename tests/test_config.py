import json
import os
import plistlib
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.harness import ROOT

sys.path.insert(0, str(ROOT / "scripts"))
import cchost_config as config
import cchost_service as service


CONFIG = '''# user settings
model_provider = "cchost"
model = "keep-my-model"
[features]
keep_this = true
[model_providers."cchost"] # existing provider
name = "CCHost"
base_url = "https://cchost.ai/v1"
wire_api = "responses"
supports_websockets = true
[model_providers.cchost.auth]
command = "/private/local/token-helper"
timeout_ms = 5000
[mcp_servers.example]
command = "keep-this-too"
'''


class ConfigTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name).resolve() / "config.toml"
        self.path.write_text(CONFIG)

    def tearDown(self):
        self.directory.cleanup()

    def install(self):
        original, candidate, state = config.plan(self.path, 8787)
        config.apply(self.path, original, candidate, state)
        return state

    def test_install_keeps_auth_model_and_other_settings(self):
        previous = config.load(self.path)[1]
        state = self.install()
        current = config.load(self.path)[1]
        for key in config.FIELDS:
            previous["model_providers"]["cchost"].pop(key, None)
            current["model_providers"]["cchost"].pop(key, None)
        self.assertEqual(previous, current)
        self.assertEqual(Path(state["backup"]).read_text(), CONFIG)
        self.assertNotIn("token-helper", config.state_path(self.path).read_text())
        if os.name == "posix":
            self.assertEqual(Path(state["backup"]).stat().st_mode & 0o777, 0o600)

    def test_reinstall_is_idempotent(self):
        first = self.install()
        installed = self.path.read_bytes()
        second = self.install()
        self.assertEqual(first, second)
        self.assertEqual(self.path.read_bytes(), installed)
        self.assertEqual(len(list(self.path.parent.glob("*.bak"))), 1)

    def test_restore_preserves_later_unrelated_edits_and_missing_keys(self):
        self.install()
        self.path.write_text(self.path.read_text().replace('model = "keep-my-model"', 'model = "new-model"'))
        config.restore(self.path)
        document = config.load(self.path)[1]
        self.assertEqual(document["model"], "new-model")
        self.assertEqual(config.provider(document)["base_url"], "https://cchost.ai/v1")
        self.assertTrue(config.provider(document)["supports_websockets"])
        self.assertNotIn("stream_max_retries", config.provider(document))
        self.assertFalse(config.state_path(self.path).exists())

    def test_restore_refuses_managed_field_drift(self):
        self.install()
        self.path.write_text(self.path.read_text().replace('http://127.0.0.1:8787', 'http://127.0.0.1:9999'))
        before = self.path.read_bytes()
        with self.assertRaises(ValueError):
            config.restore(self.path)
        self.assertEqual(self.path.read_bytes(), before)
        self.assertTrue(config.state_path(self.path).exists())

    def test_restore_preserves_changed_active_provider(self):
        self.install()
        self.path.write_text(self.path.read_text().replace('model_provider = "cchost"', 'model_provider = "openai"'))
        config.restore(self.path)
        document = config.load(self.path)[1]
        self.assertEqual(document["model_provider"], "openai")
        self.assertEqual(config.provider(document, False)["base_url"], "https://cchost.ai/v1")

    def test_edit_during_backup_is_not_overwritten(self):
        original, candidate, state = config.plan(self.path, 8787)
        write = config.atomic_write
        def edited_write(path, content):
            write(path, content)
            if path == config.state_path(self.path):
                self.path.write_text(CONFIG + "# concurrent edit\n")
        with patch.object(config, "atomic_write", side_effect=edited_write), self.assertRaises(ValueError):
            config.apply(self.path, original, candidate, state)
        self.assertEqual(self.path.read_text(), CONFIG + "# concurrent edit\n")
        self.assertFalse(config.state_path(self.path).exists())

    def test_restore_detects_concurrent_edit(self):
        self.install()
        rewrite = config.rewrite
        def edited_rewrite(*args, **kwargs):
            result = rewrite(*args, **kwargs)
            self.path.write_text(self.path.read_text() + "# concurrent edit\n")
            return result
        with patch.object(config, "rewrite", side_effect=edited_rewrite), self.assertRaises(ValueError):
            config.restore(self.path)
        self.assertTrue(self.path.read_text().endswith("# concurrent edit\n"))
        self.assertTrue(config.state_path(self.path).exists())

    def test_concurrent_config_edit_prevents_write(self):
        original, candidate, state = config.plan(self.path, 8787)
        self.path.write_text(CONFIG + "# concurrent edit\n")
        with self.assertRaises(ValueError):
            config.apply(self.path, original, candidate, state)
        self.assertFalse(config.state_path(self.path).exists())
        self.assertFalse(Path(state["backup"]).exists())

    def test_official_provider_is_not_modified(self):
        self.path.write_text(CONFIG.replace('model_provider = "cchost"', 'model_provider = "openai"'))
        with self.assertRaises(ValueError):
            config.plan(self.path, 8787)

    def test_crlf_is_preserved(self):
        self.path.write_bytes(CONFIG.replace("\n", "\r\n").encode())
        self.install()
        self.assertNotIn(b"\n", self.path.read_bytes().replace(b"\r\n", b""))

    def test_single_line_triple_quoted_url_is_supported(self):
        self.path.write_text(CONFIG.replace('base_url = "https://cchost.ai/v1"', 'base_url = """https://cchost.ai/v1"""'))
        self.install()
        self.assertEqual(config.provider(config.load(self.path)[1])["base_url"], "http://127.0.0.1:8787")

    def test_config_discovery_and_explicit_path(self):
        with patch.dict(os.environ, {"CODEX_HOME": str(self.path.parent)}):
            self.assertEqual(config.discover(), self.path)
        self.assertEqual(config.discover(str(self.path)), self.path)

    def test_dry_run_creates_nothing(self):
        before = self.path.read_bytes()
        result = subprocess.run([sys.executable, str(ROOT / "scripts/cchost.py"), "install", "--dry-run",
                                 "--config", str(self.path)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual([file.name for file in self.path.parent.iterdir()], ["config.toml"])

    def test_upstream_must_remain_cchost(self):
        for url in ("https://chatgpt.com/v1", "http://cchost.ai/v1", "https://secret@cchost.ai/v1",
                    "https://cchost.ai/v1?token=private"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                config.validate_upstream(url)

    def test_native_service_definitions_preserve_argv(self):
        with patch("cchost_service.platform.system", return_value="Darwin"):
            _, _, raw = service.service_definition(8787, "https://cchost.ai/v1", "")
            definition = plistlib.loads(raw)
            self.assertEqual(definition["Label"], service.LABEL)
            self.assertTrue(definition["KeepAlive"])
            argv = definition["ProgramArguments"]
            self.assertEqual(argv[argv.index("--upstream") + 1], "https://cchost.ai/v1")
            self.assertEqual(argv[argv.index("--max-attempts") + 1], "10")
        with patch("cchost_service.platform.system", return_value="Linux"):
            self.assertIn(b"cchost.ai/v1", service.service_definition(8787, "https://cchost.ai/v1", "")[2])


if __name__ == "__main__":
    unittest.main()
