"""Tests for the static architecture invariant checker.

These cover two things:

1. the real repository currently passes every check (a regression guard that
   runs as part of the normal suite), and
2. each check actually fires on a synthetic violation (so the checker cannot
   silently rot into a no-op).
"""

import shutil
import tempfile
import unittest
from pathlib import Path

from tools.check_invariants import (
    REPO_ROOT,
    check_client_modules_isolated,
    check_core_command_path_imports,
    check_error_codes_match_contract,
    check_no_blocking_calls_in_command_path,
    check_terminal_statuses_frozen,
    run_all_checks,
)

# A minimal protocol.py whose ErrorCode enum has exactly two codes, paired with an
# AGENTS.md whose fenced block documents the same two. Tests perturb one side to
# prove drift is caught.
_PROTOCOL_TWO_CODES = (
    "from enum import StrEnum\n\n"
    "class ErrorCode(StrEnum):\n"
    '    INVALID_JSON = "INVALID_JSON"\n'
    '    BOARD_BUSY = "BOARD_BUSY"\n'
)
_AGENTS_TWO_CODES = (
    "# AGENTS.md\n\n"
    "Error codes:\n\n"
    "```text\n"
    "INVALID_JSON  BOARD_BUSY\n"
    "```\n"
)


class RepoPassesInvariantsTests(unittest.TestCase):
    def test_real_repo_has_no_violations(self):
        violations = run_all_checks(REPO_ROOT)
        self.assertEqual(violations, [], msg=f"unexpected violations: {violations}")


class SyntheticViolationTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)

    def _write(self, rel: str, text: str) -> None:
        path = self.root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def test_new_terminal_status_is_flagged(self):
        self._write(
            "protocol.py",
            'from enum import StrEnum\n\n'
            'class TerminalStatus(StrEnum):\n'
            '    OK = "ok"\n'
            '    ERROR = "error"\n'
            '    TIMEOUT = "timeout"\n'
            '    REJECTED = "rejected"\n',
        )
        violations = check_terminal_statuses_frozen(self.root)
        self.assertTrue(violations)
        self.assertIn("rejected", str(violations[0]))

    def test_missing_terminal_status_is_flagged(self):
        self._write(
            "protocol.py",
            'from enum import StrEnum\n\n'
            'class TerminalStatus(StrEnum):\n'
            '    OK = "ok"\n'
            '    ERROR = "error"\n',
        )
        violations = check_terminal_statuses_frozen(self.root)
        self.assertTrue(violations)
        self.assertIn("timeout", str(violations[0]))

    def test_annotated_new_terminal_status_is_flagged(self):
        # AnnAssign form:  REJECTED: str = "rejected"  must not slip through.
        self._write(
            "protocol.py",
            'from enum import StrEnum\n\n'
            'class TerminalStatus(StrEnum):\n'
            '    OK = "ok"\n'
            '    ERROR = "error"\n'
            '    TIMEOUT = "timeout"\n'
            '    REJECTED: str = "rejected"\n',
        )
        violations = check_terminal_statuses_frozen(self.root)
        self.assertTrue(violations)
        self.assertIn("rejected", str(violations[0]))

    def test_frozen_terminal_statuses_pass(self):
        self._write(
            "protocol.py",
            'from enum import StrEnum\n\n'
            'class TerminalStatus(StrEnum):\n'
            '    OK = "ok"\n'
            '    ERROR = "error"\n'
            '    TIMEOUT = "timeout"\n',
        )
        self.assertEqual(check_terminal_statuses_frozen(self.root), [])

    def test_redis_in_command_path_is_flagged(self):
        self._write("controller.py", "import asyncio\nimport redis\n")
        violations = check_core_command_path_imports(self.root)
        self.assertTrue(violations)
        self.assertIn("redis", str(violations[0]))

    def test_redis_via_from_import_is_flagged(self):
        self._write("protocol.py", "from redis import Redis\n")
        violations = check_core_command_path_imports(self.root)
        self.assertTrue(violations)
        self.assertIn("redis", str(violations[0]))

    def test_observability_import_in_command_path_is_flagged(self):
        # The command path receives observability by injection; importing it
        # directly couples the command path to the telemetry/Redis backend.
        self._write("controller.py", "import observability\n")
        violations = check_core_command_path_imports(self.root)
        self.assertTrue(violations)
        self.assertIn("observability", str(violations[0]))

    def test_board_connection_is_in_command_path(self):
        self._write("board_connection.py", "from observability import Sink\n")
        violations = check_core_command_path_imports(self.root)
        self.assertTrue(violations)
        self.assertIn("board_connection.py", str(violations[0]))

    def test_client_importing_board_comms_is_flagged(self):
        self._write("demos/webapp.py", "import json\nimport board_connection\n")
        violations = check_client_modules_isolated(self.root)
        self.assertTrue(violations)
        self.assertIn("board_connection", str(violations[0]))

    def test_client_importing_controller_is_flagged(self):
        self._write("demos/client/client.py", "from controller import ControllerCore\n")
        violations = check_client_modules_isolated(self.root)
        self.assertTrue(violations)
        self.assertIn("controller", str(violations[0]))

    def test_newly_added_demo_is_discovered_dynamically(self):
        # A demo file not in any hardcoded list must still be checked.
        self._write("demos/dashboard.py", "from controller import ControllerCore\n")
        violations = check_client_modules_isolated(self.root)
        self.assertTrue(violations)
        self.assertIn("demos/dashboard.py", str(violations[0]))
        self.assertIn("controller", str(violations[0]))

    def test_mock_board_importing_controller_internals_is_flagged(self):
        self._write("demos/server/server.py", "import local_socket\n")
        violations = check_client_modules_isolated(self.root)
        self.assertTrue(violations)
        self.assertIn("local_socket", str(violations[0]))

    def test_demo_may_import_shared_contract_primitives(self):
        # protocol/state are shared contract primitives, not controller internals.
        self._write("demos/server/server.py", "import protocol\nimport state\n")
        self.assertEqual(check_client_modules_isolated(self.root), [])

    def test_clean_client_module_passes(self):
        self._write("demos/webapp.py", "import json\nimport socket\n")
        self.assertEqual(check_client_modules_isolated(self.root), [])

    # --- error-code drift --------------------------------------------------

    def test_matching_error_codes_pass(self):
        self._write("protocol.py", _PROTOCOL_TWO_CODES)
        self._write("AGENTS.md", _AGENTS_TWO_CODES)
        self.assertEqual(check_error_codes_match_contract(self.root), [])

    def test_code_missing_from_agents_md_is_flagged(self):
        """
        Verifies that an error code present in protocol.py but missing from AGENTS.md is reported as a violation.
        
        Writes a protocol with an extra `UNKNOWN_TARGET` ErrorCode and a matching AGENTS.md that lacks it, then asserts the invariant checker returns at least one violation mentioning `UNKNOWN_TARGET` and that it is "not documented".
        """
        self._write(
            "protocol.py",
            _PROTOCOL_TWO_CODES + '    UNKNOWN_TARGET = "UNKNOWN_TARGET"\n',
        )
        self._write("AGENTS.md", _AGENTS_TWO_CODES)  # still only documents two
        violations = check_error_codes_match_contract(self.root)
        self.assertTrue(violations)
        self.assertIn("UNKNOWN_TARGET", str(violations[0]))
        self.assertIn("not documented", str(violations[0]))

    def test_code_only_in_agents_md_is_flagged(self):
        self._write("protocol.py", _PROTOCOL_TWO_CODES)
        self._write(
            "AGENTS.md",
            _AGENTS_TWO_CODES.replace("INVALID_JSON  BOARD_BUSY", "INVALID_JSON  BOARD_BUSY  GHOST_CODE"),
        )
        violations = check_error_codes_match_contract(self.root)
        self.assertTrue(violations)
        self.assertIn("GHOST_CODE", str(violations[0]))
        self.assertIn("do not", str(violations[0]))

    def test_lowercase_status_block_is_not_mistaken_for_error_codes(self):
        # The ok/error/timeout block must not be picked up as the code listing.
        self._write("protocol.py", _PROTOCOL_TWO_CODES)
        self._write(
            "AGENTS.md",
            _AGENTS_TWO_CODES + "\n```text\nok\nerror\ntimeout\n```\n",
        )
        self.assertEqual(check_error_codes_match_contract(self.root), [])

    # --- no blocking calls on the command path -----------------------------

    def test_time_sleep_in_command_path_is_flagged(self):
        self._write("controller.py", "import time\n\ndef f():\n    time.sleep(1)\n")
        violations = check_no_blocking_calls_in_command_path(self.root)
        self.assertTrue(violations)
        self.assertIn("time.sleep", str(violations[0]))

    def test_sleep_imported_from_time_is_flagged(self):
        self._write("protocol.py", "from time import sleep\n\ndef f():\n    sleep(1)\n")
        violations = check_no_blocking_calls_in_command_path(self.root)
        self.assertTrue(violations)
        self.assertIn("time.sleep", str(violations[0]))

    def test_blocking_import_in_command_path_is_flagged(self):
        self._write("board_connection.py", "import requests\n")
        violations = check_no_blocking_calls_in_command_path(self.root)
        self.assertTrue(violations)
        self.assertIn("requests", str(violations[0]))

    def test_time_monotonic_is_allowed(self):
        # `import time` for a non-blocking clock must not be flagged.
        self._write("controller.py", "import time\n\ndef now():\n    return time.monotonic()\n")
        self.assertEqual(check_no_blocking_calls_in_command_path(self.root), [])


if __name__ == "__main__":
    unittest.main()
