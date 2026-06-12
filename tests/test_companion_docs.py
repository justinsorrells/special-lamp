from __future__ import annotations

import unittest
from pathlib import Path

from protocol import ERROR_CODES, TERMINAL_STATUSES
from state import BoardConnState

REPO_ROOT = Path(__file__).resolve().parent.parent
COMPANION_DIR = REPO_ROOT / "docs" / "companion"


class CompanionDocsTests(unittest.TestCase):
    def _read(self, name: str) -> str:
        return (COMPANION_DIR / name).read_text(encoding="utf-8")

    def test_companion_docs_exist_and_defer_to_frozen_contracts(self) -> None:
        expected = {
            "README.md",
            "Component_Handoff_Contracts.md",
            "Integration_Guide.md",
            "Local_Client_API.md",
            "Test_Matrix.md",
        }
        self.assertEqual({path.name for path in COMPANION_DIR.glob("*.md")}, expected)

        for name in expected:
            text = self._read(name)
            self.assertIn("docs/contracts/V1_Networking_Decisions.md", text)
            if name != "README.md":
                self.assertIn("docs/contracts/Board_Developer_Guide.md", text)
            self.assertIn("companion", text.lower())
            self.assertIn("frozen contracts win", text)

    def test_readme_links_every_companion_file_and_frozen_topology(self) -> None:
        text = self._read("README.md")
        for name in (
            "Component_Handoff_Contracts.md",
            "Integration_Guide.md",
            "Local_Client_API.md",
            "Test_Matrix.md",
        ):
            self.assertIn(name, text)
        self.assertIn(
            "local client -> Unix socket -> asyncio controller -> persistent TCP -> board",
            text,
        )
        self.assertIn("Redis is observability/read-replica only", text)

    def test_component_handoff_documents_shared_module_boundaries(self) -> None:
        text = self._read("Component_Handoff_Contracts.md")
        for module in (
            "protocol.py",
            "state.py",
            "interfaces.py",
            "controller.py",
            "board_connection.py",
            "local_socket.py",
            "observability.py",
        ):
            self.assertIn(module, text)
        for status in TERMINAL_STATUSES:
            self.assertIn(f"`{status}`", text)
        for code in (
            "BOARD_UNAVAILABLE",
            "BOARD_BUSY",
            "COMMAND_TIMEOUT",
            "ESTOP_ACTIVE",
            "CONTROLLER_SHUTDOWN",
            "PROTOCOL_VERSION_MISMATCH",
        ):
            self.assertIn(code, text)
            self.assertIn(code, ERROR_CODES)
        self.assertIn("writer lock", text)
        self.assertIn("pop-wins", text)
        self.assertIn("Redis records are mirror schemas only", text)

    def test_integration_guide_preserves_board_client_and_estop_boundaries(self) -> None:
        text = self._read("Integration_Guide.md")
        for state in BoardConnState:
            if state is BoardConnState.REGISTERED:
                self.assertIn(f"`{state.value}`", text)
        self.assertIn("The controller connects to each board", text)
        self.assertIn("Do not add a GUI-to-board path", text)
        self.assertIn("Missing means blocked", text)
        self.assertIn("Clients must read continuously", text)
        self.assertIn('command: "get_schemas"', text)
        self.assertIn("64 KiB", text)
        self.assertIn("does not auto-clear", text)
        self.assertIn("operator `estop_reset`", text)
        self.assertIn("Redis is optional observability infrastructure", text)

    def test_test_matrix_covers_boundary_conditions_from_contract(self) -> None:
        text = self._read("Test_Matrix.md")
        required_terms = (
            "8 KB",
            "1 KB",
            "BOARD_UNAVAILABLE",
            "BOARD_BUSY",
            "COMMAND_TIMEOUT",
            "ESTOP_ACTIVE",
            "CONTROLLER_SHUTDOWN",
            "pop-wins",
            "controller_ts",
            "blocked_by_estop",
            "estop_ack",
            "obs_dropped",
            "drop-oldest",
        )
        for term in required_terms:
            self.assertIn(term, text)
        for status in TERMINAL_STATUSES:
            self.assertIn(f"`{status}`", text)

    def test_local_client_api_documents_schema_discovery_shape(self) -> None:
        text = self._read("Local_Client_API.md")
        self.assertIn('"target":"controller"', text)
        self.assertIn('"command":"get_schemas"', text)
        self.assertIn('"result":{"boards"', text)
        self.assertIn("UNKNOWN_TARGET", text)
        self.assertIn("INVALID_ARGUMENT", text)
        self.assertIn("schema_updated", text)
        self.assertIn("64 KiB", text)
