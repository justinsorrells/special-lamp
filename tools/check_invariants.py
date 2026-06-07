"""Static guardrail checks for the frozen Hyperloop architecture invariants.

This is a fast, dependency-free AST checker that an agent (or a pre-commit hook,
or the pytest suite) can run to catch the highest-signal "Forbidden changes" from
``AGENTS.md`` before they ever reach a diff:

* the terminal command status set must stay exactly ``{ok, error, timeout}``
  (new error *codes* are allowed; new *statuses* are not);
* Redis must never enter the command path (the core controller/protocol modules
  must not import it);
* client-facing modules must not import the board-communication modules (the GUI
  and other local clients talk to the controller over the Unix socket, never to
  boards directly).

It is intentionally conservative: every check targets an invariant that is
explicitly frozen in ``AGENTS.md`` / ``docs/contracts/V1_Networking_Decisions.md``,
so a violation is a real architecture regression rather than a style nit.

Usage::

    python tools/check_invariants.py          # checks the repo, exits non-zero on violations

The check functions are also importable (``from tools.check_invariants import
run_all_checks``) so they can be asserted from the test suite.
"""

from __future__ import annotations

import ast
import re
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# --- Frozen invariant configuration ----------------------------------------
# Keep these lists in sync with AGENTS.md ("Forbidden changes") if the
# authoritative contract ever changes. They are the single place to widen or
# narrow what the checker considers core vs. client-facing.

# The only terminal command statuses permitted by contract 3.12. New failure
# modes must be expressed as error *codes*, never as new statuses.
ALLOWED_TERMINAL_STATUSES = frozenset({"ok", "error", "timeout"})

# Core command-path modules. Redis is observability-only and must never sit in
# the path between a client request and a board (contract 1.7). The command path
# also must not import `observability` directly: the controller receives an
# observability sink by dependency injection, so a direct import would couple the
# command path to the telemetry/Redis backend it is supposed to stay decoupled
# from.
CORE_COMMAND_PATH = (
    "protocol.py",
    "controller.py",
    "interfaces.py",
    "state.py",
    "board_connection.py",
    "local_socket.py",
)
FORBIDDEN_CORE_IMPORTS = frozenset({"redis", "observability"})

# Demo / external-actor modules (GUI, local clients, and the mock board server).
# These simulate actors outside the controller and must reach boards only through
# the controller's Unix socket, never by importing the controller's internal
# command-path modules (topology invariant: the GUI does not talk to boards, and
# a mock board is not a controller). Discovered dynamically so a newly added demo
# script (e.g. demos/dashboard.py) is covered automatically.
DEMOS_DIR = "demos"
# Shared contract primitives (`protocol`, `state`) are deliberately NOT forbidden:
# an external actor may legitimately reuse the frozen message vocabulary/framing.
CONTROLLER_INTERNAL_MODULES = frozenset(
    {"controller", "board_connection", "interfaces", "local_socket"}
)

# The human-facing navigation file. Its documented error-code list must stay in
# lockstep with the `ErrorCode` enum in protocol.py: agents read AGENTS.md to know
# which codes exist, so silent drift between the two misleads them (and would let
# a stale doc "authorize" a code that no longer exists, or hide a new one).
AGENTS_MD = "AGENTS.md"

# Blocking, synchronous libraries that must never appear on an async command-path
# module. `redis` is handled by FORBIDDEN_CORE_IMPORTS (it is also an architecture
# violation, not just a blocking one); these are blocking-only offenders. The
# `time.sleep` *call* is handled separately since `import time` itself is fine
# (e.g. time.monotonic for clocks) -- only the blocking sleep is forbidden.
BLOCKING_IMPORT_NAMES = frozenset({"requests"})


@dataclass(frozen=True)
class Violation:
    check: str
    path: str
    detail: str

    def __str__(self) -> str:
        return f"[{self.check}] {self.path}: {self.detail}"


def _imported_top_level_modules(tree: ast.AST) -> set[str]:
    """Return the top-level module names imported by an absolute import.

    ``import a.b`` -> ``a``; ``from a.b import c`` -> ``a``. Relative imports
    (``from . import x``) are skipped since they cannot reach a third-party or
    cross-layer module by name.
    """

    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module:
                modules.add(node.module.split(".")[0])
    return modules


def _parse(path: Path) -> ast.AST:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def check_terminal_statuses_frozen(root: Path = REPO_ROOT) -> list[Violation]:
    """The ``TerminalStatus`` enum in protocol.py must equal the frozen set."""

    path = root / "protocol.py"
    if not path.exists():
        return [Violation("terminal-statuses", "protocol.py", "file is missing")]

    tree = _parse(path)
    defined: set[str] = set()
    found_enum = False
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "TerminalStatus":
            found_enum = True
            for stmt in node.body:
                # Members look like:  OK = "ok"  (Assign) or, defensively, an
                # annotated form  OK: str = "ok"  (AnnAssign). AnnAssign with no
                # value (annotation only) has stmt.value is None and is skipped.
                if isinstance(stmt, (ast.Assign, ast.AnnAssign)):
                    value = stmt.value
                    if isinstance(value, ast.Constant) and isinstance(value.value, str):
                        defined.add(value.value)

    if not found_enum:
        return [Violation("terminal-statuses", "protocol.py", "TerminalStatus enum not found")]

    violations: list[Violation] = []
    extra = defined - ALLOWED_TERMINAL_STATUSES
    missing = ALLOWED_TERMINAL_STATUSES - defined
    if extra:
        violations.append(
            Violation(
                "terminal-statuses",
                "protocol.py",
                f"new terminal status value(s) {sorted(extra)} are forbidden; "
                "add an ErrorCode instead",
            )
        )
    if missing:
        violations.append(
            Violation(
                "terminal-statuses",
                "protocol.py",
                f"terminal status value(s) {sorted(missing)} are missing from TerminalStatus",
            )
        )
    return violations


def check_core_command_path_imports(root: Path = REPO_ROOT) -> list[Violation]:
    """Core command-path modules must not import forbidden modules (e.g. redis)."""

    violations: list[Violation] = []
    for rel in CORE_COMMAND_PATH:
        path = root / rel
        if not path.exists():
            continue
        imported = _imported_top_level_modules(_parse(path))
        for forbidden in sorted(imported & FORBIDDEN_CORE_IMPORTS):
            violations.append(
                Violation(
                    "core-command-path",
                    rel,
                    f"imports '{forbidden}'; it must stay out of the command path "
                    "(Redis is observability-only)",
                )
            )
    return violations


def check_client_modules_isolated(root: Path = REPO_ROOT) -> list[Violation]:
    """
    Ensure demo modules under the demos/ directory do not import controller-internal modules.
    
    Scans all Python files under demos/ (demos/**/*.py) and records a Violation for each demo file that imports any top-level module listed in CONTROLLER_INTERNAL_MODULES.
    
    Returns:
        list[Violation]: Violations found, one per demo file and forbidden import; empty if none or if demos/ is absent.
    """

    violations: list[Violation] = []
    demos_dir = root / DEMOS_DIR
    if not demos_dir.exists():
        return violations
    for path in sorted(demos_dir.rglob("*.py")):
        rel = path.relative_to(root).as_posix()
        imported = _imported_top_level_modules(_parse(path))
        for forbidden in sorted(imported & CONTROLLER_INTERNAL_MODULES):
            violations.append(
                Violation(
                    "client-isolation",
                    rel,
                    f"imports controller-internal module '{forbidden}'; demo actors "
                    "must reach boards only through the controller's Unix socket",
                )
            )
    return violations


def _enum_string_values(tree: ast.AST, class_name: str) -> set[str] | None:
    """
    Collect string constant member values from a class named `class_name` in the given AST.
    
    Handles both simple assignments (`NAME = "value"`) and annotated assignments (`NAME: str = "value"`). 
    
    Returns:
        values (set[str] | None): A set of the class's string member values if the class exists in the AST, or `None` if the class is not found.
    """

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            values: set[str] = set()
            for stmt in node.body:
                if isinstance(stmt, (ast.Assign, ast.AnnAssign)):
                    value = stmt.value
                    if isinstance(value, ast.Constant) and isinstance(value.value, str):
                        values.add(value.value)
            return values
    return None


def _error_codes_in_agents_md(root: Path) -> set[str] | None:
    """
    Extract documented error-code identifiers from AGENTS.md.
    
    Searches fenced code blocks and returns the set of tokens from the block whose tokens are all uppercase identifiers (letters, digits, underscores), with at least one token containing an underscore—this selects the specific UPPER_SNAKE error-code block and avoids unrelated tokens in prose.
    
    Parameters:
        root (Path): Repository root directory containing AGENTS.md.
    
    Returns:
        set[str] | None: Set of documented error-code names if a suitable fenced block is found; `None` if AGENTS.md is missing or no matching block is found.
    """

    path = root / AGENTS_MD
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8")
    codes: set[str] = set()
    for block in re.findall(r"```[^\n]*\n(.*?)```", text, flags=re.DOTALL):
        tokens = block.split()
        if tokens and all(re.fullmatch(r"[A-Z][A-Z0-9_]*", t) for t in tokens) and any(
            "_" in t for t in tokens
        ):
            codes.update(tokens)
    return codes or None


def check_error_codes_match_contract(root: Path = REPO_ROOT) -> list[Violation]:
    """
    Check that the set of error codes defined in protocol.ErrorCode matches the error codes documented in AGENTS.md.
    
    Returns:
        list[Violation]: A list of violations describing mismatches (codes present in the enum but not documented, or documented but not present in the enum) or missing source files/sections; empty if the documented and defined sets are identical.
    """

    protocol_path = root / "protocol.py"
    if not protocol_path.exists():
        return [Violation("error-code-drift", "protocol.py", "file is missing")]

    defined = _enum_string_values(_parse(protocol_path), "ErrorCode")
    if defined is None:
        return [Violation("error-code-drift", "protocol.py", "ErrorCode enum not found")]

    documented = _error_codes_in_agents_md(root)
    if documented is None:
        return [Violation("error-code-drift", AGENTS_MD, "no error-code block found")]

    violations: list[Violation] = []
    missing = defined - documented
    extra = documented - defined
    if missing:
        violations.append(
            Violation(
                "error-code-drift",
                AGENTS_MD,
                f"error code(s) {sorted(missing)} exist in protocol.ErrorCode but are "
                "not documented in AGENTS.md",
            )
        )
    if extra:
        violations.append(
            Violation(
                "error-code-drift",
                AGENTS_MD,
                f"error code(s) {sorted(extra)} are documented in AGENTS.md but do not "
                "exist in protocol.ErrorCode",
            )
        )
    return violations


def _has_blocking_sleep(tree: ast.AST) -> bool:
    """
    Detect whether the AST contains blocking sleep calls from the time module.
    
    Checks for calls to `time.sleep(...)` or `sleep(...)` when `sleep` is imported from `time`; a plain `import time` without calling `sleep` is not considered a blocking call.
    
    Returns:
        true if a blocking sleep call is present, false otherwise.
    """

    sleep_imported_from_time = any(
        isinstance(node, ast.ImportFrom)
        and node.level == 0
        and node.module == "time"
        and any(alias.name == "sleep" for alias in node.names)
        for node in ast.walk(tree)
    )
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if (
            isinstance(func, ast.Attribute)
            and func.attr == "sleep"
            and isinstance(func.value, ast.Name)
            and func.value.id == "time"
        ):
            return True
        if sleep_imported_from_time and isinstance(func, ast.Name) and func.id == "sleep":
            return True
    return False


def check_no_blocking_calls_in_command_path(root: Path = REPO_ROOT) -> list[Violation]:
    """
    Check core async command-path modules for forbidden blocking operations.
    
    Scans each file listed in CORE_COMMAND_PATH under `root` and reports violations when a module:
    - imports a top-level blocking library listed in BLOCKING_IMPORT_NAMES (e.g., `requests`), or
    - invokes a blocking sleep (`time.sleep(...)` or `sleep(...)` imported from `time`).
    
    Parameters:
        root (Path): Repository root to resolve command-path module files from.
    
    Returns:
        list[Violation]: A list of `no-blocking-calls` violations describing the file and reason for each detected blocking usage.
    """

    violations: list[Violation] = []
    for rel in CORE_COMMAND_PATH:
        path = root / rel
        if not path.exists():
            continue
        tree = _parse(path)
        for forbidden in sorted(_imported_top_level_modules(tree) & BLOCKING_IMPORT_NAMES):
            violations.append(
                Violation(
                    "no-blocking-calls",
                    rel,
                    f"imports blocking library '{forbidden}'; the command path is async "
                    "and must not perform synchronous I/O",
                )
            )
        if _has_blocking_sleep(tree):
            violations.append(
                Violation(
                    "no-blocking-calls",
                    rel,
                    "calls time.sleep(); use 'await asyncio.sleep(...)' on async paths",
                )
            )
    return violations


def run_all_checks(root: Path = REPO_ROOT) -> list[Violation]:
    """
    Run all repository architecture invariant checks and collect any violations.
    
    Parameters:
        root (Path): Filesystem path to the repository root to scan; defaults to the repository root constant.
    
    Returns:
        violations (list[Violation]): A list of discovered Violation instances (empty if no violations).
    """

    violations: list[Violation] = []
    violations.extend(check_terminal_statuses_frozen(root))
    violations.extend(check_core_command_path_imports(root))
    violations.extend(check_client_modules_isolated(root))
    violations.extend(check_error_codes_match_contract(root))
    violations.extend(check_no_blocking_calls_in_command_path(root))
    return violations


def main(argv: list[str] | None = None) -> int:
    root = REPO_ROOT
    violations = run_all_checks(root)
    if violations:
        print(f"FAIL: {len(violations)} architecture invariant violation(s):")
        for v in violations:
            print(f"  - {v}")
        return 1
    print("OK: all architecture invariants hold")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
