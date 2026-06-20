"""Generate the study-grade codebase reference (docs/companion/Codebase_Reference.html).

This tool walks every tracked Python file in the repository and emits a single,
self-contained HTML page with one tab per file. Each tab carries:

* an authored file overview, layer badge, and architectural-decision notes
  (curated; tied to the frozen contracts where a decision is contract-driven), and
* a complete, in-source-order breakdown of *every* top-level statement, class,
  method, and function, each in its own collapsible dropdown showing a structured
  "mechanics" explanation derived from the AST plus the **verbatim source**.

The source embedded here is copy-faithful: it is sliced directly out of the files
on disk, so regenerating after a code change keeps the reference in sync. Run:

    python tools/gen_codebase_reference.py

The page is companion material, not a protocol extension. The frozen contracts in
``docs/contracts/`` remain authoritative; if this reference disagrees, the frozen
contracts win.
"""

from __future__ import annotations

import argparse
import ast
import html
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT = REPO_ROOT / "docs" / "companion" / "Codebase_Reference.html"

# Must match tests/test_companion_docs.py::_repo_python_files exactly so every
# discovered file gets a tab and the parity test passes by construction.
IGNORED_DIRS = {".git", ".mypy_cache", ".pytest_cache", ".ruff_cache", ".venv", "__pycache__"}

# Curated ordering inside each group; files not listed fall through alphabetically.
GROUP_ORDER = ["Core stack", "Tools", "Demos", "Tests", "Other"]
CORE_ORDER = [
    "protocol.py",
    "state.py",
    "interfaces.py",
    "controller.py",
    "board_connection.py",
    "local_socket.py",
    "observability.py",
    "runtime.py",
]


def discover_python_files() -> list[str]:
    files = {
        path.relative_to(REPO_ROOT).as_posix()
        for path in REPO_ROOT.rglob("*.py")
        if not IGNORED_DIRS.intersection(path.parts)
    }
    return sorted(files)


def group_of(rel: str) -> str:
    if rel.startswith("tests/"):
        return "Tests"
    if rel.startswith("demos/"):
        return "Demos"
    if rel.startswith("tools/"):
        return "Tools"
    if "/" not in rel:
        return "Core stack"
    return "Other"


def order_key(rel: str) -> tuple[int, str]:
    if rel in CORE_ORDER:
        return (CORE_ORDER.index(rel), rel)
    return (len(CORE_ORDER), rel)


def slug(rel: str) -> str:
    stem = rel[:-3] if rel.endswith(".py") else rel
    s = re.sub(r"[^a-zA-Z0-9]+", "-", stem).strip("-").lower()
    return "tab-" + s


# --------------------------------------------------------------------------- #
# AST extraction
# --------------------------------------------------------------------------- #


@dataclass
class Member:
    kind: str  # "imports" | "module-code" | "function" | "class"
    title: str  # signature / heading shown in the <summary>
    anchor: str
    lineno: int
    end_lineno: int
    source: str
    kind_html: str  # the chip line of badges (async coroutine, dataclass, ...)
    mech_html: str  # the structured "Mechanics" table (returns/raises/awaits/...)
    narrative_html: str  # the authored or synthesized "What it does" prose
    name: str = ""  # bare symbol name used for cross-referencing
    qualname: str = ""  # e.g. "ControllerCore.handle_client_command"
    file: str = ""  # repo-relative path of the defining file
    used_by_html: str = ""  # filled in a second pass once the xref index exists
    children: list[Member] = field(default_factory=list)


def _node_end(node: ast.stmt) -> int:
    return node.end_lineno if node.end_lineno is not None else node.lineno


def _src_slice(lines: list[str], node: ast.stmt) -> str:
    start = node.lineno
    decorators = getattr(node, "decorator_list", None)
    if decorators:
        start = min(start, min(d.lineno for d in decorators))
    return "\n".join(lines[start - 1 : _node_end(node)])


def _format_arg(arg: ast.arg, default: ast.expr | None) -> str:
    text = arg.arg
    if arg.annotation is not None:
        text += ": " + ast.unparse(arg.annotation)
    if default is not None:
        sep = "=" if arg.annotation is None else " = "
        text += sep + ast.unparse(default)
    return text


def format_signature(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    a = node.args
    parts: list[str] = []

    posonly = list(a.posonlyargs)
    normal = list(a.args)
    # Align defaults to the tail of posonly+normal.
    positional = posonly + normal
    defaults = list(a.defaults)
    pad = [None] * (len(positional) - len(defaults))
    pos_defaults = pad + defaults

    for i, arg in enumerate(posonly):
        parts.append(_format_arg(arg, pos_defaults[i]))
    if posonly:
        parts.append("/")
    for j, arg in enumerate(normal):
        parts.append(_format_arg(arg, pos_defaults[len(posonly) + j]))

    if a.vararg is not None:
        parts.append("*" + _format_arg(a.vararg, None))
    elif a.kwonlyargs:
        parts.append("*")
    for arg, default in zip(a.kwonlyargs, a.kw_defaults, strict=True):
        parts.append(_format_arg(arg, default))
    if a.kwarg is not None:
        parts.append("**" + _format_arg(a.kwarg, None))

    prefix = "async def " if isinstance(node, ast.AsyncFunctionDef) else "def "
    sig = f"{prefix}{node.name}({', '.join(parts)})"
    if node.returns is not None:
        sig += " -> " + ast.unparse(node.returns)
    return sig


def _direct_body(node: ast.AST):
    """Walk a callable/class body without descending into nested scopes."""
    stack = list(getattr(node, "body", []))
    while stack:
        current = stack.pop()
        yield current
        for child in ast.iter_child_nodes(current):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
                continue
            stack.append(child)


def _call_name(call: ast.Call) -> str | None:
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _dedupe(values: list[str], limit: int = 10) -> list[str]:
    seen: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.append(value)
        if len(seen) >= limit:
            break
    return seen


@dataclass
class CallableFacts:
    is_async: bool
    decorators: list[str]
    returns: str | None
    returns_value: bool
    yields: bool
    awaits: list[str]
    calls: list[str]
    self_writes: list[str]
    raises: list[str]
    logs: bool
    has_loop: bool
    has_try: bool


def gather_callable_facts(node: ast.FunctionDef | ast.AsyncFunctionDef) -> CallableFacts:
    decorators = [ast.unparse(d) for d in node.decorator_list]
    raises: list[str] = []
    returns_value = False
    yields = False
    awaits: list[str] = []
    calls: list[str] = []
    self_writes: list[str] = []
    logs = False
    has_loop = False
    has_try = False

    for n in _direct_body(node):
        if isinstance(n, ast.Raise) and n.exc is not None:
            exc = n.exc.func if isinstance(n.exc, ast.Call) else n.exc
            try:
                raises.append(ast.unparse(exc))
            except Exception:  # pragma: no cover - defensive
                pass
        elif isinstance(n, ast.Return) and n.value is not None:
            returns_value = True
        elif isinstance(n, (ast.Yield, ast.YieldFrom)):
            yields = True
        elif isinstance(n, ast.Await):
            try:
                inner = n.value
                name = _call_name(inner) if isinstance(inner, ast.Call) else ast.unparse(inner)
                if name:
                    awaits.append(name)
            except Exception:  # pragma: no cover - defensive
                pass
        elif isinstance(n, ast.Call):
            name = _call_name(n)
            if name:
                calls.append(name)
                if isinstance(n.func, ast.Attribute) and isinstance(n.func.value, ast.Name):
                    if n.func.value.id in {"log", "logger", "_log", "LOGGER"}:
                        logs = True
        elif isinstance(n, (ast.For, ast.AsyncFor, ast.While)):
            has_loop = True
        elif isinstance(n, ast.Try):
            has_try = True
        if isinstance(n, (ast.Assign, ast.AugAssign, ast.AnnAssign)):
            targets = n.targets if isinstance(n, ast.Assign) else [n.target]
            for tgt in targets:
                if (
                    isinstance(tgt, ast.Attribute)
                    and isinstance(tgt.value, ast.Name)
                    and tgt.value.id == "self"
                ):
                    self_writes.append("self." + tgt.attr)

    return CallableFacts(
        is_async=isinstance(node, ast.AsyncFunctionDef),
        decorators=decorators,
        returns=ast.unparse(node.returns) if node.returns is not None else None,
        returns_value=returns_value,
        yields=yields,
        awaits=awaits,
        calls=calls,
        self_writes=self_writes,
        raises=raises,
        logs=logs,
        has_loop=has_loop,
        has_try=has_try,
    )


def _mech_row(label: str, value: str) -> str:
    return f"<dt>{html.escape(label)}</dt><dd><code>{html.escape(value)}</code></dd>"


def _codes(names: list[str], limit: int = 8) -> str:
    return ", ".join(f"<code>{html.escape(n)}</code>" for n in _dedupe(names, limit))


def callable_kind_label(facts: CallableFacts, *, is_method: bool, name: str) -> str:
    if facts.is_async:
        base = "Async coroutine"
    elif "property" in facts.decorators:
        base = "Computed property"
    elif "staticmethod" in facts.decorators:
        base = "Static method"
    elif "classmethod" in facts.decorators:
        base = "Class method"
    elif is_method:
        base = "Method"
    else:
        base = "Function"
    if name.startswith("__") and name.endswith("__"):
        return base + " (special/dunder)"
    if name.startswith("_") and base in {"Function", "Method", "Async coroutine"}:
        return ("Internal coroutine" if facts.is_async else f"Internal {base.lower()}")
    return base


def callable_kind_html(facts: CallableFacts) -> str:
    badges: list[str] = []
    if facts.is_async:
        badges.append("async coroutine")
    elif "property" in facts.decorators:
        badges.append("property")
    elif any(d in {"staticmethod", "classmethod"} for d in facts.decorators):
        badges.append(facts.decorators[0])
    else:
        badges.append("function")
    if facts.yields:
        badges.append("generator")
    if facts.returns_value:
        badges.append("returns a value")
    if facts.has_loop:
        badges.append("loops")
    if facts.has_try:
        badges.append("handles exceptions")
    if facts.logs:
        badges.append("logs")
    return f'<p class="kind">{" &middot; ".join(html.escape(b) for b in badges)}</p>'


def callable_mech_html(facts: CallableFacts) -> str:
    rows: list[str] = []
    if facts.decorators:
        rows.append(_mech_row("Decorators", ", ".join("@" + d for d in facts.decorators)))
    if facts.returns is not None:
        rows.append(_mech_row("Returns", facts.returns))
    if facts.raises:
        rows.append(_mech_row("Raises", ", ".join(_dedupe(facts.raises, 8))))
    if facts.awaits:
        rows.append(_mech_row("Awaits", ", ".join(_dedupe(facts.awaits, 12))))
    if facts.self_writes:
        rows.append(_mech_row("Updates", ", ".join(_dedupe(facts.self_writes, 12))))
    if facts.calls:
        rows.append(_mech_row("Calls", ", ".join(_dedupe(facts.calls, 14))))
    return ('<dl class="mech">' + "".join(rows) + "</dl>") if rows else ""


def synth_callable_narrative(
    node: ast.FunctionDef | ast.AsyncFunctionDef,
    facts: CallableFacts,
    *,
    is_method: bool,
    class_name: str,
    name: str,
) -> str:
    """Authored prose wins; otherwise lead with the docstring; otherwise synthesize from the AST."""
    lead = callable_kind_label(facts, is_method=is_method, name=name)
    if is_method and class_name:
        lead += f" on <code>{html.escape(class_name)}</code>"
    sentences = [f"{lead}."]

    doc = ast.get_docstring(node)
    if doc:
        sentences.append(html.escape(doc).replace("\n", "<br>"))

    clauses: list[str] = []
    if facts.returns and facts.returns != "None":
        clauses.append(f"returns <code>{html.escape(facts.returns)}</code>")
    elif facts.returns_value:
        clauses.append("returns a value")
    if facts.yields:
        clauses.append("yields items (generator)")
    if facts.awaits:
        clauses.append("awaits " + _codes(facts.awaits, 4))
    if facts.self_writes:
        clauses.append("updates " + _codes(facts.self_writes, 5))
    if facts.raises:
        clauses.append("may raise " + _codes(facts.raises, 4))
    if facts.has_loop:
        clauses.append("iterates in a loop")
    if facts.has_try:
        clauses.append("handles exceptions")
    if facts.calls:
        clauses.append("delegates to " + _codes(facts.calls, 5))
    if clauses and not doc:
        sentences.append("Mechanically, it " + "; ".join(clauses) + ".")
    return "".join(f"<p>{s}</p>" for s in sentences)


def class_facts(node: ast.ClassDef) -> tuple[str, str]:
    """Return (kind_html, mech_html) for a class."""
    bases = [ast.unparse(b) for b in node.bases]
    keywords = [ast.unparse(k) for k in node.keywords]
    decorators = [ast.unparse(d) for d in node.decorator_list]
    methods = [b for b in node.body if isinstance(b, (ast.FunctionDef, ast.AsyncFunctionDef))]
    fields = [b for b in node.body if isinstance(b, (ast.Assign, ast.AnnAssign))]

    badges = ["class"]
    if any(d.startswith("dataclass") for d in decorators):
        badges.append("dataclass")
    if any("Enum" in b or "StrEnum" in b for b in bases):
        badges.append("enum")
    if any(b in {"Protocol", "ABC"} for b in bases):
        badges.append("interface")
    badges.append(f"{len(methods)} method(s)")
    badges.append(f"{len(fields)} field(s)")

    rows: list[str] = []
    if decorators:
        rows.append(_mech_row("Decorators", ", ".join("@" + d for d in decorators)))
    if bases or keywords:
        rows.append(_mech_row("Bases", ", ".join(bases + keywords)))

    kind_html = f'<p class="kind">{" &middot; ".join(html.escape(b) for b in badges)}</p>'
    mech_html = ('<dl class="mech">' + "".join(rows) + "</dl>") if rows else ""
    return kind_html, mech_html


def synth_class_narrative(node: ast.ClassDef) -> str:
    bases = [ast.unparse(b) for b in node.bases]
    decorators = [ast.unparse(d) for d in node.decorator_list]
    methods = [b for b in node.body if isinstance(b, (ast.FunctionDef, ast.AsyncFunctionDef))]
    fields = [b for b in node.body if isinstance(b, (ast.Assign, ast.AnnAssign))]

    if any("frozen=True" in d for d in decorators):
        lead = "Frozen (immutable) dataclass value object"
    elif any(d.startswith("dataclass") for d in decorators):
        lead = "Dataclass"
    elif any("StrEnum" in b for b in bases):
        lead = "String-valued enumeration (closed vocabulary)"
    elif any("Enum" in b for b in bases):
        lead = "Enumeration"
    elif any(b in {"Protocol"} for b in bases):
        lead = "Structural <code>Protocol</code> (an injection seam, not a concrete type)"
    elif any("Error" in b or "Exception" in b for b in bases):
        lead = "Exception type"
    else:
        lead = "Class"
    sentences = [f"{lead}" + (f", subclassing <code>{html.escape(', '.join(bases))}</code>" if bases else "") + "."]

    doc = ast.get_docstring(node)
    if doc:
        sentences.append(html.escape(doc).replace("\n", "<br>"))
    else:
        detail = []
        if fields:
            detail.append(f"defines {len(fields)} field(s)")
        if methods:
            detail.append(f"exposes {len(methods)} method(s) (see below)")
        if detail:
            sentences.append("It " + " and ".join(detail) + ".")
    return "".join(f"<p>{s}</p>" for s in sentences)


def class_header_source(lines: list[str], node: ast.ClassDef) -> str:
    """Class definition line(s) plus field/enum-member statements, no method bodies."""
    pieces: list[str] = []
    first_body_line = node.body[0].lineno if node.body else _node_end(node)
    header_start = node.lineno
    if node.decorator_list:
        header_start = min(header_start, min(d.lineno for d in node.decorator_list))
    pieces.append("\n".join(lines[header_start - 1 : first_body_line - 1]).rstrip())
    for stmt in node.body:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        pieces.append(_src_slice(lines, stmt))
    return "\n".join(p for p in pieces if p.strip())


def extract_members(rel: str, source: str) -> tuple[list[Member], int]:
    lines = source.splitlines()
    tree = ast.parse(source)
    members: list[Member] = []
    misc_buffer: list[ast.stmt] = []
    counter = {"n": 0}

    def next_anchor(name: str) -> str:
        counter["n"] += 1
        return f"{slug(rel)}--{counter['n']}-{re.sub(r'[^a-zA-Z0-9]+', '-', name).strip('-').lower()}"

    def flush_misc() -> None:
        if not misc_buffer:
            return
        imports_only = all(isinstance(s, (ast.Import, ast.ImportFrom)) for s in misc_buffer)
        start = misc_buffer[0].lineno
        end = _node_end(misc_buffer[-1])
        src = "\n".join(_src_slice(lines, s) for s in misc_buffer)
        title = (
            f"Imports (lines {start}–{end})"
            if imports_only
            else f"Module-level statements (lines {start}–{end})"
        )
        note = (
            "Import statements that establish this module's dependencies."
            if imports_only
            else "Module-level constants, configuration, and top-level code executed at import."
        )
        members.append(
            Member(
                kind="imports" if imports_only else "module-code",
                title=title,
                anchor=next_anchor("module"),
                lineno=start,
                end_lineno=end,
                source=src,
                kind_html='<p class="kind">module scope</p>',
                mech_html="",
                narrative_html=f"<p>{note}</p>",
                file=rel,
            )
        )
        misc_buffer.clear()

    def build_callable(node: ast.FunctionDef | ast.AsyncFunctionDef, *, class_name: str) -> Member:
        facts = gather_callable_facts(node)
        qual = f"{class_name}.{node.name}" if class_name else node.name
        authored = SYMBOL_DOCS.get((rel, qual))
        narrative = authored or synth_callable_narrative(
            node, facts, is_method=bool(class_name), class_name=class_name, name=node.name
        )
        anchor_seed = f"{class_name}-{node.name}" if class_name else node.name
        return Member(
            kind="function",
            title=format_signature(node),
            anchor=next_anchor(anchor_seed),
            lineno=node.lineno,
            end_lineno=_node_end(node),
            source=_src_slice(lines, node),
            kind_html=callable_kind_html(facts),
            mech_html=callable_mech_html(facts),
            narrative_html=narrative,
            name=node.name,
            qualname=qual,
            file=rel,
        )

    for stmt in tree.body:
        if isinstance(stmt, ast.ClassDef):
            flush_misc()
            child_members = [
                build_callable(sub, class_name=stmt.name)
                for sub in stmt.body
                if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef))
            ]
            kind_html, mech_html = class_facts(stmt)
            authored = SYMBOL_DOCS.get((rel, stmt.name))
            members.append(
                Member(
                    kind="class",
                    title=f"class {stmt.name}",
                    anchor=next_anchor(stmt.name),
                    lineno=stmt.lineno,
                    end_lineno=_node_end(stmt),
                    source=class_header_source(lines, stmt),
                    kind_html=kind_html,
                    mech_html=mech_html,
                    narrative_html=authored or synth_class_narrative(stmt),
                    name=stmt.name,
                    qualname=stmt.name,
                    file=rel,
                    children=child_members,
                )
            )
        elif isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
            flush_misc()
            members.append(build_callable(stmt, class_name=""))
        else:
            misc_buffer.append(stmt)
    flush_misc()
    return members, len(lines)


def module_docstring(source: str) -> str | None:
    return ast.get_docstring(ast.parse(source))


# --------------------------------------------------------------------------- #
# Authored, curated per-file notes (layer, overview, architectural decisions)
# --------------------------------------------------------------------------- #


@dataclass
class FileDoc:
    layer: str
    badge: str
    overview: str
    decisions: list[str]


FILE_DOCS: dict[str, FileDoc] = {
    "protocol.py": FileDoc(
        layer="shared primitive",
        badge="Read first",
        overview=(
            "Defines the newline-delimited JSON contract primitives used by every runtime "
            "boundary. It deliberately contains no socket, Redis, GUI, or firmware integration, "
            "which keeps validation and serialization independent from routing decisions."
        ),
        decisions=[
            "<code>MessageType</code>, <code>TerminalStatus</code>, and <code>ErrorCode</code> "
            "are closed contract vocabularies; new failure modes are error <em>codes</em>, never "
            "new statuses (contract 3.11/3.12).",
            "Client <code>seq</code> and controller-owned <code>board_seq</code> are intentionally "
            "separate sequence spaces and must never be conflated (contract 1.1/3.1).",
            "<code>pop_pending()</code> is the shared pop-wins helper: a late, duplicate, "
            "timed-out, or post-disconnect response is dropped and logged, never double-resolved "
            "(contract 1.8).",
            "<code>controller_ts</code> is a monotonic round-trip token, not wall-clock time; "
            "<code>blocked_by_estop</code> defaults to <em>blocked</em> when absent (contract 1.17).",
            "Receive-side line limits are enforced here (8 KiB controller / 1 KiB board) and "
            "malformed input becomes a structured error, never a crash (contract 1.9).",
        ],
    ),
    "state.py": FileDoc(
        layer="shared primitive",
        badge="State shapes",
        overview=(
            "Holds the in-memory controller state and Redis mirror schemas without performing I/O. "
            "This is the best place to understand the two orthogonal axes: per-board connection "
            "state and global/per-board safety state."
        ),
        decisions=[
            "<code>BoardConnState</code> is the frozen connection axis (disconnected, connecting, "
            "connected, registered, faulted); it is kept orthogonal to safety state (contract 2).",
            "<code>SystemState.estop_active</code> is the single global latch, cleared only by an "
            "operator <code>estop_reset</code>; it is never auto-cleared and never gated on "
            "per-board acks (contract 1.13/2.2).",
            "<code>BoardState.estop_ack</code> tracks per-board convergence and is observability, "
            "not a dispatch gate.",
            "<code>PendingCommand.__post_init__()</code> enforces the 10 s execution-timeout "
            "ceiling (contract 1.5).",
            "<code>BoardStateRecord</code> / <code>SystemStateRecord</code> are Redis mirror "
            "payloads exposing current values; they are not authority objects.",
        ],
    ),
    "interfaces.py": FileDoc(
        layer="shared primitive",
        badge="Injection seams",
        overview=(
            "Defines the protocol-style handoff boundaries between controller components. These "
            "seams keep the command path decoupled from concrete sockets, Redis clients, and "
            "local-client implementations."
        ),
        decisions=[
            "<code>SerializedBoardWriter</code> wraps a byte-writing callback with newline-JSON "
            "serialization and an <code>asyncio.Lock</code>: no two coroutines ever write the same "
            "board stream concurrently (contract 1.19).",
            "<code>send_estop()</code> bypasses the per-board FIFO and the in-flight slot but still "
            "acquires the same per-board writer lock as normal writes (contract 1.19).",
            "<code>ClientReplyHandle</code> centralizes response/event delivery and is where "
            "orphaned local-client responses are counted.",
            "<code>BoardDownHandler</code> is the connection-manager-to-dispatcher drain hook used "
            "to fail pending/FIFO work on disconnect (contract 1.11).",
            "This module stays free of concrete TCP, Unix socket, Redis, and GUI imports by design.",
        ],
    ),
    "controller.py": FileDoc(
        layer="controller core",
        badge="Dispatch owner",
        overview=(
            "Implements the in-memory asyncio controller core. It owns command routing, per-board "
            "FIFO behavior, pending resolution, e-stop latching/gating, board-down drains, schema "
            "discovery for local clients, lifecycle counters, and graceful shutdown."
        ),
        decisions=[
            "Single authority: the controller owns all board communication and all board state; "
            "the GUI never talks to boards directly.",
            "One command in flight per board; further commands queue in a bounded per-board FIFO "
            "and are rejected with <code>BOARD_BUSY</code> when full (contract 1.2).",
            "Queued commands have a residency cap; the execution-timeout clock starts only after "
            "the board write, under a 10 s hard ceiling (contract 1.5).",
            "In-flight commands are not synthetically failed on e-stop because they may already "
            "have physical effects; command gating during e-stop is schema-driven (contract 1.17).",
            "Observability is injected; the command path must keep working with Redis disabled or "
            "unavailable (contract 1.7).",
        ],
    ),
    "board_connection.py": FileDoc(
        layer="board TCP adapter",
        badge="Persistent board transport",
        overview=(
            "Maintains controller-to-board TCP connections. Boards are the TCP servers; this "
            "module connects to configured endpoints, requires schema as the first message, "
            "forwards unsolicited telemetry/events, and reports disconnects back to the core."
        ),
        decisions=[
            "The board is the server and the controller connects; connections are persistent and "
            "reconnect with exponential full-jitter backoff (contract 1.4).",
            "Registration fails if the first message is not a schema, or if the board ID / protocol "
            "version is wrong (contract 1.3 / PROTOCOL_VERSION_MISMATCH).",
            "Telemetry is one-way push (~50 ms) and drives liveness; <code>event: estop_ack</code> "
            "is unsolicited and flips <code>board.estop_ack</code> via the core (contract 1.6).",
            "Controller accepts board inbound lines up to 8 KiB; outbound commands must fit the "
            "board's 1 KiB limit (contract 1.9).",
            "On read failure or disconnect, <code>board_down()</code> hands ownership back so the "
            "core fails pending/FIFO entries with <code>BOARD_UNAVAILABLE</code> (contract 1.11).",
        ],
    ),
    "local_socket.py": FileDoc(
        layer="local Unix socket adapter",
        badge="Full-duplex clients",
        overview=(
            "Implements the local client boundary over Unix sockets. Local clients send "
            "newline-JSON requests and must continuously read both request-correlated responses "
            "and unsolicited controller events."
        ),
        decisions=[
            "Each client gets one bounded outbound queue (depth 1000) so a slow client cannot "
            "block the command path or other clients (contract 1.14).",
            "Non-critical events are drop-oldest under pressure; responses and e-stop/safety events "
            "are critical and are never silently dropped.",
            "A client whose critical queue saturates is disconnected rather than allowed to wedge "
            "the broadcaster.",
            "The socket path is guarded against stale files and live-controller conflicts on bind.",
            "This module never talks directly to boards; it is purely the client edge.",
        ],
    ),
    "observability.py": FileDoc(
        layer="observability",
        badge="Redis side channel",
        overview=(
            "Contains the bounded observability queue, best-effort Redis writer, and serializers "
            "for board state, system state, telemetry, controller events, and command lifecycle "
            "records."
        ),
        decisions=[
            "The observability queue is bounded drop-oldest — intentionally the opposite of "
            "command FIFO behavior — so telemetry can never apply backpressure to dispatch.",
            "Redis is a read-replica / logging target only; write failures are counted and logged, "
            "never raised into the command path (contract 1.7).",
            "<code>serialize_command_lifecycle()</code> validates terminal statuses before emitting "
            "records, keeping the frozen status set honest.",
            "<code>redis=None</code> is a supported disabled mode for tests and local operation.",
            "Serialization preserves connection state and safety state as separate fields.",
        ],
    ),
    "runtime.py": FileDoc(
        layer="process composition",
        badge="Startup/shutdown",
        overview=(
            "Composes the controller core, board TCP connections, local Unix socket server, and "
            "optional observability worker into a process runtime. It loads TOML configuration and "
            "installs signal handlers."
        ),
        decisions=[
            "Startup waits for components that must be ready; reconnecting board tasks keep retrying "
            "forever rather than failing startup.",
            "Shutdown is idempotent and delegates command draining to the controller core, which "
            "fails remaining work with <code>CONTROLLER_SHUTDOWN</code> (never a new status).",
            "Redis construction is optional and isolated from command-path code.",
            "Config helpers validate positive numeric values and typed mappings up front.",
            "SIGINT/SIGTERM schedule the same callable shutdown path.",
        ],
    ),
    "demos/client/client.py": FileDoc(
        layer="demo client",
        badge="Unix socket CLI",
        overview=(
            "A command-line local client that connects to the controller Unix socket, sends one "
            "command or watches events, and prints JSON responses. It demonstrates the expected "
            "full-duplex local-client behavior."
        ),
        decisions=[
            "The demo goes through the Unix socket and never contacts board TCP endpoints.",
            "It uses compact protocol messages so examples stay close to the real client boundary.",
            "Watching events is not optional for production clients; e-stop and state events can "
            "arrive without request correlation.",
        ],
    ),
    "demos/run_controller_redis.py": FileDoc(
        layer="demo launcher",
        badge="Runtime wrapper",
        overview=(
            "Small command-line entry point for running the controller runtime with Redis "
            "configuration. It keeps demo startup logic out of the core runtime module."
        ),
        decisions=[
            "This is a composition demo, not a separate controller implementation.",
            "Redis stays optional observability infrastructure; the command topology is unchanged.",
        ],
    ),
    "demos/server/server.py": FileDoc(
        layer="demo board",
        badge="Mock TCP board",
        overview=(
            "A mock board TCP server for local testing and demonstrations. It pushes schema on "
            "connect, sends telemetry, handles registered commands, and emits e-stop "
            "acknowledgements after applying local mock safe state."
        ),
        decisions=[
            "The mock board is a TCP server, matching the frozen topology.",
            "Telemetry is pushed without solicitation.",
            "The schema declares <code>blocked_by_estop</code> per command so controller gating can "
            "be exercised end to end.",
        ],
    ),
    "demos/webapp.py": FileDoc(
        layer="minimal web demo",
        badge="FastAPI demo",
        overview=(
            "A tiny FastAPI example that sends controller-local commands through the Unix socket. "
            "It is intentionally minimal and stays outside the controller internals."
        ),
        decisions=[
            "The web demo uses the same local-client boundary as any GUI.",
            "It does not import or call board TCP code directly.",
            "For richer state/event handling, study <code>demos/webapp_dashboard.py</code>.",
        ],
    ),
    "demos/webapp_dashboard.py": FileDoc(
        layer="dashboard demo",
        badge="FastAPI dashboard",
        overview=(
            "A richer web dashboard around the local controller socket and optional Redis "
            "observability. It exposes board schemas, command forms, state snapshots, event "
            "streams, and diagnostics without changing the command path."
        ),
        decisions=[
            "The dashboard learns board capabilities from controller-local schema discovery.",
            "Redis data is read as observability state and cannot authorize or complete commands.",
            "Command submission still goes through the Unix socket to the controller.",
            "UI state reacts to unsolicited e-stop and board-state events.",
        ],
    ),
    "tools/check_invariants.py": FileDoc(
        layer="tool",
        badge="Static invariant checker",
        overview=(
            "Runs AST and text checks that enforce repository-level architecture rules. It catches "
            "status vocabulary drift, error-code mismatches, forbidden command-path imports, direct "
            "client-to-board coupling, blocking sleeps, and root contract duplication."
        ),
        decisions=[
            "This tool is a guardrail for the contracts, not a replacement for tests.",
            "When it fails, fix the architectural drift rather than weakening the checker.",
            "Run it after changes touching protocol, state, controller, transports, demos, or docs.",
        ],
    ),
    "tools/agent_orchestrator/orchestrate.py": FileDoc(
        layer="tool",
        badge="Agent workflow automation",
        overview=(
            "A local orchestration tool for multi-agent task selection, implementation, review, "
            "adjudication, and optional commit handling. It is tooling around the repository, not "
            "part of the controller runtime."
        ),
        decisions=[
            "It has its own operational safety rules (never-auto policies) distinct from the stack.",
            "It must not be imported by controller runtime code.",
            "Git helpers are centralized so workflow policy can be tested.",
        ],
    ),
    "tools/gen_codebase_reference.py": FileDoc(
        layer="tool",
        badge="This generator",
        overview=(
            "Generates this very page. It walks every tracked Python file, extracts each top-level "
            "statement, class, method, and function with verbatim source, derives a structured "
            "mechanics summary from the AST, and renders the tabbed HTML. Regenerate after code "
            "changes to keep the embedded source faithful."
        ),
        decisions=[
            "File discovery mirrors <code>tests/test_companion_docs.py</code> exactly so every "
            "repo Python file gets exactly one tab.",
            "Source is sliced directly from disk (decorators included) so it is copy-faithful.",
            "Explanations are derived from the AST and docstrings, plus the curated notes in this "
            "module; nothing about protocol behavior is invented here.",
        ],
    ),
    "tests/conftest.py": FileDoc(
        layer="test support",
        badge="Shared fakes",
        overview=(
            "Reusable helpers and fakes for deterministic async tests. Despite the pytest filename, "
            "the suite primarily uses stdlib <code>unittest</code> patterns."
        ),
        decisions=[
            "Prefer these helpers before adding new bespoke fakes.",
            "Timeout tests advance fake clocks or synchronize on events, never sleep for real time.",
            "Captured writes are decoded through protocol helpers where possible.",
        ],
    ),
}

DEFAULT_DECISIONS = [
    "Companion/test material: it documents or exercises the frozen architecture but does not "
    "extend the protocol. Read the per-symbol mechanics and source below for specifics.",
]


def file_doc(rel: str) -> FileDoc:
    if rel in FILE_DOCS:
        return FILE_DOCS[rel]
    if rel.startswith("tests/"):
        return FileDoc(
            layer="tests",
            badge="Test coverage",
            overview=(
                "Test module. The dropdowns below show every test/helper with its verbatim source "
                "so you can read the exercised behavior directly. Tests are stdlib "
                "<code>unittest</code>, imported as top-level modules and run from the repo root."
            ),
            decisions=DEFAULT_DECISIONS,
        )
    return FileDoc(layer="module", badge="Source", overview="", decisions=DEFAULT_DECISIONS)


# --------------------------------------------------------------------------- #
# Authored, per-symbol "What it does" prose (keyed by (relpath, qualname)).
# Where an entry exists it replaces the synthesized narrative; everything not
# listed here still gets an accurate AST-synthesized description plus its
# docstring and source. Add entries as understanding deepens.
# --------------------------------------------------------------------------- #

def _p(*paragraphs: str) -> str:
    return "".join(f"<p>{para}</p>" for para in paragraphs)


SYMBOL_DOCS: dict[tuple[str, str], str] = {
    # ---- protocol.py -----------------------------------------------------
    ("protocol.py", "MessageType"): _p(
        "The closed set of v1 wire message <code>type</code> values. Every newline-JSON frame on every "
        "hop (client&harr;controller and controller&harr;board) declares one of these. It is a "
        "<code>StrEnum</code> so the member doubles as the literal JSON string, which is what "
        "<code>validate_message</code> matches on.",
    ),
    ("protocol.py", "TerminalStatus"): _p(
        "The <strong>only three</strong> terminal command outcomes: <code>ok</code>, <code>error</code>, "
        "<code>timeout</code> (contract 3.12). This set is frozen &mdash; new failure modes become "
        "<code>ErrorCode</code> values carried inside an <code>error</code> response, never new statuses. "
        "<code>check_invariants.py</code> statically enforces that nothing widens this enum.",
    ),
    ("protocol.py", "ErrorCode"): _p(
        "The closed vocabulary of error <em>codes</em> that ride inside <code>error</code>/<code>timeout</code> "
        "responses (contract 3.11). Rejections and disconnects are expressed here "
        "(<code>BOARD_BUSY</code>, <code>BOARD_UNAVAILABLE</code>, <code>ESTOP_ACTIVE</code>, "
        "<code>CONTROLLER_SHUTDOWN</code>, &hellip;) rather than as new statuses.",
    ),
    ("protocol.py", "ErrorObject"): _p(
        "A frozen <code>{code, message}</code> value object &mdash; the canonical shape of the "
        "<code>error</code> field in a non-ok response. Being frozen makes it safe to share across the "
        "pending table and observability without defensive copies.",
    ),
    ("protocol.py", "ProtocolValidationError"): _p(
        "The one exception every validator raises. Crucially it <em>carries a structured "
        "<code>ErrorObject</code></em> (<code>self.error</code>), so a caller can turn a malformed message "
        "straight into a wire error response instead of crashing the event loop &mdash; the "
        "&ldquo;structured-error-not-crash&rdquo; rule for the whole stack.",
    ),
    ("protocol.py", "ParseResult"): _p(
        "The result of <code>parse_message</code>: exactly one of <code>message</code> or <code>error</code> "
        "is set. This lets receive loops branch on <code>.ok</code> without try/except at every call site.",
    ),
    ("protocol.py", "ParseResult.ok"): _p(
        "<code>True</code> when parsing/validation succeeded (i.e. <code>error is None</code>). Read by board "
        "and local-socket receive loops to decide whether to dispatch the message or emit the error.",
    ),
    ("protocol.py", "parse_line"): _p(
        "The first gate on every inbound frame. Enforces the receiver&rsquo;s byte limit "
        "(<code>8&nbsp;KiB</code> controller-side, <code>1&nbsp;KiB</code> board-side, contract 1.9), requires "
        "the trailing newline, decodes UTF-8, and insists the payload is a JSON <em>object</em>. Any failure is "
        "a structured <code>INVALID_JSON</code>/<code>INVALID_TYPE</code>, never an uncaught exception.",
    ),
    ("protocol.py", "parse_message"): _p(
        "Convenience wrapper that runs <code>parse_line</code> then <code>validate_message</code> and folds any "
        "<code>ProtocolValidationError</code> into a <code>ParseResult</code>. This is the function receive loops "
        "actually call.",
    ),
    ("protocol.py", "serialize_message"): _p(
        "The single egress encoder: validates the message, emits <strong>compact</strong> JSON "
        "(<code>separators=(&quot;,&quot;,&quot;:&quot;)</code>) plus a newline, and optionally enforces the "
        "receiver&rsquo;s max line size. Passing <code>max_line_bytes</code> is how the controller guarantees a "
        "board never receives an over-1&nbsp;KiB command frame.",
    ),
    ("protocol.py", "validate_message"): _p(
        "The schema router for inbound messages. It reads <code>type</code>, maps it to a "
        "<code>MessageType</code>, and dispatches to the matching <code>_validate_*</code> checker. This is the "
        "choke point that keeps malformed structure out of the controller&rsquo;s logic.",
    ),
    ("protocol.py", "command_blocked_by_estop"): _p(
        "Implements the <strong>fail-safe e-stop gate</strong> (contract 1.17): a schema command entry is "
        "blocked while e-stop is latched <em>unless</em> it explicitly sets <code>blocked_by_estop: false</code>. "
        "Absent means blocked. A non-boolean value is rejected rather than silently coerced.",
    ),
    ("protocol.py", "extract_blocked_by_estop"): _p(
        "Pre-computes the <code>{command_name: blocked}</code> map from a board&rsquo;s schema so the controller "
        "can gate commands in O(1) at dispatch time. Run once per registration/re-registration.",
    ),
    ("protocol.py", "check_protocol_version"): _p(
        "On board registration, confirms the schema&rsquo;s <code>protocol_version</code> matches the "
        "controller&rsquo;s <code>PROTOCOL_VERSION</code>; a mismatch raises "
        "<code>PROTOCOL_VERSION_MISMATCH</code> and the connection is refused (contract 1.3).",
    ),
    ("protocol.py", "build_board_command"): _p(
        "Rewrites a validated <em>client</em> command onto the <em>board</em> hop. The pivotal move is swapping "
        "the client&rsquo;s <code>seq</code> for the controller-owned <code>board_seq</code> and stamping a "
        "monotonic <code>controller_ts</code> &mdash; preserving the two distinct sequence spaces (contract "
        "1.1/3.1) and the round-trip latency token.",
    ),
    ("protocol.py", "build_client_response"): _p(
        "The inverse hop: takes a board response and restores the original client <code>seq</code> and "
        "<code>target</code>, while surfacing the controller-owned <code>board_seq</code> (and optional "
        "<code>latency_ms</code>) inside <code>result</code>. Only <code>ok</code> responses carry a result "
        "object; errors keep <code>result: null</code>.",
    ),
    ("protocol.py", "build_error_response"): _p(
        "Builds a controller-originated error/timeout response. It enforces the status&harr;code coupling: "
        "<code>ok</code> can never be an error, and <code>COMMAND_TIMEOUT</code> is pinned to the "
        "<code>timeout</code> status (and vice-versa). This is how rejections stay codes, not statuses.",
    ),
    ("protocol.py", "build_estop_message"): _p(
        "Constructs the out-of-band <code>estop</code> frame for a board. Deliberately tiny and "
        "seq-less &mdash; e-stop bypasses the command FIFO and in-flight slot (but not the writer lock; see "
        "<code>interfaces.send_estop</code>).",
    ),
    ("protocol.py", "is_estop_ack_event"): _p(
        "Recognizes the board&rsquo;s unsolicited <code>event: estop_ack</code> with "
        "<code>details.state == &quot;safe&quot;</code>. The board connection uses this to flip per-board "
        "<code>estop_ack</code> &mdash; convergence evidence only, never a gate on the global latch.",
    ),
    ("protocol.py", "pop_pending"): _p(
        "The single enforcement point for the <strong>pop-wins</strong> rule (contract 1.8). Whoever pops a "
        "<code>board_seq</code> first owns that command; a late board response after timeout, a duplicate, or a "
        "reply after disconnect all pop <code>None</code> and must be dropped and logged. Because it is a plain "
        "<code>dict.pop</code> with no <code>await</code> inside, check-and-remove is atomic against other "
        "coroutines &mdash; which is why the docstring forbids awaiting between selecting a seq and calling it.",
    ),
    ("protocol.py", "_validate_uint64"): _p(
        "The numeric guard behind every <code>seq</code>/<code>board_seq</code>. Rejects non-ints, "
        "<code>bool</code> (which is an <code>int</code> subclass in Python), negatives, and anything beyond "
        "the uint64 range &mdash; keeping sequence numbers wire-faithful.",
    ),
    # ---- interfaces.py ---------------------------------------------------
    ("interfaces.py", "ClientReplyHandle"): _p(
        "A structural <code>Protocol</code> (injection seam) for delivering a <code>response</code> or "
        "unsolicited <code>event</code> to one local client, plus an <code>is_connected</code> probe. Centralizing "
        "this is what lets the dispatcher count <em>orphaned</em> responses instead of writing to dead sockets.",
    ),
    ("interfaces.py", "BoardDownHandler"): _p(
        "The seam the board-connection manager calls when a board drops. Its single "
        "<code>board_down(board_id)</code> hands ownership back to the dispatcher so it can fail the in-flight "
        "command and drain the FIFO with <code>BOARD_UNAVAILABLE</code> (contract 1.11).",
    ),
    ("interfaces.py", "BoardWriterHandle"): _p(
        "The serialized write boundary for one board, exposing the shared <code>lock</code> and "
        "<code>write_message</code>. Both normal commands and out-of-band e-stop go through this same handle so "
        "no two coroutines ever interleave bytes on one board stream (contract 1.19).",
    ),
    ("interfaces.py", "SerializedBoardWriter"): _p(
        "The concrete single-writer for one board. It owns the per-board <code>asyncio.Lock</code> and the "
        "newline-JSON serialization contract, delegating the raw bytes to an injected <code>write_bytes</code> "
        "callback &mdash; keeping this class free of any socket dependency.",
    ),
    ("interfaces.py", "SerializedBoardWriter.write_message"): _p(
        "Serializes the message to bounded newline-JSON <em>first</em>, then takes the lock only around the "
        "actual <code>write_bytes</code>. Holding the lock across the write is the guarantee that command and "
        "e-stop frames never tear into each other on the wire.",
    ),
    ("interfaces.py", "send_estop"): _p(
        "The out-of-band e-stop path. It skips the FIFO and the one-in-flight slot for latency, but still writes "
        "<em>through the same <code>BoardWriterHandle</code></em>, so it acquires the per-board writer lock like "
        "any other write (contract 1.19). This is the &ldquo;bypass queue, not serialization&rdquo; rule in code.",
    ),
    ("interfaces.py", "deliver_client_response"): _p(
        "Sends a response only if the client is still connected; otherwise it bumps an optional "
        "<code>orphaned_counter</code> and returns <code>False</code>. This is where a response whose client "
        "vanished mid-flight is accounted for instead of raising.",
    ),
    # ---- state.py --------------------------------------------------------
    ("state.py", "BoardConnState"): _p(
        "The <strong>connection</strong> axis: <code>DISCONNECTED &rarr; CONNECTING &rarr; CONNECTED &rarr; "
        "REGISTERED</code>, plus <code>FAULTED</code>. This axis is kept strictly orthogonal to safety "
        "(<code>system.estop_active</code>) &mdash; the two must never be merged into one field (contract 2). "
        "Only <code>REGISTERED</code> boards accept commands.",
    ),
    ("state.py", "SystemState"): _p(
        "Global controller state holding the <strong>single e-stop latch</strong> "
        "(<code>estop_active</code>) and a derived <code>connected_count</code>. The latch is the safety axis; "
        "it is set on any e-stop and cleared <em>only</em> by an operator reset.",
    ),
    ("state.py", "SystemState.latch_estop"): _p(
        "Sets the global <code>estop_active</code> latch. Called when an e-stop is triggered; it never "
        "auto-clears and is never gated on per-board acks (contract 1.13/2.2).",
    ),
    ("state.py", "SystemState.operator_reset_estop"): _p(
        "The <em>only</em> way <code>estop_active</code> goes back to <code>False</code> &mdash; an explicit "
        "operator <code>estop_reset</code>. Convergence acks from boards do not clear the latch.",
    ),
    ("state.py", "BoardState"): _p(
        "The controller&rsquo;s in-memory, single-source-of-truth record for one board: connection state, "
        "per-board <code>estop_ack</code>, the in-flight <code>board_seq</code> and queue depth, the cached "
        "<code>schema</code>, latest telemetry, and the latency/telemetry-rate observers. Redis only ever "
        "<em>mirrors</em> this; it is never authoritative.",
    ),
    ("state.py", "BoardState.mark_estop_ack"): _p(
        "Flips per-board <code>estop_ack</code> true when the board&rsquo;s unsolicited <code>estop_ack</code> "
        "event arrives &mdash; convergence evidence for observability. It does <em>not</em> touch the global "
        "latch.",
    ),
    ("state.py", "PendingCommand"): _p(
        "One in-flight or queued command: the <code>board_seq</code>&harr;<code>client_seq</code> mapping, the "
        "awaiting <code>future</code>, the owning client, and the two independent clocks &mdash; a queue "
        "<em>residency</em> cap and an <em>execution</em> timeout. Entries live in the per-board pending table "
        "that <code>pop_pending</code> resolves.",
    ),
    ("state.py", "PendingCommand.__post_init__"): _p(
        "Guards the <strong>10&nbsp;s hard ceiling</strong> (contract 1.5): constructing a command with an "
        "<code>execution_timeout_s</code> above <code>MAX_COMMAND_TIMEOUT_S</code> raises immediately, so no "
        "command can ever be configured to outlive the ceiling.",
    ),
    ("state.py", "PendingCommand.queue_residency_expired"): _p(
        "True once a still-queued command has waited past its residency cap. The execution clock is separate and "
        "only starts at board-write, so a command that never reaches the wire still fails in bounded time.",
    ),
    ("state.py", "BoardStateRecord"): _p(
        "The frozen <strong>Redis mirror</strong> schema for <code>board:state:&lt;id&gt;</code>. It flattens "
        "live <code>BoardState</code> (including latency percentiles and telemetry rate) into a hashable record. "
        "Its existence is the read-replica boundary: observability serializes <em>this</em>, never the live "
        "object, and Redis is never read back as truth.",
    ),
    ("state.py", "BoardStateRecord.from_board_state"): _p(
        "Snapshots a live <code>BoardState</code> into the immutable mirror record, computing p50/p95/p99 latency "
        "and current telemetry rate at capture time.",
    ),
    ("state.py", "LatencyPercentileObservation.percentile"): _p(
        "Returns the nearest-rank percentile over the bounded sample window (or <code>None</code> when empty). "
        "Backs the p50/p95/p99 latency metrics surfaced to Redis and <code>get_schemas</code>-adjacent state.",
    ),
    ("state.py", "TelemetryRateObservation.observe"): _p(
        "Folds one telemetry arrival timestamp into rolling rate (<code>rate_hz</code>) and "
        "<code>jitter_ms</code>. Because telemetry is a ~50&nbsp;ms one-way push, this rate is also what "
        "liveness/FAULT detection keys off.",
    ),
    ("state.py", "BoardSeqCounter"): _p(
        "The per-board monotonic generator for controller-owned <code>board_seq</code> values. Each board has its "
        "own counter, keeping board sequence spaces independent of each other and of client <code>seq</code>.",
    ),
    ("state.py", "BoardSeqCounter.next"): _p(
        "Hands out the next <code>board_seq</code> and raises <code>OverflowError</code> if the uint64 range is "
        "ever exhausted rather than wrapping (which would alias a live pending entry).",
    ),
    ("state.py", "ControllerState"): _p(
        "The root in-memory state: the global <code>SystemState</code> plus the <code>{board_id: BoardState}</code> "
        "map. This object <em>is</em> the single authority for board state that the whole stack reads and writes.",
    ),
    ("state.py", "ControllerState.connected_count"): _p(
        "Counts boards in <code>REGISTERED</code> &mdash; the only state in which a board is actually command-"
        "ready &mdash; rather than merely TCP-connected.",
    ),
}


# --------------------------------------------------------------------------- #
# Cross-reference index ("Used by"): name-based usage sites across the repo.
# --------------------------------------------------------------------------- #


@dataclass
class Span:
    start: int
    end: int
    anchor: str
    label: str


def _span_label(member: Member) -> str:
    if member.kind in {"imports", "module-code"}:
        return "module scope"
    return member.qualname or member.name or member.title


def flatten_spans(members: list[Member]) -> list[Span]:
    spans: list[Span] = []
    for member in members:
        spans.append(Span(member.lineno, member.end_lineno, member.anchor, _span_label(member)))
        for child in member.children:
            spans.append(Span(child.lineno, child.end_lineno, child.anchor, _span_label(child)))
    return spans


def collect_references(rel: str, tree: ast.Module) -> list[tuple[str, str, int]]:
    """Every name used in a load context, as (name, file, line)."""
    refs: list[tuple[str, str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            refs.append((node.id, rel, node.lineno))
        elif isinstance(node, ast.Attribute) and isinstance(node.ctx, ast.Load):
            refs.append((node.attr, rel, node.lineno))
    return refs


def resolve_scope(spans: list[Span], line: int) -> Span | None:
    """Innermost (smallest) span containing the line."""
    best: Span | None = None
    for span in spans:
        if span.start <= line <= span.end:
            if best is None or (span.end - span.start) < (best.end - best.start):
                best = span
    return best


def build_used_by_html(
    member: Member,
    refs_by_name: dict[str, list[tuple[str, int]]],
    spans_by_file: dict[str, list[Span]],
) -> str:
    if not member.name:
        return ""
    scopes: dict[tuple[str, str], list] = {}  # (rel, anchor) -> [label, count, first_line]
    for rel, line in refs_by_name.get(member.name, []):
        if rel == member.file and member.lineno <= line <= member.end_lineno:
            continue  # skip the definition and its own body (recursion, fields)
        span = resolve_scope(spans_by_file.get(rel, []), line)
        anchor = span.anchor if span else ""
        label = span.label if span else "module scope"
        if anchor == member.anchor:
            continue
        key = (rel, anchor)
        if key not in scopes:
            scopes[key] = [label, 0, line]
        scopes[key][1] += 1

    if not scopes:
        return (
            '<p class="muted">No internal references found &mdash; likely an entry point, an '
            "externally-called API, or invoked dynamically.</p>"
        )

    ordered = sorted(
        scopes.items(),
        key=lambda kv: (GROUP_ORDER.index(group_of(kv[0][0])), order_key(kv[0][0]), kv[1][2]),
    )
    cap = 40
    items: list[str] = []
    for (rel, anchor), (label, count, _line) in ordered[:cap]:
        badge = f' <span class="xcount">&times;{count}</span>' if count > 1 else ""
        loc = f"<code>{html.escape(rel)}</code> &middot; {html.escape(label)}"
        if anchor:
            items.append(f'<li><a class="xref" href="#{anchor}">{loc}</a>{badge}</li>')
        else:
            items.append(f"<li>{loc}{badge}</li>")
    more = len(ordered) - cap
    if more > 0:
        items.append(f'<li class="muted">+{more} more reference site(s)</li>')
    note = (
        '<p class="muted xnote">Name-based references across the repo (a same-named symbol '
        "elsewhere may appear). Click to jump to the using code.</p>"
    )
    return f'<ul class="xref-list">{"".join(items)}</ul>{note}'


def assign_used_by(
    members: list[Member],
    refs_by_name: dict[str, list[tuple[str, int]]],
    spans_by_file: dict[str, list[Span]],
) -> None:
    for member in members:
        member.used_by_html = build_used_by_html(member, refs_by_name, spans_by_file)
        for child in member.children:
            child.used_by_html = build_used_by_html(child, refs_by_name, spans_by_file)


# --------------------------------------------------------------------------- #
# HTML rendering
# --------------------------------------------------------------------------- #

CSS_STATIC = """
    :root {
      color-scheme: light;
      --bg: #f8fafc; --ink: #111827; --muted: #4b5563; --line: #d1d5db;
      --panel: #ffffff; --accent: #075985; --accent-soft: #e0f2fe; --code: #f3f4f6;
    }
    * { box-sizing: border-box; }
    body { margin: 0; background: var(--bg); color: var(--ink);
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      line-height: 1.5; }
    main { width: min(1240px, calc(100% - 32px)); margin: 0 auto; padding: 32px 0 64px; }
    h1, h2, h3 { line-height: 1.2; margin: 0 0 12px; }
    h1 { font-size: 2rem; } h2 { font-size: 1.35rem; margin-top: 24px; }
    h3 { font-size: 1.05rem; margin-top: 20px; }
    p { margin: 0 0 12px; } ul { margin: 0 0 16px 1.2rem; padding: 0; } li { margin: 4px 0; }
    code { background: var(--code); border-radius: 4px; padding: 1px 4px;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 0.92em; }
    a { color: var(--accent); }
    .authority, .orientation, .tab-panel { background: var(--panel); border: 1px solid var(--line);
      border-radius: 8px; padding: 18px; }
    .authority { margin: 18px 0; border-left: 4px solid var(--accent); }
    .orientation { margin-bottom: 20px; }
    .tabs { display: grid; grid-template-columns: 280px minmax(0, 1fr); gap: 16px; align-items: start; }
    .tab-input { position: absolute; opacity: 0; pointer-events: none; }
    .tab-list { position: sticky; top: 12px; display: grid; gap: 4px;
      max-height: calc(100vh - 24px); overflow: auto; padding: 8px; border: 1px solid var(--line);
      border-radius: 8px; background: var(--panel); }
    .tab-list h2 { margin: 10px 8px 4px; font-size: 0.78rem; text-transform: uppercase;
      letter-spacing: 0.08em; color: var(--muted); }
    .tab-list label { display: block; padding: 7px 10px; border-radius: 6px; cursor: pointer;
      color: var(--muted); font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 0.84rem; }
    .tab-list label:hover { background: var(--code); color: var(--ink); }
    #file-filter { margin: 2px 4px 8px; padding: 7px 9px; border: 1px solid var(--line);
      border-radius: 6px; font-size: 0.84rem; width: calc(100% - 8px); }
    .tab-panels { min-width: 0; }
    .tab-panel { display: none; }
    .meta { display: flex; flex-wrap: wrap; gap: 8px; margin: 0 0 16px; }
    .meta span { border: 1px solid var(--line); border-radius: 999px; padding: 3px 9px;
      color: var(--muted); font-size: 0.84rem; background: #fbfdff; }
    .decisions { background: #f8fbff; border: 1px solid var(--line); border-left: 4px solid var(--accent);
      border-radius: 8px; padding: 12px 16px; margin: 0 0 18px; }
    .decisions h3 { margin-top: 0; }
    .panel-tools { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; margin: 6px 0 14px; }
    .panel-tools input[type=search] { flex: 1 1 220px; min-width: 160px; padding: 7px 9px;
      border: 1px solid var(--line); border-radius: 6px; font-size: 0.88rem; }
    .panel-tools button { padding: 6px 12px; border: 1px solid var(--line); border-radius: 6px;
      background: var(--panel); cursor: pointer; font-size: 0.84rem; color: var(--accent); }
    .panel-tools button:hover { background: var(--accent-soft); }
    details.member { border: 1px solid var(--line); border-radius: 8px; margin: 8px 0;
      background: var(--panel); overflow: hidden; }
    details.member > summary { cursor: pointer; padding: 10px 14px; list-style: none;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 0.86rem;
      display: flex; gap: 10px; align-items: baseline; }
    details.member > summary::-webkit-details-marker { display: none; }
    details.member > summary::before { content: "\\25B8"; color: var(--accent); font-size: 0.8em; }
    details.member[open] > summary::before { content: "\\25BE"; }
    summary .lines { margin-left: auto; color: var(--muted); font-size: 0.78rem; flex: 0 0 auto; }
    .member.kind-class > summary { background: var(--accent-soft); color: var(--accent); }
    .member.kind-imports > summary, .member.kind-module-code > summary { color: var(--muted); }
    .member-body { padding: 0 14px 14px; border-top: 1px solid var(--line); }
    .member.kind-class > .member-body { background: #fbfeff; }
    .kind { font-size: 0.74rem; text-transform: uppercase; letter-spacing: 0.06em;
      color: var(--accent); margin: 12px 0 8px; font-weight: 700; }
    .doc { white-space: normal; }
    h4.sub { margin: 14px 0 6px; font-size: 0.72rem; text-transform: uppercase;
      letter-spacing: 0.07em; color: var(--muted); border-top: 1px dashed var(--line);
      padding-top: 10px; }
    .member-body > p { margin: 0 0 8px; }
    .muted { color: var(--muted); font-size: 0.88rem; }
    ul.xref-list { list-style: none; margin: 4px 0 0; padding: 0;
      display: grid; gap: 3px; }
    ul.xref-list li { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 0.82rem; }
    .xcount { color: var(--muted); }
    .xnote { margin-top: 6px; font-size: 0.78rem; }
    dl.mech { display: grid; grid-template-columns: max-content 1fr; gap: 4px 14px; margin: 0 0 12px; }
    dl.mech dt { color: var(--muted); font-size: 0.82rem; }
    dl.mech dd { margin: 0; min-width: 0; }
    dl.mech dd code { background: transparent; padding: 0; word-break: break-word; }
    pre.src { background: #0f172a; color: #e2e8f0; border-radius: 8px; padding: 14px 16px;
      overflow: auto; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 0.82rem; line-height: 1.55; margin: 8px 0 0; }
    pre.src code { background: transparent; color: inherit; padding: 0; font-size: inherit; }
    .nested { margin: 10px 0 0; padding: 0 0 0 12px; border-left: 2px solid var(--accent-soft); }
    .nested h4 { margin: 6px 0; font-size: 0.85rem; color: var(--muted); }
    @media (max-width: 900px) {
      main { width: min(100% - 20px, 1240px); padding-top: 20px; }
      .tabs { grid-template-columns: 1fr; }
      .tab-list { position: static; max-height: none; }
    }
"""

JS = """
(function () {
  function activePanel() {
    var checked = document.querySelector('.tab-input:checked');
    if (!checked) return null;
    var id = checked.id.replace(/^tab-/, '');
    return document.querySelector('.tab-panel[data-slug="' + id + '"]');
  }
  document.addEventListener('input', function (e) {
    if (e.target.id === 'file-filter') {
      var q = e.target.value.trim().toLowerCase();
      document.querySelectorAll('.tab-list label[for^="tab-"]').forEach(function (label) {
        label.style.display = label.textContent.toLowerCase().indexOf(q) === -1 ? 'none' : '';
      });
      return;
    }
    if (e.target.classList.contains('member-filter')) {
      var panel = e.target.closest('.tab-panel');
      var term = e.target.value.trim().toLowerCase();
      panel.querySelectorAll('details.member').forEach(function (d) {
        var hay = d.querySelector('summary').textContent.toLowerCase()
          + ' ' + d.querySelector('.member-body').textContent.toLowerCase();
        d.style.display = (!term || hay.indexOf(term) !== -1) ? '' : 'none';
      });
    }
  });
  document.addEventListener('click', function (e) {
    if (e.target.dataset && e.target.dataset.action) {
      var panel = e.target.closest('.tab-panel') || activePanel();
      if (!panel) return;
      var open = e.target.dataset.action === 'expand';
      panel.querySelectorAll('details.member').forEach(function (d) {
        if (d.style.display !== 'none') d.open = open;
      });
      return;
    }
    var link = e.target.closest('a.xref');
    if (link) {
      e.preventDefault();
      var id = link.getAttribute('href').slice(1);
      activateAnchor(id);
      history.replaceState(null, '', '#' + id);
    }
  });
  function activateAnchor(id) {
    var el = document.getElementById(id);
    if (!el) return;
    var panel = el.closest('.tab-panel');
    if (panel) {
      var radio = document.getElementById('tab-' + panel.getAttribute('data-slug'));
      if (radio) radio.checked = true;
    }
    var node = el;
    while (node) {
      if (node.tagName === 'DETAILS') node.open = true;
      node = node.parentElement;
    }
    el.scrollIntoView({ block: 'start' });
  }
  window.addEventListener('load', function () {
    if (location.hash.length > 1) activateAnchor(location.hash.slice(1));
  });
})();
"""


def render_source(source: str) -> str:
    return f'<pre class="src"><code>{html.escape(source)}</code></pre>'


def _section(title: str, inner: str) -> str:
    return f'<h4 class="sub">{title}</h4>{inner}'


def render_member(member: Member) -> str:
    body = [member.kind_html]
    body.append(_section("What it does", member.narrative_html))
    if member.mech_html:
        body.append(_section("Mechanics", member.mech_html))
    if member.used_by_html:
        body.append(_section("Used by", member.used_by_html))
    if member.children:
        body.append(_section("Source — class definition &amp; fields", render_source(member.source)))
        body.append('<div class="nested"><h4>Methods</h4>')
        for child in member.children:
            body.append(render_member(child))
        body.append("</div>")
    else:
        body.append(_section("Source", render_source(member.source)))
    line_label = (
        f"line {member.lineno}"
        if member.lineno == member.end_lineno
        else f"lines {member.lineno}–{member.end_lineno}"
    )
    return (
        f'<details class="member kind-{member.kind}" id="{member.anchor}">'
        f'<summary><span class="title">{html.escape(member.title)}</span>'
        f'<span class="lines">{line_label}</span></summary>'
        f'<div class="member-body">{"".join(body)}</div>'
        f"</details>"
    )


def render_panel(rel: str, members: list[Member], total_lines: int, mod_doc: str | None, checked: bool) -> str:
    doc = file_doc(rel)
    member_count = sum(1 + len(m.children) for m in members)
    meta = [
        f"<span>Layer: {html.escape(doc.layer)}</span>",
        f"<span>{total_lines} lines</span>",
        f"<span>{member_count} documented members</span>",
    ]
    if doc.badge:
        meta.append(f"<span>{html.escape(doc.badge)}</span>")

    decisions = "".join(f"<li>{d}</li>" for d in doc.decisions)
    overview = f"<p>{doc.overview}</p>" if doc.overview else ""
    if mod_doc:
        overview += f'<p class="doc"><em>Module docstring:</em> {html.escape(mod_doc).replace(chr(10), "<br>")}</p>'

    members_html = "".join(render_member(m) for m in members)
    return (
        f'<article class="tab-panel" data-python-file="{html.escape(rel)}" data-slug="{slug(rel)[4:]}">'
        f"<h2>{html.escape(rel)}</h2>"
        f'<p class="meta">{"".join(meta)}</p>'
        f"{overview}"
        f'<div class="decisions"><h3>Architectural decisions</h3><ul>{decisions}</ul></div>'
        f'<div class="panel-tools">'
        f'<input type="search" class="member-filter" placeholder="Filter members in this file…">'
        f'<button type="button" data-action="expand">Expand all</button>'
        f'<button type="button" data-action="collapse">Collapse all</button>'
        f"</div>"
        f'<div class="members">{members_html}</div>'
        f"</article>"
    )


def render(files: list[str]) -> str:
    # Stable group/order so regeneration is deterministic.
    ordered = sorted(files, key=lambda r: (GROUP_ORDER.index(group_of(r)), order_key(r)))

    # Pass 1: parse every file, extract members, and build the global xref index.
    file_members: dict[str, list[Member]] = {}
    file_lines: dict[str, int] = {}
    file_mod_doc: dict[str, str | None] = {}
    refs_by_name: dict[str, list[tuple[str, int]]] = {}
    spans_by_file: dict[str, list[Span]] = {}

    for rel in ordered:
        source = (REPO_ROOT / rel).read_text(encoding="utf-8")
        tree = ast.parse(source)
        members, total_lines = extract_members(rel, source)
        file_members[rel] = members
        file_lines[rel] = total_lines
        file_mod_doc[rel] = ast.get_docstring(tree)
        spans_by_file[rel] = flatten_spans(members)
        for name, ref_rel, line in collect_references(rel, tree):
            refs_by_name.setdefault(name, []).append((ref_rel, line))

    # Pass 2: now that every name is indexed, compute each member's "Used by".
    for rel in ordered:
        assign_used_by(file_members[rel], refs_by_name, spans_by_file)

    label_css: list[str] = []
    panel_css: list[str] = []
    inputs: list[str] = []
    nav_by_group: dict[str, list[str]] = {g: [] for g in GROUP_ORDER}
    panels: list[str] = []

    for index, rel in enumerate(ordered):
        sid = slug(rel)
        checked = " checked" if index == 0 else ""
        inputs.append(
            f'<input class="tab-input" type="radio" name="python-file" id="{sid}"{checked}>'
        )
        label_css.append(f'#{sid}:checked ~ .tabs label[for="{sid}"]')
        panel_css.append(f'#{sid}:checked ~ .tabs [data-python-file="{rel}"]')
        nav_by_group[group_of(rel)].append(
            f'<label for="{sid}">{html.escape(rel)}</label>'
        )
        panels.append(
            render_panel(rel, file_members[rel], file_lines[rel], file_mod_doc[rel], index == 0)
        )

    nav_sections = []
    for group in GROUP_ORDER:
        if nav_by_group[group]:
            nav_sections.append(f"<h2>{group}</h2>" + "".join(nav_by_group[group]))
    nav_html = (
        '<input type="text" id="file-filter" placeholder="Filter files…" aria-label="Filter files">'
        + "".join(nav_sections)
    )

    css = (
        CSS_STATIC
        + "\n    "
        + ",\n    ".join(label_css)
        + " {\n      background: var(--accent-soft); color: var(--accent); font-weight: 700;\n    }\n    "
        + ",\n    ".join(panel_css)
        + " {\n      display: block;\n    }\n"
    )

    statuses = "".join(f"<code>{s}</code> " for s in ("ok", "error", "timeout"))

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Hyperloop Python Codebase Reference</title>
  <style>{css}  </style>
</head>
<body>
<main>
  <h1>Hyperloop Python Codebase Reference</h1>
  <p>One tab per tracked Python source file. Every class, method, function, and top-level block
  is a dropdown showing a structured explanation and its <strong>verbatim source</strong>. This is
  companion material, not a protocol extension &mdash; it is generated by
  <code>tools/gen_codebase_reference.py</code>; regenerate it after code changes.</p>

  <section class="authority">
    <h2>Authority</h2>
    <p>The frozen controller/networking contract
    <a href="../contracts/V1_Networking_Decisions.md">docs/contracts/V1_Networking_Decisions.md</a>
    remains authoritative, and
    <a href="../contracts/Board_Developer_Guide.md">docs/contracts/Board_Developer_Guide.md</a>
    governs firmware-facing behavior. If this companion reference disagrees with either, the
    frozen contracts win.</p>
  </section>

  <section class="orientation">
    <h2>How To Study The Stack</h2>
    <ul>
      <li>Start with <code>protocol.py</code>, <code>state.py</code>, and <code>interfaces.py</code>;
      these are the shared contract primitives.</li>
      <li>Read <code>controller.py</code> before the transport files. It owns dispatch, pending
      resolution, e-stop gating, and shutdown behavior.</li>
      <li>Read <code>board_connection.py</code> and <code>local_socket.py</code> as adapters around
      the controller core: TCP boards on one side, full-duplex local Unix clients on the other.</li>
      <li>Keep the topology in view:
      <code>local client -&gt; Unix socket -&gt; asyncio controller -&gt; persistent TCP -&gt; board</code>.
      Redis remains observability/read-replica only.</li>
      <li>Every command resolves to exactly one terminal status: {statuses}. New failure modes are
      error <em>codes</em>, never new statuses.</li>
    </ul>
  </section>

  {"".join(inputs)}

  <div class="tabs">
    <nav class="tab-list" aria-label="Python files">
      {nav_html}
    </nav>
    <div class="tab-panels">
      {"".join(panels)}
    </div>
  </div>
</main>
<script>{JS}</script>
</body>
</html>
"""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Fail if the output is stale.")
    args = parser.parse_args(argv)

    files = discover_python_files()
    output = render(files)

    if args.check:
        current = OUTPUT.read_text(encoding="utf-8") if OUTPUT.exists() else ""
        if current != output:
            print("Codebase_Reference.html is stale; run: python tools/gen_codebase_reference.py", file=sys.stderr)
            return 1
        print("Codebase_Reference.html is up to date.")
        return 0

    OUTPUT.write_text(output, encoding="utf-8")
    print(f"Wrote {OUTPUT.relative_to(REPO_ROOT)} ({len(output)} bytes, {len(files)} files).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
