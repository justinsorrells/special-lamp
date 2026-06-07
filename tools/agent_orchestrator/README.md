# Hyperloop Board Networking Stack - Agent Orchestrator

The **Agent Orchestrator** is a repo-local utility designed to automate the development, review, auditing, and commit cycles for the Hyperloop board networking stack. It enforces strict compliance with the project's frozen contracts and invariants by organizing agents into a structured pipeline:

```
[Task] ──> Codex (Implementation) ──> Verification Checks ──> Claude CLI (Adversarial Review) ──> Antigravity (Independent Audit) ──> [Auto-Commit or Stop]
```

---

## Desired Agent Workflow

- **Antigravity (Gemini 3.5 Flash)**: Project manager, orchestrator, task sequencer, and final auditor.
- **Codex (GPT-5.5)**: Primary implementation agent that writes production code and tests.
- **Claude CLI (Opus 4.8)**: Adversarial reviewer that verifies the git diff against the invariants.
- **Human**: Exception handler, resolver of ambiguity, and final merge authority.

No human gating is required for successful, passing tasks where all audits pass. If all checks, reviews, and audits pass, the orchestrator commits the changes to an agent branch. However, if Antigravity audit fails, the bounded override policy will route hard-stop and ambiguous cases to human review. The operator is also interrupted when a verification step fails or high-risk modifications are detected.

---

## Setup & Configuration

Copy the example configuration to create your local config file:

```bash
cp tools/agent_orchestrator/config.example.toml tools/agent_orchestrator/config.toml
```

### Configuration Options

Edit `config.toml` to configure model names, CLI paths, verification checks, and limits:

```toml
[agents.codex]
command = "codex"
mode = "exec"
model = "gpt-5.5"

[agents.claude]
command = "claude"
mode = "print"
model = "opus"

[agents.antigravity]
command = "agy"
model = "gemini-3.5-flash"

[repo]
main_branch = "main"
agent_branch_prefix = "agent/"
require_clean_worktree = true
never_auto_push = true
never_auto_merge = true

[checks]
pytest = true
compileall = true
ruff = "auto"    # "auto" runs if installed, true fails if missing/failed, false disables
mypy = "auto"    # "auto" runs if installed, true fails if missing/failed, false disables

[limits]
max_changed_files = 30
max_diff_lines = 2500
max_task_cycles = 1
```

---

## CLI Usage

### 1. Dry Run Mode
Verifies configuration and model connections, and outputs the planned execution commands without invoking Codex or Claude.

```bash
python tools/agent_orchestrator/orchestrate.py --task-file tools/agent_orchestrator/examples/task.md --dry-run
```

### 2. Single-Task Execution
Executes the full autonomous loop for a single task file.

```bash
python tools/agent_orchestrator/orchestrate.py --task-file path/to/task.md
```

### 3. Backlog Execution
Runs in backlog mode, processing the first unchecked task checkbox (`- [ ]`) from a backlog file. If the task is completed successfully, it is marked checked (`- [x]`) and checked back into the file.

```bash
python tools/agent_orchestrator/orchestrate.py --backlog path/to/backlog.md
```

---

## Model Verification Probes

Before starting any task, the orchestrator runs non-interactive version and help probes for each configured command (e.g., `codex --help`, `claude --help`, `antigravity --help`). 

If a command is not found or fails to run, or if the requested model cannot be verified, the execution stops with a `STOP_MODEL_UNVERIFIED` classification to prevent silent model fallback or silent downgrade.

---

## Stop Conditions & Safety Gates

The orchestrator will immediately halt and require manual intervention if:
1. **Tests / Checks fail**: `pytest`, `compileall` (which compiles only modified Python files to prevent slow walks), or the static architecture invariants checker fails. Logs of failed checks are cleanly structured and fed back to Codex.
2. **Review fails**:
   - Claude's verdict is `FAIL` or Antigravity's audit verdict is `FAIL`. Verdicts are matched robustly regardless of Markdown emphasis (e.g. `**Final verdict:** PASS`).
   - Claude Code CLI lists items in the `"Must fix before commit:"` section. Negation expressions like `"None"`, `"n/a"`, or `"no issues"` are ignored to avoid false positives.
3. **Forbidden changes (content scans run on added lines scoped per-file)**:
   - Codex modifies `docs/contracts/` or `AGENTS.md` / `.agents/skills/` without explicit task permission.
   - Codex introduces a terminal status other than `ok`, `error`, or `timeout` (validated via a robust regex covering Python assignments, annotations, and key-values scoped to each file's added lines).
   - Codex attempts to route commands through Redis (by importing `redis`/`aioredis` or calling pubsub/publish/xadd methods inside core controller files, checked using robust file basenames).
   - Codex attempts to establish direct board TCP connections (`open_connection`) outside `board_connection.py` (checked by scanning each file's individual diff chunk).
4. **Security / Risk triggers**:
   - Newly created files are staged with `git add -N .` immediately after Codex executes, ensuring they are subject to all safety scans and review audits.
   - Diff size exceeds configured limits.
   - Hardcoded secrets or tokens are detected in the global added lines.
   - Dependency files (`requirements.txt`, etc.) or CI/deployment files are modified. Temporary backlog task files (`scratch_task.md`) are ignored via `.gitignore` to prevent accidental staging.
   - Git worktree is dirty (unless `--allow-dirty` is used).


---

## Run Artifacts

Every execution cycle saves detailed diagnostics under `.agent_runs/<timestamp>/` (e.g., `.agent_runs/2026-06-06T15-42-11/`):

- `task.md` — Copy of the input task.
- `codex_prompt.md` — Complete prompt constructed for Codex.
- `codex_stdout.txt` / `codex_stderr.txt` — Capture of Codex execution.
- `git_status_before.txt` / `git_status_after.txt` — Short status logs.
- `git_diff_stat.txt` / `git_diff.patch` — Git diff outputs.
- `pytest.txt` / `compileall.txt` / `ruff.txt` / `mypy.txt` / `check_invariants.txt` — Execution logs of test, lint, typecheck, and invariant tools.
- `claude_prompt.md` / `claude_review.md` — Verification details.
- `antigravity_audit.md` — Independent audit evaluation.
- `claude_adjudication_prompt.md` / `claude_adjudication.md` — Adjudication request and response details when override is evaluated.
- `final_report.md` — Summary report including classifications, verdicts, and next steps.

---

## Result Classifications

Every execution ends with one of the following terminal statuses logged in `final_report.md`:

- `AUTO_COMMITTED`
- `DRY_RUN_OK`
- `STOP_BACKLOG_EMPTY`
- `STOP_TESTS_FAILED`
- `STOP_COMPILE_FAILED`
- `STOP_INVARIANTS_FAILED`
- `STOP_LINT_FAILED`
- `STOP_TYPECHECK_FAILED`
- `STOP_CLAUDE_REVIEW_FAILED`
- `STOP_ANTIGRAVITY_AUDIT_FAILED`
- `STOP_HUMAN_REVIEW_REQUIRED`
- `STOP_CONTRACT_CHANGE`
- `STOP_ARCHITECTURE_RISK`
- `STOP_HIGH_RISK_CHANGE`
- `STOP_MODEL_UNVERIFIED`
- `STOP_DIRTY_WORKTREE`
- `STOP_TOOL_ERROR`

---

## Bounded Antigravity Override Policy & Human Gating

While Claude remains the strongest adjudication model, Antigravity functions as an independent audit. If Antigravity fails (verdict `FAIL`), the orchestrator enforces deterministic hard stops. Claude override is **not** a blanket escape hatch:
1. If tests, typecheck, or invariants fail, auto-override is blocked.
2. If command-path files, safety/e-stop path files, sequence number logic (detected via diff-keyword heuristics for "board_seq", "PendingCommand", "pending", "pop_pending", "seq"), or writer serialization logic (detected via diff-keyword heuristics for "SerializedBoardWriter", "writer lock", "send_estop", "write_message", "writer.drain", "writer.write") are modified, the orchestrator halts immediately with `STOP_HUMAN_REVIEW_REQUIRED`.
3. Claude may only adjudicate and override `FAIL` on safe non-critical logic changes under structured adjudication.

## Self-Modification Limitations & Maintenance Mode

Modifications to `tools/agent_orchestrator/*`, `tools/check_invariants.py`, or tests under `tests/` are allowed for orchestrator-maintenance and system-hardening tasks, but they require close reviewer attention because they bypass certain code paths and self-modify the pipeline gates.

When changing the static invariant checker `tools/check_invariants.py`, you must add or run tests verifying that the checker continues to catch representative architecture and design-invariant violations.

