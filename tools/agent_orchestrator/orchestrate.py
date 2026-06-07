#!/usr/bin/env python3
"""
Agent Orchestrator for Hyperloop Board Networking Stack.
Orchestrates: Codex (implementation), Claude CLI (review), Antigravity (audit/coordination).
"""

import os
import sys
import re
import argparse
import subprocess
import shutil
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple

# Fallback TOML parser to maintain python 3.9 compatibility
# Fallback TOML parser to maintain python 3.9 compatibility
def parse_simple_toml(file_path: Path) -> dict:
    config = {}
    current_section = None
    section_name = None
    if not file_path.exists():
        return config
    
    in_array = False
    array_lines = []
    array_key = None
    array_section = None

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
                
            if in_array:
                array_lines.append(line)
                if "]" in line:
                    val_str = " ".join(array_lines)
                    inner = val_str[val_str.find("[")+1:val_str.find("]")].strip()
                    val = []
                    if inner:
                        parts = [p.strip() for p in inner.split(",")]
                        for p in parts:
                            if not p:
                                continue
                            if p.startswith('"') and p.endswith('"'):
                                val.append(p[1:-1])
                            elif p.startswith("'") and p.endswith("'"):
                                val.append(p[1:-1])
                            elif p.lower() == "true":
                                val.append(True)
                            elif p.lower() == "false":
                                val.append(False)
                            elif p.isdigit():
                                val.append(int(p))
                            else:
                                val.append(p)
                    
                    if array_section:
                        sec_parts = array_section.split(".")
                        curr_sec = config
                        for part in sec_parts:
                            curr_sec = curr_sec[part]
                        curr_sec[array_key] = val
                    else:
                        config[array_key] = val
                    in_array = False
                    array_lines = []
                    array_key = None
                    array_section = None
                continue

            section_match = re.match(r"^\[([\w\.\-_]+)\]$", line)
            if section_match:
                section_name = section_match.group(1)
                parts = section_name.split(".")
                current = config
                for part in parts[:-1]:
                    current = current.setdefault(part, {})
                current_section = parts[-1]
                current[current_section] = {}
                continue
            
            kv_match = re.match(r"^([\w\-_]+)\s*=\s*(.*)$", line)
            if kv_match:
                key, val_str = kv_match.group(1), kv_match.group(2).strip()
                if " #" in val_str:
                    val_str = val_str.split(" #")[0].strip()
                
                if val_str.startswith("[") and not val_str.endswith("]"):
                    in_array = True
                    array_lines = [val_str]
                    array_key = key
                    array_section = section_name
                    continue
                
                # Parse value
                if val_str.startswith('[') and val_str.endswith(']'):
                    inner = val_str[1:-1].strip()
                    val = []
                    if inner:
                        parts = [p.strip() for p in inner.split(",")]
                        for p in parts:
                            if not p:
                                continue
                            if p.startswith('"') and p.endswith('"'):
                                val.append(p[1:-1])
                            elif p.startswith("'") and p.endswith("'"):
                                val.append(p[1:-1])
                            elif p.lower() == "true":
                                val.append(True)
                            elif p.lower() == "false":
                                val.append(False)
                            elif p.isdigit():
                                val.append(int(p))
                            else:
                                val.append(p)
                elif val_str.startswith('"') and val_str.endswith('"'):
                    val = val_str[1:-1]
                elif val_str.startswith("'") and val_str.endswith("'"):
                    val = val_str[1:-1]
                elif val_str.lower() == "true":
                    val = True
                elif val_str.lower() == "false":
                    val = False
                elif val_str.isdigit():
                    val = int(val_str)
                else:
                    val = val_str
                
                if current_section:
                    sec_parts = section_name.split(".")
                    curr_sec = config
                    for part in sec_parts:
                        curr_sec = curr_sec[part]
                    curr_sec[key] = val
                else:
                    config[key] = val
    return config

def load_config(config_path: Optional[Path] = None) -> dict:
    if config_path:
        return parse_simple_toml(config_path)
    
    local_toml = Path("tools/agent_orchestrator/config.toml")
    if local_toml.exists():
        return parse_simple_toml(local_toml)
    
    example_toml = Path("tools/agent_orchestrator/config.example.toml")
    if example_toml.exists():
        return parse_simple_toml(example_toml)
    
    # Absolute default fallback
    return {
        "agents": {
            "codex": {"command": "codex", "mode": "exec", "model": "gpt-5.5"},
            "claude": {"command": "claude", "mode": "print", "model": "opus"},
            "antigravity": {"command": "agy", "model": "gemini-3.5-flash"}
        },
        "repo": {
            "main_branch": "main",
            "agent_branch_prefix": "agent/",
            "require_clean_worktree": True,
            "never_auto_push": True,
            "never_auto_merge": True
        },
        "checks": {
            "pytest": True,
            "compileall": True,
            "ruff": "auto",
            "mypy": "auto",
            "invariants": True
        },
        "limits": {
            "max_changed_files": 30,
            "max_diff_lines": 2500,
            "max_task_cycles": 5
        },
        "review": {
            "allow_claude_override_antigravity": True,
            "antigravity_hard_stop_categories": [
                "contract_violation",
                "safety_or_estop_issue",
                "command_path_violation",
                "redis_boundary_violation",
                "seq_board_seq_confusion",
                "writer_serialization_issue",
                "race_condition",
                "data_loss",
                "unbounded_queue_or_blocking_async",
                "test_failure",
                "invariant_failure",
                "frozen_contract_modified",
                "security_or_secret_exposure"
            ]
        }
    }

# Shell runner helper
def run_cmd(args: List[str], input_str: Optional[str] = None, cwd: Optional[Path] = None, env: Optional[Dict[str, str]] = None) -> Tuple[int, str, str]:
    try:
        proc_env = os.environ.copy()
        if env:
            proc_env.update(env)
        # Ensure PYTHONPATH includes the current directory so tests/imports resolve
        if "PYTHONPATH" not in proc_env:
            proc_env["PYTHONPATH"] = str(Path.cwd())
        else:
            proc_env["PYTHONPATH"] = f"{Path.cwd()}:{proc_env['PYTHONPATH']}"

        res = subprocess.run(
            args,
            input=input_str,
            capture_output=True,
            text=True,
            cwd=str(cwd) if cwd else None,
            env=proc_env
        )
        return res.returncode, res.stdout, res.stderr
    except FileNotFoundError as e:
        return 127, "", f"Command not found: {args[0]} ({str(e)})"
    except Exception as e:
        return -1, "", str(e)

# Git utility functions
def git_status_porcelain() -> str:
    _, out, _ = run_cmd(["git", "status", "--porcelain"])
    return out.strip()

def parse_changed_files(status_output: str) -> List[str]:
    files = []
    for line in status_output.splitlines():
        if not line or len(line) < 3:
            continue
        # Status code is in characters 0-1
        path_part = line[2:].strip()
        
        # Handle renames e.g. R  old -> new or RM old -> new
        if " -> " in path_part:
            parts = path_part.split(" -> ")
            path_part = parts[-1].strip()
            
        # Strip outer/wrapping quotes if any
        path_part = path_part.strip('"')
        
        # Decode git octal/escape representation if needed
        if "\\" in path_part:
            try:
                path_part = path_part.encode().decode('unicode-escape')
            except Exception:
                pass
                
        files.append(path_part)
    return files

def parse_must_fix(content: str) -> bool:
    content = content.strip()
    if not content:
        return False
    
    lines = []
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        
        # Remove list bullet prefixes: -, *, +, 1., etc.
        line = re.sub(r"^([-\*\+]\s*|\d+\.\s*)", "", line).strip()
        if not line:
            continue
        
        # Check if the line is just a negation/empty indicator
        lower_line = line.lower().rstrip(".")
        if lower_line in ["none", "n/a", "na", "nothing", "no issues", "no findings", "looks clean", "looks good", "no action needed", "no action required", "nil"]:
            continue
        
        lines.append(line)
    
    return len(lines) > 0


def extract_added_lines(diff: str) -> str:
    added = []
    for line in diff.splitlines():
        if line.startswith("+") and not line.startswith("+++"):
            added.append(line[1:])
    return "\n".join(added)

def parse_per_file_diff(diff: str) -> Dict[str, str]:
    file_diffs = {}
    current_file = None
    current_lines = []
    
    for line in diff.splitlines():
        if line.startswith("diff --git "):
            # Save the previous file's diff
            if current_file and current_lines:
                file_diffs[current_file] = "\n".join(current_lines)
            
            # Extract new file name (b/path)
            # git diff format: diff --git a/path/to/file b/path/to/file
            match = re.search(r" b/(.+)$", line)
            if match:
                path_part = match.group(1).strip()
                path_part = path_part.strip('"')
                current_file = path_part
            else:
                current_file = None
            current_lines = []
        elif current_file is not None:
            current_lines.append(line)
            
    if current_file and current_lines:
        file_diffs[current_file] = "\n".join(current_lines)
        
    return file_diffs


def git_current_branch() -> str:
    _, out, _ = run_cmd(["git", "branch", "--show-current"])
    return out.strip()

def git_create_branch(branch_name: str) -> bool:
    code, _, _ = run_cmd(["git", "checkout", "-b", branch_name])
    return code == 0

def git_switch_branch(branch_name: str) -> bool:
    code, _, _ = run_cmd(["git", "checkout", branch_name])
    return code == 0

def git_commit(message: str) -> Tuple[int, str]:
    # Stage all additions, deletions, and modifications first
    run_cmd(["git", "add", "-A"])
    code, out, err = run_cmd(["git", "commit", "-m", message])
    return code, out + "\n" + err

def git_diff() -> str:
    _, out, _ = run_cmd(["git", "diff"])
    return out

def git_diff_stat() -> str:
    _, out, _ = run_cmd(["git", "diff", "--stat"])
    return out

def slugify(text: str) -> str:
    # Convert spaces/special chars to hyphens
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[-\s]+", "-", text)
    return text.strip("-")

@dataclass
class AgentResult:
    command_used: str
    stdout: str
    stderr: str
    exit_code: int

class Orchestrator:
    def __init__(self, config: dict, dry_run: bool = False, allow_dirty: bool = False):
        self.config = config
        self.dry_run = dry_run
        self.allow_dirty = allow_dirty
        self.run_dir: Optional[Path] = None
        self.models_verified = {}
        self.agent_runs_parent = Path(".agent_runs")
        self.final_artifacts = {}

    def log_artifact(self, filename: str, content: str):
        if self.run_dir:
            self.run_dir.mkdir(parents=True, exist_ok=True)
            (self.run_dir / filename).write_text(content, encoding="utf-8")

    def verify_models(self) -> Tuple[bool, str]:
        """Runs probes on the three agents' command binaries to verify availability."""
        print("=== Running Agent Model Verification Probes ===")
        
        # 1. Codex
        codex_cfg = self.config.get("agents", {}).get("codex", {})
        codex_cmd = codex_cfg.get("command", "codex")
        codex_model = codex_cfg.get("model", "gpt-5.5")
        
        print(f"Probing Codex (cmd: {codex_cmd}, model: {codex_model})...")
        code1, out1, err1 = run_cmd([codex_cmd, "--version"])
        code2, out2, err2 = run_cmd([codex_cmd, "--help"])
        code3, out3, err3 = run_cmd([codex_cmd, "exec", "--help"])
        
        codex_ok = (code1 == 0 or code2 == 0)
        self.models_verified["codex"] = {
            "status": "CLI_AVAILABLE" if codex_ok else "UNAVAILABLE",
            "version": out1.strip() if code1 == 0 else "unknown",
            "probe_details": f"version_code={code1}\nhelp_code={code2}\nexec_help_code={code3}\n{err1}\n{err2}\n{err3}"
        }
        
        # 2. Claude
        claude_cfg = self.config.get("agents", {}).get("claude", {})
        claude_cmd = claude_cfg.get("command", "claude")
        claude_model = claude_cfg.get("model", "opus")
        
        print(f"Probing Claude CLI (cmd: {claude_cmd}, model: {claude_model})...")
        cl_code1, cl_out1, cl_err1 = run_cmd([claude_cmd, "--version"])
        cl_code2, cl_out2, cl_err2 = run_cmd([claude_cmd, "--help"])
        cl_code3, cl_out3, cl_err3 = run_cmd([claude_cmd, "-p", "--help"])
        
        claude_ok = (cl_code1 == 0 or cl_code2 == 0)
        self.models_verified["claude"] = {
            "status": "CLI_AVAILABLE" if claude_ok else "UNAVAILABLE",
            "version": cl_out1.strip() if cl_code1 == 0 else "unknown",
            "probe_details": f"version_code={cl_code1}\nhelp_code={cl_code2}\nprint_help_code={cl_code3}\n{cl_err1}\n{cl_err2}\n{cl_err3}"
        }

        # 3. Antigravity
        anti_cfg = self.config.get("agents", {}).get("antigravity", {})
        anti_cmd = anti_cfg.get("command", "antigravity")
        anti_model = anti_cfg.get("model", "gemini-3.5-flash")
        
        print(f"Probing Antigravity CLI (cmd: {anti_cmd}, model: {anti_model})...")
        ant_code1, ant_out1, ant_err1 = run_cmd([anti_cmd, "--version"])
        ant_code2, ant_out2, ant_err2 = run_cmd([anti_cmd, "--help"])
        
        # Check if actually available
        antigravity_ok = (ant_code1 == 0 or ant_code2 == 0)
        self.models_verified["antigravity"] = {
            "status": "CLI_AVAILABLE" if antigravity_ok else "UNAVAILABLE",
            "version": ant_out1.strip() if ant_code1 == 0 else "unknown",
            "probe_details": f"version_code={ant_code1}\nhelp_code={ant_code2}\n{ant_err1}\n{ant_err2}"
        }

        # Check if the requested models are available (or commands exist)
        missing = []
        if not codex_ok:
            missing.append(f"Codex CLI '{codex_cmd}' not found or returned error")
        if not claude_ok:
            missing.append(f"Claude Code CLI '{claude_cmd}' not found or returned error")
        if not antigravity_ok:
            missing.append(f"Antigravity CLI '{anti_cmd}' not found or returned error")

        if missing:
            err_msg = "; ".join(missing)
            print(f"Verification Failed: {err_msg}")
            # Write probes to current/global logs if run_dir isn't active yet
            return False, err_msg
        
        print("All agents successfully verified!")
        return True, "All models verified"

    def run_checks(self, changed_files: List[str]) -> Tuple[bool, Dict[str, str], str, List[str]]:
        """Runs configured checks (pytest, compileall, ruff, mypy)."""
        print("=== Running Verification Checks ===")
        checks_cfg = self.config.get("checks", {})
        results = {}
        passed_all = True
        fail_reason = ""
        failed_checks = []

        # 1. compileall (only on changed python files, avoiding walking all files)
        if checks_cfg.get("compileall", True):
            print("Running python -m compileall...")
            py_bin = ".venv/bin/python" if Path(".venv/bin/python").exists() else "python3"
            changed_py_files = [f for f in changed_files if f.endswith(".py") and Path(f).exists()]
            if not changed_py_files:
                print("No Python files changed, skipping compileall.")
                results["compileall"] = "No Python files changed."
            else:
                print(f"Compiling changed Python files: {changed_py_files}")
                code, out, err = run_cmd([py_bin, "-m", "compileall"] + changed_py_files)
                results["compileall"] = out + "\n" + err
                self.log_artifact("compileall.txt", results["compileall"])
                if code != 0:
                    passed_all = False
                    failed_checks.append("compileall")
                    fail_reason = "STOP_COMPILE_FAILED"
                    print("compileall failed!")
                else:
                    print("compileall passed.")
        else:
            self.log_artifact("compileall.txt", "compileall was not configured.")

        # 2. pytest
        if checks_cfg.get("pytest", True):
            print("Running pytest...")
            pytest_bin = ".venv/bin/pytest" if Path(".venv/bin/pytest").exists() else "pytest"
            code, out, err = run_cmd([pytest_bin], env={"PYTHONPATH": str(Path.cwd())})
            results["pytest"] = out + "\n" + err
            self.log_artifact("pytest.txt", results["pytest"])
            if code != 0:
                passed_all = False
                failed_checks.append("pytest")
                if not fail_reason:
                    fail_reason = "STOP_TESTS_FAILED"
                print(f"pytest failed with exit code {code}!")
            else:
                print("pytest passed.")
        else:
            self.log_artifact("pytest.txt", "pytest was not configured.")

        # 3. ruff
        ruff_mode = checks_cfg.get("ruff", "auto")
        if ruff_mode in (True, "auto"):
            print("Running ruff...")
            code, out, err = run_cmd(["ruff", "check", "."])
            results["ruff"] = out + "\n" + err
            self.log_artifact("ruff.txt", results["ruff"])
            if code == 127: # not found
                if ruff_mode == "auto":
                    print("ruff not found (skipping as mode is 'auto').")
                else:
                    passed_all = False
                    failed_checks.append("ruff")
                    if not fail_reason:
                        fail_reason = "STOP_LINT_FAILED"
                    print("ruff not found, but is required!")
            elif code != 0:
                passed_all = False
                failed_checks.append("ruff")
                if not fail_reason:
                    fail_reason = "STOP_LINT_FAILED"
                print("ruff check failed!")
            else:
                print("ruff check passed.")
        else:
            self.log_artifact("ruff.txt", "ruff check was disabled.")

        # 4. mypy
        mypy_mode = checks_cfg.get("mypy", "auto")
        if mypy_mode in (True, "auto"):
            print("Running mypy...")
            mypy_bin = ".venv/bin/mypy" if Path(".venv/bin/mypy").exists() else "mypy"
            code, out, err = run_cmd([mypy_bin, "."], env={"PYTHONPATH": str(Path.cwd())})
            results["mypy"] = out + "\n" + err
            self.log_artifact("mypy.txt", results["mypy"])
            if code == 127: # not found
                if mypy_mode == "auto":
                    print("mypy not found (skipping as mode is 'auto').")
                else:
                    passed_all = False
                    failed_checks.append("mypy")
                    if not fail_reason:
                        fail_reason = "STOP_TYPECHECK_FAILED"
                    print("mypy not found, but is required!")
            elif code != 0:
                passed_all = False
                failed_checks.append("mypy")
                if not fail_reason:
                    fail_reason = "STOP_TYPECHECK_FAILED"
                print("mypy check failed!")
            else:
                print("mypy check passed.")
        else:
            self.log_artifact("mypy.txt", "mypy was disabled.")

        # 5. Invariants checker
        if checks_cfg.get("invariants", True):
            print("Running invariants check...")
            py_bin = ".venv/bin/python" if Path(".venv/bin/python").exists() else "python3"
            code, out, err = run_cmd([py_bin, "tools/check_invariants.py"])
            results["invariants"] = out + "\n" + err
            self.log_artifact("check_invariants.txt", results["invariants"])
            if code != 0:
                passed_all = False
                failed_checks.append("invariants")
                if not fail_reason:
                    fail_reason = "STOP_INVARIANTS_FAILED"
                print("invariants check failed!")
            else:
                print("invariants check passed.")
        else:
            self.log_artifact("check_invariants.txt", "invariants check was disabled.")

        return passed_all, results, fail_reason, failed_checks

    def check_forbidden_patterns(self, diff: str, changed_files: List[str], task_desc: str) -> Optional[str]:
        """Programmatically check git diff and changed files for forbidden constraints."""
        # 1. Diff size limits
        limits = self.config.get("limits", {})
        max_files = limits.get("max_changed_files", 30)
        max_lines = limits.get("max_diff_lines", 2500)

        if len(changed_files) > max_files:
            return "STOP_HIGH_RISK_CHANGE (Too many files changed)"
        
        diff_lines = len(diff.splitlines())
        if diff_lines > max_lines:
            return "STOP_HIGH_RISK_CHANGE (Diff too large)"

        # 2. File edit permissions check
        task_allows_contracts = any(kw in task_desc.lower() for kw in ["allow editing contracts", "modify contracts", "edit contracts"])
        task_allows_skills = any(kw in task_desc.lower() for kw in ["allow modifying skills", "edit skills", "modify skills"])

        for f in changed_files:
            if f.startswith("docs/contracts/"):
                if not task_allows_contracts:
                    return "STOP_CONTRACT_CHANGE"
            if f.startswith(".agents/skills/") or f == "AGENTS.md":
                if not task_allows_skills:
                    return "STOP_HIGH_RISK_CHANGE (Modified AGENTS.md or skills without permission)"
            # CI/CD or config file updates
            if f.startswith(".github/") or f in ["Dockerfile", "docker-compose.yml", "package.json"]:
                return "STOP_HIGH_RISK_CHANGE (Modified CI/deployment config)"
            # Dependencies modification
            if f in ["requirements.txt", "Pipfile", "poetry.lock"]:
                return "STOP_HIGH_RISK_CHANGE (Modified dependency configuration)"

        # Extract only added lines globally for secrets scan (C2)
        added_lines = extract_added_lines(diff)

        # 3. Secrets / token checks on added lines (C2)
        secrets_patterns = [
            r"(?i)api_key\s*=\s*['\"][a-zA-Z0-9_\-]{16,}['\"]",
            r"(?i)password\s*=\s*['\"][a-zA-Z0-9_\-]{8,}['\"]",
            r"(?i)secret_key\s*=\s*['\"][a-zA-Z0-9_\-]{16,}['\"]",
            r"(?i)token\s*=\s*['\"][a-zA-Z0-9_\-]{16,}['\"]"
        ]
        for pattern in secrets_patterns:
            if re.search(pattern, added_lines):
                return "STOP_HIGH_RISK_CHANGE (Detected hardcoded secrets/credentials)"

        # 4. Check for forbidden design constraints scoped per-file
        file_diffs = parse_per_file_diff(diff)
        forbidden_redis_files = ["controller.py", "protocol.py", "board_connection.py", "interfaces.py"]
        redis_patterns = [
            r"(?i)\bimport\s+redis\b",
            r"(?i)\bfrom\s+redis\b",
            r"(?i)\bimport\s+aioredis\b",
            r"(?i)\bfrom\s+aioredis\b",
            r"(?i)\bredis\s*=\s*",
            r"\.(publish|pubsub|subscribe|hset|hget|xadd)\("
        ]

        for f in changed_files:
            file_name = Path(f).name
            file_diff = file_diffs.get(f)
            if file_diff is not None:
                file_added = extract_added_lines(file_diff)
            else:
                # Fallback to global added lines to fail closed if diff parser fails to match filename
                file_added = added_lines

            # Redis check per-file
            if file_name in forbidden_redis_files:
                for pattern in redis_patterns:
                    if re.search(pattern, file_added):
                        return f"STOP_ARCHITECTURE_RISK (Redis in command path or controller file: {f})"

            # Direct board access check per-file
            if file_name != "board_connection.py" and not f.startswith("tests/") and not f.startswith("tools/"):
                if "open_connection" in file_added:
                    return f"STOP_ARCHITECTURE_RISK (Direct board connection attempted outside board_connection.py in {f})"

            # New terminal status values check per-file (skip tests/ and tools/)
            if not f.startswith("tests/") and not f.startswith("tools/"):
                status_assign_matches = re.findall(r"(?i)['\"]?\bstatus\b['\"]?\s*(?::\s*[a-zA-Z0-9_\.\[\]]+)?\s*[=:]\s*['\"]([a-zA-Z0-9_\-]+)['\"]", file_added)
                for stat in status_assign_matches:
                    if stat.lower() not in ["ok", "error", "timeout"]:
                        return f"STOP_ARCHITECTURE_RISK (New terminal status in {f}: {stat})"

        return None

    def execute_task(self, task_file: Path) -> str:
        """Executes a single task following the autonomous loop."""
        # Setup run folder
        timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        self.run_dir = self.agent_runs_parent / timestamp
        
        task_content = task_file.read_text(encoding="utf-8")
        
        # Parse task title for branch name
        title_line = [line for line in task_content.splitlines() if line.startswith("# ") or line.startswith("Task:")]
        task_title = title_line[0].replace("# ", "").replace("Task:", "").strip() if title_line else task_file.stem
        branch_slug = slugify(task_title)
        agent_branch = f"{self.config.get('repo', {}).get('agent_branch_prefix', 'agent/')}{branch_slug}"

        print(f"=== Starting Task: {task_title} ===")
        print(f"Artifact folder: {self.run_dir}")
        self.log_artifact("task.md", task_content)

        # 1. Clean worktree check
        if self.config.get("repo", {}).get("require_clean_worktree", True) and not self.allow_dirty:
            print("Checking worktree state...")
            dirty = git_status_porcelain()
            self.log_artifact("git_status_before.txt", dirty)
            if dirty:
                print("Worktree is dirty. Aborting.")
                self.log_report(
                    task_title, agent_branch, "STOP_DIRTY_WORKTREE",
                    "Dirty worktree detected before starting task."
                )
                return "STOP_DIRTY_WORKTREE"
        else:
            self.log_artifact("git_status_before.txt", "Skipped (clean check disabled or allow-dirty passed).")

        # 2. Branch switching
        current_branch = git_current_branch()
        main_br = self.config.get("repo", {}).get("main_branch", "main")
        print(f"Current branch: {current_branch}")
        
        if current_branch == main_br:
            print(f"Switching from '{main_br}' to agent branch '{agent_branch}'...")
            if not self.dry_run:
                # Check if branch already exists
                code, _, _ = run_cmd(["git", "show-ref", f"refs/heads/{agent_branch}"])
                if code == 0:
                    success = git_switch_branch(agent_branch)
                else:
                    success = git_create_branch(agent_branch)
                if not success:
                    print("Failed to switch branch.")
                    return "STOP_TOOL_ERROR"
        else:
            agent_branch = current_branch

        # 3. Model verification
        v_ok, v_err = self.verify_models()
        if not v_ok:
            self.log_report(task_title, agent_branch, "STOP_MODEL_UNVERIFIED", v_err)
            return "STOP_MODEL_UNVERIFIED"

        # Read context files
        context_files = [
            "AGENTS.md",
            "docs/contracts/V1_Networking_Decisions.md",
            "docs/contracts/Board_Developer_Guide.md"
        ]
        # Dynamically read any skill files
        skills_dir = Path(".agents/skills")
        if skills_dir.exists():
            for skill_path in skills_dir.rglob("SKILL.md"):
                context_files.append(str(skill_path))
        
        context_text = ""
        for cf in context_files:
            cf_path = Path(cf)
            if cf_path.exists():
                context_text += f"\n=== File: {cf} ===\n"
                context_text += cf_path.read_text(encoding="utf-8") + "\n"

        # Build Codex prompt
        codex_template_path = Path("tools/agent_orchestrator/prompts/codex_implement.md")
        codex_template = codex_template_path.read_text(encoding="utf-8") if codex_template_path.exists() else "{TASK_CONTENT}"
        codex_prompt = codex_template.replace("{TASK_CONTENT}", task_content).replace("{CONTEXT_TEXT}", context_text)

        # 4. Dry Run Mode Check
        if self.dry_run:
            print("\n--- Dry Run Mode ---")
            print(f"Branch: {agent_branch}")
            print(f"Codex cmd: {self.config['agents']['codex']['command']} exec --model {self.config['agents']['codex']['model']}")
            print(f"Claude cmd: {self.config['agents']['claude']['command']} -p --model {self.config['agents']['claude']['model']}")
            print("No changes will be applied.")
            self.log_artifact("codex_prompt.md", codex_prompt)
            self.log_report(task_title, agent_branch, "DRY_RUN_OK", "Dry run completed.", dry_run=True)
            return "DRY_RUN_OK"

        self.final_artifacts = {
            "codex_prompt.md": "",
            "codex_stdout.txt": "",
            "codex_stderr.txt": "",
            "git_status_after.txt": "",
            "git_diff.patch": "",
            "git_diff_stat.txt": "",
            "pytest.txt": "",
            "compileall.txt": "",
            "ruff.txt": "",
            "mypy.txt": "",
            "check_invariants.txt": "",
            "claude_prompt.md": "",
            "claude_stdout.txt": "",
            "claude_stderr.txt": "",
            "claude_review.md": "",
            "antigravity_prompt.md": "",
            "antigravity_audit.md": "",
            "claude_adjudication_prompt.md": "",
            "claude_adjudication.md": ""
        }

        limits = self.config.get("limits", {})
        max_cycles = limits.get("max_task_cycles", 5)
        
        cycle = 0
        feedback_to_codex = None
        
        final_classification = "STOP_TOOL_ERROR"
        final_reason = "Loop terminated without resolving."

        while cycle < max_cycles:
            cycle += 1
            print(f"\n--- Starting Loop Cycle {cycle}/{max_cycles} ---")
            
            # Format prompt with feedback if any
            current_prompt = codex_prompt
            if feedback_to_codex:
                current_prompt += f"\n\n## Feedback from Reviewers / Checks\nPlease address the following findings from the previous run:\n{feedback_to_codex}"
                feedback_to_codex = None
            
            self.final_artifacts["codex_prompt.md"] = current_prompt
            self.log_artifact(f"codex_prompt_cycle_{cycle}.md", current_prompt)
            
            # 5. Codex execution
            codex_cfg = self.config["agents"]["codex"]
            codex_cmd = [codex_cfg["command"], codex_cfg["mode"], "--model", codex_cfg["model"]]
            codex_cmd.append("--sandbox=workspace-write")
            
            print(f"Invoking Codex CLI implementation (cycle {cycle})...")
            code, out, err = run_cmd(codex_cmd, input_str=current_prompt)
            
            self.final_artifacts["codex_stdout.txt"] = out
            self.final_artifacts["codex_stderr.txt"] = err
            self.log_artifact(f"codex_stdout_cycle_{cycle}.txt", out)
            self.log_artifact(f"codex_stderr_cycle_{cycle}.txt", err)
            
            if code != 0:
                print(f"Codex exited with code {code}.")
                self.log_report(task_title, agent_branch, "STOP_TOOL_ERROR", f"Codex execution failed with code {code}. Stderr: {err}")
                return "STOP_TOOL_ERROR"

            # Stage newly-created files so they are visible to git status / git diff (B1)
            run_cmd(["git", "add", "-N", "."])

            # 6. Capture changes
            status_after = git_status_porcelain()
            self.final_artifacts["git_status_after.txt"] = status_after
            self.log_artifact(f"git_status_after_cycle_{cycle}.txt", status_after)
            
            diff = git_diff()
            self.final_artifacts["git_diff.patch"] = diff
            self.log_artifact(f"git_diff_cycle_{cycle}.patch", diff)
            
            diff_stat = git_diff_stat()
            self.final_artifacts["git_diff_stat.txt"] = diff_stat
            self.log_artifact(f"git_diff_stat_cycle_{cycle}.txt", diff_stat)

            changed_files = parse_changed_files(status_after)
            if not changed_files:
                print("Codex made no changes.")
                self.log_report(task_title, agent_branch, "STOP_BACKLOG_EMPTY", "Codex executed but made no modifications.")
                return "STOP_BACKLOG_EMPTY"

            # 7. Check forbidden changes before running reviews
            forbidden_stop = self.check_forbidden_patterns(diff, changed_files, task_content)
            if forbidden_stop:
                print(f"Forbidden change detected: {forbidden_stop}")
                self.log_report(task_title, agent_branch, forbidden_stop, f"Check blocked by security/architecture gating: {forbidden_stop}")
                return forbidden_stop

            # 8. Run checks (pytest, compileall, etc.)
            checks_pass, check_logs, stop_reason, failed_checks = self.run_checks(changed_files)
            self.final_artifacts["pytest.txt"] = check_logs.get("pytest", "")
            self.final_artifacts["compileall.txt"] = check_logs.get("compileall", "")
            self.final_artifacts["ruff.txt"] = check_logs.get("ruff", "")
            self.final_artifacts["mypy.txt"] = check_logs.get("mypy", "")

            if not checks_pass:
                if cycle < max_cycles:
                    print(f"Checks failed. Feeding failures back to Codex for cycle {cycle+1}.")
                    errors = []
                    for check_name in failed_checks:
                        check_log = check_logs.get(check_name, "")
                        if check_log.strip():
                            errors.append(f"### {check_name} failure:\n{check_log}")
                    feedback_to_codex = "\n".join(errors)
                    continue
                else:
                    self.log_report(task_title, agent_branch, stop_reason, f"Verification checks failed: {stop_reason}")
                    return stop_reason

            # 9. Claude review
            claude_template_path = Path("tools/agent_orchestrator/prompts/claude_review.md")
            claude_template = claude_template_path.read_text(encoding="utf-8") if claude_template_path.exists() else "{GIT_DIFF}"
            claude_prompt = claude_template.replace("{GIT_DIFF}", diff).replace("{CONTEXT_TEXT}", context_text)
            self.final_artifacts["claude_prompt.md"] = claude_prompt
            self.log_artifact(f"claude_prompt_cycle_{cycle}.md", claude_prompt)

            claude_cfg = self.config["agents"]["claude"]
            claude_cmd = [claude_cfg["command"], "-p", "--model", claude_cfg["model"]]
            
            print(f"Invoking Claude CLI review (cycle {cycle})...")
            cl_code, cl_out, cl_err = run_cmd(claude_cmd, input_str=claude_prompt)
            
            self.final_artifacts["claude_stdout.txt"] = cl_out
            self.final_artifacts["claude_stderr.txt"] = cl_err
            self.final_artifacts["claude_review.md"] = cl_out
            self.log_artifact(f"claude_stdout_cycle_{cycle}.txt", cl_out)
            self.log_artifact(f"claude_stderr_cycle_{cycle}.txt", cl_err)
            self.log_artifact(f"claude_review_cycle_{cycle}.md", cl_out)

            if cl_code != 0:
                print(f"Claude CLI exited with code {cl_code}.")
                self.log_report(task_title, agent_branch, "STOP_CLAUDE_REVIEW_FAILED", f"Claude CLI execution failed: {cl_err}")
                return "STOP_CLAUDE_REVIEW_FAILED"

            # Parse Claude verdict (B4) safely (anchored multiline regex, taking the last match)
            claude_verdict_matches = list(re.finditer(r"^Final verdict:\s*(PASS|FAIL)\s*$", cl_out, re.MULTILINE | re.IGNORECASE))
            if not claude_verdict_matches:
                print("Claude review response was missing a valid final verdict.")
                self.log_report(task_title, agent_branch, "STOP_CLAUDE_REVIEW_FAILED", "Could not parse PASS/FAIL verdict from Claude output.")
                return "STOP_CLAUDE_REVIEW_FAILED"
            
            claude_verdict = claude_verdict_matches[-1].group(1).upper()
            print(f"Claude Review Verdict (cycle {cycle}): {claude_verdict}")
            
            # Check if there are items under "Must fix before commit:"
            must_fix_match = re.search(r"Must fix before commit:(.*?)(?=Should fix soon:|Looks good:|Questions for operator:|Final verdict:|$)", cl_out, re.DOTALL | re.IGNORECASE)
            must_fix_content = must_fix_match.group(1).strip() if must_fix_match else ""
            has_must_fix = parse_must_fix(must_fix_content)

            if claude_verdict == "FAIL" or has_must_fix:
                if cycle < max_cycles:
                    print(f"Claude review found issues. Feeding findings back to Codex for cycle {cycle+1}.")
                    feedback_to_codex = f"Claude Review Findings:\n{cl_out}"
                    continue
                else:
                    self.log_report(task_title, agent_branch, "STOP_CLAUDE_REVIEW_FAILED", "Claude review returned FAIL after maximum cycles.")
                    return "STOP_CLAUDE_REVIEW_FAILED"

            # 10. Antigravity audit
            print("Claude review passed. Invoking Antigravity final audit...")
            antigravity_template_path = Path("tools/agent_orchestrator/prompts/antigravity_audit.md")
            antigravity_template = antigravity_template_path.read_text(encoding="utf-8") if antigravity_template_path.exists() else ""
            antigravity_prompt = antigravity_template \
                .replace("{TASK_CONTENT}", task_content) \
                .replace("{GIT_STATUS}", status_after) \
                .replace("{GIT_DIFF_STAT}", diff_stat) \
                .replace("{GIT_DIFF}", diff) \
                .replace("{PYTEST_LOGS}", check_logs.get("pytest", "No logs")) \
                .replace("{CONTEXT_TEXT}", context_text)
            
            self.final_artifacts["antigravity_prompt.md"] = antigravity_prompt
            self.log_artifact(f"antigravity_prompt_cycle_{cycle}.md", antigravity_prompt)

            anti_cfg = self.config["agents"]["antigravity"]
            anti_cmd = [anti_cfg["command"], "--model", anti_cfg["model"]]
            
            ant_code, ant_out, ant_err = run_cmd(anti_cmd, input_str=antigravity_prompt)
            
            self.final_artifacts["antigravity_audit.md"] = ant_out
            self.log_artifact(f"antigravity_stdout_cycle_{cycle}.txt", ant_out)
            self.log_artifact(f"antigravity_stderr_cycle_{cycle}.txt", ant_err)
            self.log_artifact(f"antigravity_audit_cycle_{cycle}.md", ant_out)

            if ant_code != 0:
                print(f"Antigravity CLI exited with code {ant_code}.")
                self.log_report(task_title, agent_branch, "STOP_ANTIGRAVITY_AUDIT_FAILED", f"Antigravity audit command failed: {ant_err}")
                return "STOP_ANTIGRAVITY_AUDIT_FAILED"

            # Parse Antigravity verdict safely (anchored multiline regex, taking the last match)
            anti_verdict_matches = list(re.finditer(r"^Final verdict:\s*(PASS|FAIL)\s*$", ant_out, re.MULTILINE | re.IGNORECASE))
            if not anti_verdict_matches:
                print("Antigravity audit response was missing a valid final verdict.")
                self.log_report(task_title, agent_branch, "STOP_ANTIGRAVITY_AUDIT_FAILED", "Could not parse PASS/FAIL verdict from Antigravity audit.")
                return "STOP_ANTIGRAVITY_AUDIT_FAILED"
            
            anti_verdict = anti_verdict_matches[-1].group(1).upper()
            print(f"Antigravity Audit Verdict: {anti_verdict}")
            
            if anti_verdict == "PASS":
                print("Antigravity audit passed. Auto-committing changes.")
                final_classification = "AUTO_COMMITTED"
                break
            else:
                # Antigravity returned FAIL
                allow_override = self.config.get("review", {}).get("allow_claude_override_antigravity", True)
                if not allow_override:
                    print("Antigravity audit failed, and Claude override is disabled. Stopping for human review.")
                    self.log_report(task_title, agent_branch, "STOP_HUMAN_REVIEW_REQUIRED", f"Antigravity audit returned FAIL, and override is disabled. Audit output:\n{ant_out}")
                    return "STOP_HUMAN_REVIEW_REQUIRED"
                
                # Compute orchestrator facts
                tests_passed = "pytest" not in failed_checks and "compileall" not in failed_checks
                invariants_passed = "invariants" not in failed_checks
                frozen_contracts_changed = any(f.startswith("docs/contracts/") for f in changed_files)
                
                command_path_files = {
                    "controller.py",
                    "board_connection.py",
                    "protocol.py",
                    "state.py",
                    "interfaces.py",
                    "local_socket.py"
                }
                command_path_files_changed = any(Path(f).name in command_path_files for f in changed_files)
                
                safety_keywords = ["estop", "estop_ack", "blocked_by_estop", "safe state", "safety"]
                safety_or_estop_path_changed = any(kw in diff for kw in safety_keywords)
                
                seq_keywords = ["board_seq", "PendingCommand", "pending", "pop_pending", "seq"]
                seq_or_board_seq_logic_changed = any(kw in diff for kw in seq_keywords)
                
                writer_keywords = ["SerializedBoardWriter", "writer lock", "send_estop", "write_message", "writer.drain", "writer.write"]
                writer_serialization_logic_changed = any(kw in diff for kw in writer_keywords)
                
                security_or_secret_stop_triggered = (forbidden_stop is not None)
                
                # Check deterministic hard-stop conditions:
                has_hard_stop_fact = False
                hard_stop_reasons = []
                
                if not tests_passed:
                    has_hard_stop_fact = True
                    hard_stop_reasons.append("Tests did not pass")
                if not invariants_passed:
                    has_hard_stop_fact = True
                    hard_stop_reasons.append("Invariants check did not pass")
                if frozen_contracts_changed:
                    has_hard_stop_fact = True
                    hard_stop_reasons.append("Frozen contracts were modified")
                if security_or_secret_stop_triggered:
                    has_hard_stop_fact = True
                    hard_stop_reasons.append("Security or secret stop was triggered")
                if command_path_files_changed:
                    has_hard_stop_fact = True
                    hard_stop_reasons.append("Command path files were modified")
                if safety_or_estop_path_changed:
                    has_hard_stop_fact = True
                    hard_stop_reasons.append("Safety or e-stop logic was affected")
                if seq_or_board_seq_logic_changed:
                    has_hard_stop_fact = True
                    hard_stop_reasons.append("Sequence number logic was affected")
                if writer_serialization_logic_changed:
                    has_hard_stop_fact = True
                    hard_stop_reasons.append("Writer serialization logic was affected")
                
                if has_hard_stop_fact:
                    reasons_str = "; ".join(hard_stop_reasons)
                    print(f"Antigravity failed and hard-stop facts were met: {reasons_str}. Stopping for human review.")
                    self.log_report(task_title, agent_branch, "STOP_HUMAN_REVIEW_REQUIRED", f"Antigravity audit returned FAIL and hard-stop facts were met: {reasons_str}. Audit output:\n{ant_out}")
                    return "STOP_HUMAN_REVIEW_REQUIRED"
                
                # Ask Claude for structured adjudication
                print("Antigravity raised concerns. Passing findings to Claude for structured adjudication...")
                
                claude_adjudication_template_path = Path("tools/agent_orchestrator/prompts/claude_adjudication.md")
                if claude_adjudication_template_path.exists():
                    claude_adjudication_template = claude_adjudication_template_path.read_text(encoding="utf-8")
                else:
                    claude_adjudication_template = "Adjudicate Antigravity concerns:\n{ANTIGRAVITY_AUDIT_TEXT}\nContext:\n{CONTEXT_TEXT}"
                
                claude_adjudication_prompt = claude_adjudication_template \
                    .replace("{ANTIGRAVITY_AUDIT_TEXT}", ant_out) \
                    .replace("{CONTEXT_TEXT}", context_text)
                
                self.final_artifacts["claude_adjudication_prompt.md"] = claude_adjudication_prompt
                self.log_artifact(f"claude_adjudication_prompt_cycle_{cycle}.md", claude_adjudication_prompt)
                
                print("Invoking Claude CLI to adjudicate Antigravity concerns...")
                cl_eval_code, cl_eval_out, cl_eval_err = run_cmd(claude_cmd, input_str=claude_adjudication_prompt)
                
                self.final_artifacts["claude_adjudication.md"] = cl_eval_out
                self.log_artifact(f"claude_adjudication_stdout_cycle_{cycle}.txt", cl_eval_out)
                self.log_artifact(f"claude_adjudication_stderr_cycle_{cycle}.txt", cl_eval_err)
                self.log_artifact(f"claude_adjudication_cycle_{cycle}.md", cl_eval_out)
                
                if cl_eval_code != 0:
                    print(f"Claude CLI exited with code {cl_eval_code} during adjudication.")
                    self.log_report(task_title, agent_branch, "STOP_CLAUDE_REVIEW_FAILED", f"Claude CLI adjudication failed: {cl_eval_err}")
                    return "STOP_CLAUDE_REVIEW_FAILED"
                
                # Parse adjudication verdict safely
                adj_matches = list(re.finditer(r"^ANTIGRAVITY_ADJUDICATION:\s*(OVERRIDE_ALLOWED|HARD_STOP)\s*$", cl_eval_out, re.MULTILINE | re.IGNORECASE))
                if not adj_matches:
                    print("Claude adjudication response was missing a valid ANTIGRAVITY_ADJUDICATION line.")
                    self.log_report(task_title, agent_branch, "STOP_HUMAN_REVIEW_REQUIRED", "Claude adjudication output could not be parsed safely.")
                    return "STOP_HUMAN_REVIEW_REQUIRED"
                
                adjudication_verdict = adj_matches[-1].group(1).upper()
                print(f"Claude Adjudication Verdict: {adjudication_verdict}")
                
                conf_matches = list(re.finditer(r"^confidence:\s*(low|medium|high)\s*$", cl_eval_out, re.MULTILINE | re.IGNORECASE))
                if not conf_matches:
                    print("Claude adjudication response was missing a valid confidence level.")
                    self.log_report(task_title, agent_branch, "STOP_HUMAN_REVIEW_REQUIRED", "Claude confidence level is missing from adjudication.")
                    return "STOP_HUMAN_REVIEW_REQUIRED"
                
                confidence = conf_matches[-1].group(1).lower()
                if confidence == "low":
                    print("Claude adjudication confidence is low. Stopping for human review.")
                    self.log_report(task_title, agent_branch, "STOP_HUMAN_REVIEW_REQUIRED", "Claude confidence level is low.")
                    return "STOP_HUMAN_REVIEW_REQUIRED"
                
                if adjudication_verdict == "HARD_STOP":
                    print("Claude adjudication returned HARD_STOP. Stopping for human review.")
                    self.log_report(task_title, agent_branch, "STOP_HUMAN_REVIEW_REQUIRED", "Claude adjudication returned HARD_STOP.")
                    return "STOP_HUMAN_REVIEW_REQUIRED"
                
                # Override allowed
                print("Claude found Antigravity's concerns safe to override. Overriding and committing.")
                final_classification = "AUTO_COMMITTED"
                break
        else:
            print("Reached maximum execution cycles without matching PASS verdicts.")
            self.log_report(task_title, agent_branch, "STOP_TOOL_ERROR", "Max cycles reached without matching PASS verdicts.")
            return "STOP_TOOL_ERROR"

        # 11. Auto commit
        if final_classification == "AUTO_COMMITTED":
            print("All checks passed. Committing changes...")
            commit_msg = f"agent: {task_title}"
            c_code, c_log = git_commit(commit_msg)
            if c_code != 0:
                print(f"Git commit failed with code {c_code}. details: {c_log}")
                self.log_report(task_title, agent_branch, "STOP_TOOL_ERROR", f"Git commit failed: {c_log}")
                return "STOP_TOOL_ERROR"

            # Get latest commit hash
            _, ref_out, _ = run_cmd(["git", "rev-parse", "HEAD"])
            commit_hash = ref_out.strip()

            self.log_report(
                task_title, agent_branch, "AUTO_COMMITTED",
                "Task implemented, verified, reviewed, and audited successfully.",
                commit_hash=commit_hash
            )
            return "AUTO_COMMITTED"
        else:
            self.log_report(task_title, agent_branch, final_classification, final_reason)
            return final_classification

    def log_report(self, task_summary: str, branch: str, classification: str, reason: str, commit_hash: Optional[str] = None, dry_run: bool = False):
        """Builds and logs the final report."""
        # Write final standard artifacts using the final successful or stopped cycle data
        for filename, content in getattr(self, "final_artifacts", {}).items():
            if content:
                self.log_artifact(filename, content)

        codex_cfg = self.config.get("agents", {}).get("codex", {})
        claude_cfg = self.config.get("agents", {}).get("claude", {})
        anti_cfg = self.config.get("agents", {}).get("antigravity", {})
        
        model_status_str = ""
        for name, details in self.models_verified.items():
            model_status_str += f"- {name}: {details['status']} ({details.get('version', 'unknown')})\n"
        
        git_diff_stat_str = ""
        if self.run_dir and (self.run_dir / "git_diff_stat.txt").exists():
            git_diff_stat_str = (self.run_dir / "git_diff_stat.txt").read_text(encoding="utf-8")
        
        pytest_status = "PASS" if not classification.startswith("STOP_TEST") else "FAIL"
        if not self.config.get("checks", {}).get("pytest", True):
            pytest_status = "SKIPPED"
        
        invariants_status = "PASS" if not classification.startswith("STOP_INVARIANTS") else "FAIL"
        if not self.config.get("checks", {}).get("invariants", True):
            invariants_status = "SKIPPED"
        
        claude_verdict = "PASS" if not classification.startswith("STOP_CLAUDE") else "FAIL"
        anti_verdict = "PASS" if not classification.startswith("STOP_ANTIGRAVITY") else "FAIL"
        
        auto_commit_decision = "ALLOWED" if classification == "AUTO_COMMITTED" else "STOPPED"
        if dry_run:
            auto_commit_decision = "DRY_RUN (NOT COMMITTED)"
            
        stop_reason_val = classification if classification not in ("AUTO_COMMITTED", "DRY_RUN_OK") else "None (Completed)"
        
        report_content = f"""# Final Report: {task_summary}

## Metadata
- **Agent Branch**: {branch}
- **Dry Run**: {dry_run}
- **Timestamp**: {datetime.now().isoformat()}

## Configured Models
- **Codex**: {codex_cfg.get('model')} via `{codex_cfg.get('command')}`
- **Claude**: {claude_cfg.get('model')} via `{claude_cfg.get('command')}`
- **Antigravity**: {anti_cfg.get('model')} via `{anti_cfg.get('command')}`

## Verified Model Status
{model_status_str}

## Execution Summary
- **Codex Command Used**: `{codex_cfg.get('command')} {codex_cfg.get('mode')} --model {codex_cfg.get('model')}`
- **Auto-Commit Decision**: {auto_commit_decision}
- **Stop Reason**: {stop_reason_val}
- **Commit Hash**: {commit_hash if commit_hash else "None"}

## Verification Outcomes
- **Pytest Output**: {pytest_status}
- **Invariants Output**: {invariants_status}
- **Claude Verdict**: {claude_verdict}
- **Antigravity Audit Verdict**: {anti_verdict}

## Changed Files
```text
{git_diff_stat_str}
```

## Details
{reason}
"""
        self.log_artifact("final_report.md", report_content)
        print("\n=== FINAL EXECUTION REPORT ===")
        print(report_content)


def main():
    parser = argparse.ArgumentParser(description="Autonomous agent orchestrator loop.")
    parser.add_argument("--task-file", type=str, help="Path to single task.md file to run.")
    parser.add_argument("--backlog", type=str, help="Path to backlog.md file to run in backlog mode.")
    parser.add_argument("--dry-run", action="store_true", help="Print planned actions without running Codex/Claude.")
    parser.add_argument("--allow-dirty", action="store_true", help="Allow running tasks on a dirty worktree.")
    parser.add_argument("--config", type=str, help="Path to TOML configuration file.")
    
    args = parser.parse_args()
    
    if not args.task_file and not args.backlog:
        parser.error("You must specify either --task-file or --backlog.")

    # Resolve configuration
    config_path = Path(args.config) if args.config else None
    config = load_config(config_path)

    orchestrator = Orchestrator(config, dry_run=args.dry_run, allow_dirty=args.allow_dirty)

    if args.task_file:
        task_path = Path(args.task_file)
        if not task_path.exists():
            print(f"Error: Task file '{args.task_file}' does not exist.")
            sys.exit(1)
        
        classification = orchestrator.execute_task(task_path)
        if classification in ("AUTO_COMMITTED", "DRY_RUN_OK"):
            sys.exit(0)
        else:
            sys.exit(1)

    elif args.backlog:
        backlog_path = Path(args.backlog)
        if not backlog_path.exists():
            print(f"Error: Backlog file '{args.backlog}' does not exist.")
            sys.exit(1)

        print(f"Running in Backlog Mode on '{args.backlog}'...")
        # Simple backlog parser: find lines matching `- [ ]` or `* [ ]`
        backlog_content = backlog_path.read_text(encoding="utf-8")
        
        # Regex to find tasks of style `- [ ] Task: ...` or `* [ ] Task: ...`
        task_pattern = re.compile(r"^\s*[-\*]\s*\[\s*\]\s+(.*)$", re.MULTILINE)
        matches = list(task_pattern.finditer(backlog_content))
        
        if not matches:
            print("No pending tasks found in backlog. Stop.")
            sys.exit(0)

        # Process the first task found
        match = matches[0]
        task_line_text = match.group(1).strip()
        
        # Extract task details until next top-level checkbox or heading
        task_start = match.start()
        remaining_text = backlog_content[task_start:]
        lines = remaining_text.splitlines()
        
        task_lines = [lines[0]]
        for line in lines[1:]:
            is_checkbox = re.match(r"^\s*[-\*]\s*\[\s*[xX ]\s*\]", line)
            is_top_level_heading = re.match(r"^#\s+", line)
            is_hr = re.match(r"^---", line)
            if is_checkbox or is_top_level_heading or is_hr:
                break
            task_lines.append(line)
            
        task_lines[0] = f"# {task_line_text}"
        full_scratch_content = "\n".join(task_lines)
        
        # Create a temporary task file for execution
        temp_task_file = Path("tools/agent_orchestrator/scratch_task.md")
        temp_task_file.parent.mkdir(parents=True, exist_ok=True)
        temp_task_file.write_text(full_scratch_content, encoding="utf-8")
        
        try:
            classification = orchestrator.execute_task(temp_task_file)
            if classification == "AUTO_COMMITTED":
                # Mark as checked in the backlog file
                start, end = match.span()
                # Find the checkbox position
                checkbox_match = re.search(r"([-\*])\s*\[\s*\]", backlog_content[start-10:end]) # lookback a bit
                
                # Slices of content
                lines = backlog_content.splitlines()
                for idx, line in enumerate(lines):
                    stripped = line.strip()
                    if (stripped.startswith("- [ ]") or stripped.startswith("* [ ]")) and task_line_text in line:
                        prefix = "- [ ]" if stripped.startswith("- [ ]") else "* [ ]"
                        replacement = "- [x]" if stripped.startswith("- [ ]") else "* [x]"
                        lines[idx] = line.replace(prefix, replacement, 1)
                        break
                
                backlog_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
                print(f"Task '{task_line_text}' completed and checked off in backlog.")
                sys.exit(0)
            elif classification == "DRY_RUN_OK":
                print("Dry run completed successfully in backlog mode. Not committing.")
                sys.exit(0)
            else:
                print(f"Stopped execution on backlog task. Classification: {classification}")
                sys.exit(1)
        finally:
            if temp_task_file.exists():
                temp_task_file.unlink()

if __name__ == "__main__":
    main()
