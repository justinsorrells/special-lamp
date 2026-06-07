import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.agent_orchestrator.orchestrate import (
    Orchestrator,
    extract_added_lines,
    parse_changed_files,
    parse_must_fix,
    parse_per_file_diff,
    parse_simple_toml,
    slugify,
)


class TestAgentOrchestrator(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.config = {
            "agents": {
                "codex": {"command": "codex", "mode": "exec", "model": "gpt-5.5"},
                "claude": {"command": "claude", "mode": "print", "model": "opus"},
                "antigravity": {"command": "antigravity", "model": "gemini-3.5-flash"}
            },
            "repo": {
                "main_branch": "main",
                "agent_branch_prefix": "agent/",
                "require_clean_worktree": True
            },
            "checks": {
                "pytest": False,
                "compileall": False,
                "ruff": False,
                "mypy": False
            },
            "limits": {
                "max_changed_files": 5,
                "max_diff_lines": 50
            }
        }
        self.orchestrator = Orchestrator(self.config, dry_run=True, allow_dirty=True)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_parse_simple_toml(self):
        toml_content = """
[agents.codex]
command = "custom_codex"
mode = "exec"
model = "gpt-5.5"

[repo]
require_clean_worktree = false
never_auto_push = true
max_limit = 100
"""
        toml_file = Path(self.temp_dir) / "config.toml"
        toml_file.write_text(toml_content, encoding="utf-8")
        
        parsed = parse_simple_toml(toml_file)
        self.assertEqual(parsed["agents"]["codex"]["command"], "custom_codex")
        self.assertEqual(parsed["agents"]["codex"]["model"], "gpt-5.5")
        self.assertEqual(parsed["repo"]["require_clean_worktree"], False)
        self.assertEqual(parsed["repo"]["never_auto_push"], True)
        self.assertEqual(parsed["repo"]["max_limit"], 100)

    def test_slugify(self):
        self.assertEqual(slugify("Task: Implement Redis integration!"), "task-implement-redis-integration")
        self.assertEqual(slugify("  Hello   World  "), "hello-world")
        self.assertEqual(slugify("ESTOP-active check"), "estop-active-check")

    def test_forbidden_patterns_changed_files_limit(self):
        diff = "some diff"
        changed_files = ["file1.py", "file2.py", "file3.py", "file4.py", "file5.py", "file6.py"]
        res = self.orchestrator.check_forbidden_patterns(diff, changed_files, "test task")
        self.assertIsNotNone(res)
        self.assertIn("STOP_HIGH_RISK_CHANGE", res)

    def test_forbidden_patterns_diff_lines_limit(self):
        diff = "line\n" * 60
        changed_files = ["file1.py"]
        res = self.orchestrator.check_forbidden_patterns(diff, changed_files, "test task")
        self.assertIsNotNone(res)
        self.assertIn("STOP_HIGH_RISK_CHANGE", res)

    def test_forbidden_patterns_contract_file(self):
        diff = "edit contracts"
        changed_files = ["docs/contracts/V1_Networking_Decisions.md"]
        res = self.orchestrator.check_forbidden_patterns(diff, changed_files, "ordinary task description")
        self.assertEqual(res, "STOP_CONTRACT_CHANGE")
        
        # Should allow if task explicitly contains permission keyword
        res_allowed = self.orchestrator.check_forbidden_patterns(
            diff, changed_files, "allow editing contracts for this task"
        )
        self.assertNotEqual(res_allowed, "STOP_CONTRACT_CHANGE")

    def test_forbidden_patterns_skills_file(self):
        diff = "edit skills"
        changed_files = [".agents/skills/asyncio-controller/SKILL.md"]
        res = self.orchestrator.check_forbidden_patterns(diff, changed_files, "ordinary task")
        self.assertIn("STOP_HIGH_RISK_CHANGE", res)

    def test_forbidden_patterns_secrets(self):
        diff = '+api_key = "abc123xyz789SECRET"'
        changed_files = ["config.py"]
        res = self.orchestrator.check_forbidden_patterns(diff, changed_files, "test task")
        self.assertIsNotNone(res)
        self.assertIn("STOP_HIGH_RISK_CHANGE", res)

    def test_forbidden_patterns_new_status(self):
        diff = """diff --git a/controller.py b/controller.py
--- a/controller.py
+++ b/controller.py
+ "status": "completed"
"""
        changed_files = ["controller.py"]
        res = self.orchestrator.check_forbidden_patterns(diff, changed_files, "test task")
        self.assertIsNotNone(res)
        self.assertIn("STOP_ARCHITECTURE_RISK", res)

    def test_parse_changed_files(self):
        status_output = """ M ordinary_file.py
?? "space file.py"
R  old_file.py -> new_file.py
RM "quoted old.py" -> "quoted new.py"
"""
        files = parse_changed_files(status_output)
        self.assertEqual(files, [
            "ordinary_file.py",
            "space file.py",
            "new_file.py",
            "quoted new.py"
        ])

    def test_extract_added_lines(self):
        diff = """--- a/some_file.py
+++ b/some_file.py
@@ -10,3 +10,4 @@
-old line
+added line 1
+added line 2
 context line
"""
        added = extract_added_lines(diff)
        self.assertEqual(added, "added line 1\nadded line 2")

    def test_parse_must_fix(self):
        self.assertFalse(parse_must_fix("None"))
        self.assertFalse(parse_must_fix("- N/A"))
        self.assertFalse(parse_must_fix("No issues."))
        self.assertFalse(parse_must_fix("Looks clean"))
        self.assertTrue(parse_must_fix("- Fix validation in protocol.py"))
        self.assertTrue(parse_must_fix("noneexistent status check should be fixed"))

    def test_verdict_regex_emphasis(self):
        import re
        pattern = r"(?i)Final verdict\b[\s*:*_]*(PASS|FAIL)\b"
        
        m1 = re.search(pattern, "**Final verdict:** PASS")
        self.assertIsNotNone(m1)
        self.assertEqual(m1.group(1).upper(), "PASS")

        m2 = re.search(pattern, "*Final verdict:* **FAIL**")
        self.assertIsNotNone(m2)
        self.assertEqual(m2.group(1).upper(), "FAIL")

        m3 = re.search(pattern, "Final Verdict: PASS")
        self.assertIsNotNone(m3)
        self.assertEqual(m3.group(1).upper(), "PASS")

    def test_parse_per_file_diff(self):
        diff = """diff --git a/board_connection.py b/board_connection.py
index 12345..67890 100644
--- a/board_connection.py
+++ b/board_connection.py
@@ -10,3 +10,4 @@
+added in board_connection
diff --git a/controller.py b/controller.py
index abcde..fghij 100644
--- a/controller.py
+++ b/controller.py
@@ -20,3 +20,4 @@
+added in controller
"""
        file_diffs = parse_per_file_diff(diff)
        self.assertIn("board_connection.py", file_diffs)
        self.assertIn("controller.py", file_diffs)
        self.assertIn("added in board_connection", file_diffs["board_connection.py"])
        self.assertIn("added in controller", file_diffs["controller.py"])

    def test_check_forbidden_patterns_scoped(self):
        # board_connection.py legitimately adds open_connection, and controller.py is modified.
        # This must PASS and NOT trigger STOP_ARCHITECTURE_RISK.
        diff = """diff --git a/board_connection.py b/board_connection.py
--- a/board_connection.py
+++ b/board_connection.py
+            reader, raw_writer = await asyncio.open_connection("host", 8080)
diff --git a/controller.py b/controller.py
--- a/controller.py
+++ b/controller.py
+    # Just wiring something up, no direct board access here
"""
        changed_files = ["board_connection.py", "controller.py"]
        res = self.orchestrator.check_forbidden_patterns(diff, changed_files, "test task")
        self.assertIsNone(res)

        # However, if controller.py adds open_connection directly, it must FAIL.
        bad_diff = """diff --git a/controller.py b/controller.py
--- a/controller.py
+++ b/controller.py
+            reader, raw_writer = await asyncio.open_connection("host", 8080)
"""
        res_bad = self.orchestrator.check_forbidden_patterns(bad_diff, ["controller.py"], "test task")
        self.assertIsNotNone(res_bad)
        self.assertIn("STOP_ARCHITECTURE_RISK", res_bad)

    def test_check_forbidden_patterns_fallback(self):
        # If the file path is mismatched or missing in diff, it should fall back to scanning the global added_lines.
        # This will fail closed (raise violation) instead of failing open.
        diff = """diff --git a/mismatched_filename.py b/mismatched_filename.py
--- a/mismatched_filename.py
+++ b/mismatched_filename.py
+            reader, raw_writer = await asyncio.open_connection("host", 8080)
"""
        # Even though "controller.py" is passed as the changed file (which does not match the diff),
        # the fallback scan must detect the connection creation in the global diff and fail closed.
        res = self.orchestrator.check_forbidden_patterns(diff, ["controller.py"], "test task")
        self.assertIsNotNone(res)
        self.assertIn("STOP_ARCHITECTURE_RISK", res)

    def test_config_example_toml_parsing(self):
        # Verify simple toml parser can read config.example.toml
        path = Path("tools/agent_orchestrator/config.example.toml")
        parsed = parse_simple_toml(path)
        self.assertIn("agents", parsed)
        self.assertIn("checks", parsed)
        self.assertIn("review", parsed)
        self.assertEqual(parsed["checks"]["invariants"], True)
        self.assertEqual(parsed["review"]["allow_claude_override_antigravity"], False)

    def test_dry_run_and_prompt_rendering(self):
        # Set agent_runs_parent to temp_dir
        self.orchestrator.agent_runs_parent = Path(self.temp_dir)
        
        # Create a temp task file
        task_file = Path(self.temp_dir) / "dummy_task.md"
        task_file.write_text("# Dummy Task\n\nImplement something cool.", encoding="utf-8")
        
        # Mock verify_models to return True
        self.orchestrator.verify_models = lambda: (True, "All models verified")
        
        # Run execution
        res = self.orchestrator.execute_task(task_file)
        
        # 1. Assert dry run returns DRY_RUN_OK
        self.assertEqual(res, "DRY_RUN_OK")
        
        # 2. Assert codex_prompt.md artifact contains prompt context
        prompt_artifact = next(Path(self.temp_dir).rglob("codex_prompt.md"), None)
        self.assertIsNotNone(prompt_artifact)
        
        prompt_content = prompt_artifact.read_text(encoding="utf-8")
        self.assertIn("Dummy Task", prompt_content)
        self.assertIn("Loaded Contracts and Context", prompt_content)
        self.assertIn("Hyperloop board networking stack", prompt_content)

    @patch("tools.agent_orchestrator.orchestrate.run_cmd")
    def test_invariant_check_failure_stops_before_claude(self, mock_run_cmd):
        def side_effect(args, *arg, **kw):
            cmd = args[0]
            if cmd == "codex":
                return 0, "Codex finished successfully", ""
            elif cmd == "git":
                sub = args[1]
                if sub == "status":
                    return 0, " M file.py\n", ""
                elif sub == "diff":
                    return 0, "some diff", ""
                elif sub == "rev-parse":
                    return 0, "commit_hash", ""
                elif sub == "checkout":
                    return 0, "", ""
                elif sub == "branch":
                    return 0, "agent/branch", ""
                elif sub == "add":
                    return 0, "", ""
            elif any("compileall" in a for a in args):
                return 0, "compile ok", ""
            elif any("pytest" in a for a in args):
                return 0, "pytest ok", ""
            elif any("ruff" in a for a in args):
                return 0, "ruff ok", ""
            elif any("mypy" in a for a in args):
                return 0, "mypy ok", ""
            elif any("check_invariants.py" in a for a in args):
                return 1, "Failed: invariants violated", ""
            elif cmd == "claude":
                if "--version" in args or "--help" in args:
                    return 0, "probe ok", ""
                return 0, "Final verdict: PASS\nMust fix before commit: None", ""
            elif cmd in ("agy", "antigravity"):
                return 0, "probe ok", ""
            return 0, "", ""
            
        mock_run_cmd.side_effect = side_effect
        
        self.orchestrator.dry_run = False
        self.orchestrator.allow_dirty = True
        self.orchestrator.agent_runs_parent = Path(self.temp_dir)
        
        self.orchestrator.config["checks"]["invariants"] = True
        self.orchestrator.config["limits"]["max_task_cycles"] = 1
        
        task_file = Path(self.temp_dir) / "dummy_task.md"
        task_file.write_text("# Dummy Task\n\nImplement something cool.", encoding="utf-8")
        
        res = self.orchestrator.execute_task(task_file)
        self.assertEqual(res, "STOP_INVARIANTS_FAILED")
        
        invariants_log_file = next(Path(self.temp_dir).rglob("check_invariants.txt"), None)
        self.assertIsNotNone(invariants_log_file)
        self.assertIn("Failed: invariants violated", invariants_log_file.read_text())
        
        claude_review_file = next(Path(self.temp_dir).rglob("claude_review.md"), None)
        self.assertIsNone(claude_review_file)

    @patch("tools.agent_orchestrator.orchestrate.run_cmd")
    def test_antigravity_fail_stops_for_human_review(self, mock_run_cmd):
        def side_effect(args, *arg, **kw):
            cmd = args[0]
            if cmd == "codex":
                return 0, "Codex finished successfully", ""
            elif cmd == "git":
                sub = args[1]
                if sub == "status":
                    return 0, " M file.py\n", ""
                elif sub == "diff":
                    return 0, "some diff", ""
            elif any(
                k in a
                for k in ("compileall", "pytest", "ruff", "mypy", "check_invariants.py")
                for a in args
            ):
                return 0, "check passed", ""
            elif cmd == "claude":
                if "--version" in args or "--help" in args:
                    return 0, "version ok", ""
                return 0, "Final verdict: PASS\nMust fix before commit: None", ""
            elif cmd in ("agy", "antigravity"):
                if "--version" in args or "--help" in args:
                    return 0, "version ok", ""
                return 0, "Final verdict: FAIL\nReasoning: Some issues found.", ""
            return 0, "", ""
            
        mock_run_cmd.side_effect = side_effect
        
        self.orchestrator.dry_run = False
        self.orchestrator.allow_dirty = True
        self.orchestrator.agent_runs_parent = Path(self.temp_dir)
        self.orchestrator.config["limits"]["max_task_cycles"] = 1
        self.orchestrator.config["review"] = {"allow_claude_override_antigravity": False}
        
        task_file = Path(self.temp_dir) / "dummy_task.md"
        task_file.write_text("# Dummy Task\n\nImplement something cool.", encoding="utf-8")
        
        res = self.orchestrator.execute_task(task_file)
        self.assertEqual(res, "STOP_ANTIGRAVITY_AUDIT_FAILED")

    def test_backlog_extraction_with_nested_bullets(self):
        backlog_content = """# Backlog
* [ ] Task: Task with details
  ## Goal
  Do some cool stuff.
  - bullet 1
  - bullet 2
* [ ] Task: Next Task
"""
        backlog_file = Path(self.temp_dir) / "backlog.md"
        backlog_file.write_text(backlog_content, encoding="utf-8")
        
        import re
        task_pattern = re.compile(r"^\s*[-\*]\s*\[\s*\]\s+(.*)$", re.MULTILINE)
        matches = list(task_pattern.finditer(backlog_content))
        
        self.assertEqual(len(matches), 2)
        match = matches[0]
        task_line_text = match.group(1).strip()
        self.assertEqual(task_line_text, "Task: Task with details")
        
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
        
        self.assertIn("# Task: Task with details", full_scratch_content)
        self.assertIn("## Goal", full_scratch_content)
        self.assertIn("- bullet 1", full_scratch_content)
        self.assertIn("- bullet 2", full_scratch_content)
        self.assertNotIn("Next Task", full_scratch_content)

if __name__ == "__main__":
    unittest.main()
