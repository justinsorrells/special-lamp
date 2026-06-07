import unittest
from pathlib import Path
import tempfile
import shutil
from tools.agent_orchestrator.orchestrate import (
    parse_simple_toml,
    slugify,
    Orchestrator,
    parse_changed_files,
    extract_added_lines,
    parse_must_fix,
    parse_per_file_diff
)

class TestAgentOrchestrator(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.config = {
            "agents": {
                "codex": {"command": "codex", "mode": "exec", "model": "gpt-5.5"},
                "claude": {"command": "claude", "mode": "print", "model": "opus-4.8"},
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
        res_allowed = self.orchestrator.check_forbidden_patterns(diff, changed_files, "allow editing contracts for this task")
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

if __name__ == "__main__":
    unittest.main()
