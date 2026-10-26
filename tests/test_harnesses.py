"""Check clanker-analytics' history paths against a vendored copy of the agent spec sheet.

tests/harnesses.toml is harnesses.toml from https://github.com/wakamex/agent-spec-sheet at the
commit below, copied unchanged. To pick up a change, copy the new file and update the commit.
"""

import os
import re
import tomllib
from pathlib import Path

import clanker_analytics.main as main_mod

from support import Case

HARNESSES = Path(__file__).with_name("harnesses.toml")
HARNESSES_COMMIT = "7d51d15"
READER = "clanker-analytics"

# Spec sheet entry -> the tool its usage is reported as.
TOOLS = {
    "claude": "Claude Code",
    "codex": "Codex",
    "agy": "Agy",
    "gemini": "Gemini",
    "zcode": main_mod.ZCODE_TOOL,
    "muse": main_mod.MUSE_TOOL,
}

# Session files listed in the sheet that clanker-analytics does not discover on purpose.
NOT_DISCOVERED = {
    # A compact copy of transcript_full.jsonl; reading both would count each step twice.
    ("agy", "brain/<id>/.system_generated/logs/transcript.jsonl"),
    # Read for each transcript it belongs to, as the source of models and usage.
    ("agy", "conversations/<id>.db"),
    # An index of sessions, with no usage.
    ("muse", "session-index.db"),
}

HOME_TEMPLATE = re.compile(r"^\$\{(?P<var>[A-Z_]+):-(?P<default>[^}]*)\}(?P<rest>.*)$")


def _example(template: str) -> str:
    """A concrete relative path matching a session template."""
    path = re.sub(r"<[^>]+>", "x1", template)
    path = path.replace("YYYY", "2026").replace("MM", "10").replace("DD", "26")
    return path.replace("*", "a")


class TestHarnesses(Case):
    def setUp(self):
        super().setUp()
        self.entries = {name: entry for name, entry in tomllib.loads(HARNESSES.read_text()).items()
                        if isinstance(entry, dict)}

    def test_reads_exactly_the_harnesses_that_list_it(self):
        listed = {name for name, entry in self.entries.items() if READER in entry.get("readers", [])}
        assert listed == set(TOOLS)

    def test_finds_each_listed_session_file_with_its_home_variable_unset_and_set(self):
        for name, tool in TOOLS.items():
            entry = self.entries[name]
            match = HOME_TEMPLATE.match(entry["home"])
            for variable_set in ((False, True) if match else (False,)):
                with self.subTest(harness=name, variable_set=variable_set):
                    self._check(name, tool, entry, match, variable_set)

    def _check(self, name, tool, entry, match, variable_set):
        root = self.tmp_path / f"{name}-{variable_set}"
        home = root / "home"
        home.mkdir(parents=True)
        self.monkeypatch.setattr(main_mod, "HOME", home.as_posix())
        self.monkeypatch.setattr(main_mod, "_WSL_HOMES", [])
        self.monkeypatch.setenv("XDG_CONFIG_HOME", str(root / "config"))
        for variable in ("CLAUDE_CONFIG_DIR", "CODEX_HOME", "GEMINI_CLI_HOME", "XDG_DATA_HOME"):
            self.monkeypatch.delenv(variable, raising=False)
        if match:
            if variable_set:
                base = root / "elsewhere"
                self.monkeypatch.setenv(match["var"], str(base))
            else:
                base = Path(match["default"].replace("~", home.as_posix(), 1))
            harness_home = Path(f"{base}{match['rest']}")
        else:
            harness_home = Path(entry["home"].replace("~", home.as_posix(), 1))

        expected = {}
        for template in entry["sessions"]:
            if (name, template) in NOT_DISCOVERED:
                continue
            path = harness_home / _example(template)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("{}\n")
            expected[path.as_posix()] = tool
        self.monkeypatch.setattr(main_mod, "SOURCE_TREES", [
            tree for tree in main_mod._home_trees() if tree[0] == tool])

        files, _, _ = main_mod.scan_source_files()

        assert expected and {path: files[path].tool for path in expected if path in files} == expected
