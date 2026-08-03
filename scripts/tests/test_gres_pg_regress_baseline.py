#!/usr/bin/env python3
"""Behavioral tests for gres-pg-regress-baseline.py."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).parents[1] / "gres-pg-regress-baseline.py"
RUNNER = Path(__file__).parents[1] / "gres-pg-regress.sh"


def diff_section(
    root: Path,
    test: str,
    *,
    expected: str | None = None,
    hunks: int = 1,
    marker: str = "value",
) -> str:
    expected = expected or f"{test}.out"
    lines = [
        f"diff -U3 {root}/source/expected/{expected} {root}/build/results/{test}.out",
        f"--- {root}/source/expected/{expected}\t2026-08-01 01:02:03 +0000",
        f"+++ {root}/build/results/{test}.out\t2026-08-02 04:05:06 +0000",
    ]
    for number in range(hunks):
        lines.extend(
            (
                f"@@ -{number + 1} +{number + 1} @@",
                f"-from {root}/source/{marker}-{number}",
                f"+from {root}/build/{marker}-{number}",
            )
        )
    return "\n".join(lines) + "\n"


class Dataset:
    def __init__(
        self,
        root: Path,
        tests: list[str],
        failures: dict[str, tuple[int, str]],
        expected: dict[str, str] | None = None,
    ) -> None:
        self.root = root
        self.schedule = root / "parallel_schedule"
        self.tap = root / "regression.out"
        self.diff = root / "regression.diffs"
        self.source_root = root / "source"
        self.build_root = root / "build"
        self.schedule.write_text("test: " + " ".join(tests) + "\n", encoding="utf-8")
        tap_lines = []
        for number, test in enumerate(tests, 1):
            status = "not ok" if test in failures else "ok"
            tap_lines.append(f"{status} {number} + {test} 1 ms")
        self.tap.write_text("\n".join(tap_lines) + "\n", encoding="utf-8")
        self.diff.write_text(
            "".join(
                diff_section(
                    root,
                    test,
                    hunks=failures[test][0],
                    marker=failures[test][1],
                    expected=(expected or {}).get(test),
                )
                for test in tests
                if test in failures
            ),
            encoding="utf-8",
        )

    def arguments(self) -> list[str]:
        return [
            "--postgres-tag",
            "REL_18_4",
            "--schedule",
            str(self.schedule),
            "--tap",
            str(self.tap),
            "--diff",
            str(self.diff),
            "--source-root",
            str(self.source_root),
            "--build-root",
            str(self.build_root),
        ]


def run(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *arguments],
        check=False,
        capture_output=True,
        text=True,
    )


class BaselineTest(unittest.TestCase):
    def test_runner_checks_serial_baseline_from_retained_command_log(self) -> None:
        runner = RUNNER.read_text(encoding="utf-8")
        start = runner.index("check_serial_baseline()")
        end = runner.index("\n}\n", start)
        function = runner[start:end]

        self.assertIn('--tap "${output}/command.log"', function)
        self.assertNotIn("regression.out", function)

    def test_hunk_content_that_looks_like_a_header_is_hashed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset = Dataset(root, ["comments"], {"comments": (1, "unused")})
            header = (
                f"diff -U3 {root}/source/expected/comments.out "
                f"{root}/build/results/comments.out\n"
                f"--- {root}/source/expected/comments.out\told\n"
                f"+++ {root}/build/results/comments.out\tnew\n"
                "@@ -1 +1 @@\n"
            )
            dataset.diff.write_text(
                header + "--- removed SQL comment\n+++ added plus text\n",
                encoding="utf-8",
            )
            first = run("generate", *dataset.arguments())
            self.assertEqual(first.returncode, 0, first.stderr)
            first_hash = json.loads(first.stdout)["failures"]["comments"]["sha256"]

            dataset.diff.write_text(
                header + "--- different SQL comment\n+++ added plus text\n",
                encoding="utf-8",
            )
            second = run("generate", *dataset.arguments())
            self.assertEqual(second.returncode, 0, second.stderr)
            second_hash = json.loads(second.stdout)["failures"]["comments"]["sha256"]
            self.assertNotEqual(first_hash, second_hash)

    def test_generate_is_stable_across_roots_and_timestamps(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first_root = root / "first"
            second_root = root / "second"
            first_root.mkdir()
            second_root.mkdir()
            first = Dataset(
                first_root,
                ["float4"],
                {"float4": (1, "same")},
                {"float4": "float4-mingw32.out"},
            )
            second = Dataset(
                second_root,
                ["float4"],
                {"float4": (1, "same")},
                {"float4": "float4-mingw32.out"},
            )

            first_result = run("generate", *first.arguments())
            second_result = run("generate", *second.arguments())

            self.assertEqual(first_result.returncode, 0, first_result.stderr)
            self.assertEqual(second_result.returncode, 0, second_result.stderr)
            first_document = json.loads(first_result.stdout)
            second_document = json.loads(second_result.stdout)
            self.assertEqual(first_document, second_document)
            self.assertEqual(
                first_document["failures"]["float4"]["expected"],
                "float4-mingw32.out",
            )
            self.assertEqual(first_document["failures"]["float4"]["hunks"], 1)
            self.assertEqual(first_document["failures"]["float4"]["changed_lines"], 2)

    def test_check_accepts_all_pass_command_log_without_diff(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            old_root = root / "old"
            current_root = root / "current"
            old_root.mkdir()
            current_root.mkdir()
            old = Dataset(old_root, ["smoke"], {"smoke": (1, "old")})
            current = Dataset(current_root, ["smoke"], {})
            command_log = current_root / "command.log"
            current.tap.rename(command_log)
            current.tap = command_log
            current.diff.unlink()
            baseline = root / "baseline.json"
            actual = root / "actual.json"
            self.assertEqual(
                run("seed", *old.arguments(), "--baseline", str(baseline)).returncode,
                0,
            )

            checked = run(
                "check",
                *current.arguments(),
                "--baseline",
                str(baseline),
                "--actual-output",
                str(actual),
            )

            self.assertEqual(checked.returncode, 1, checked.stderr)
            self.assertIn("removed: smoke", checked.stderr)
            document = json.loads(actual.read_text(encoding="utf-8"))
            self.assertEqual(document["passed"], 1)
            self.assertEqual(document["failures"], {})

    def test_check_reports_every_failure_classification(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            old_root = root / "old"
            new_root = root / "new"
            old_root.mkdir()
            new_root.mkdir()
            tests = ["exact", "removed", "worse", "changed", "improve", "new"]
            old = Dataset(
                old_root,
                tests,
                {
                    "exact": (1, "exact"),
                    "removed": (1, "removed"),
                    "worse": (1, "worse"),
                    "changed": (1, "before"),
                    "improve": (2, "improve"),
                },
            )
            current = Dataset(
                new_root,
                tests,
                {
                    "exact": (1, "exact"),
                    "worse": (2, "worse"),
                    "changed": (1, "after"),
                    "improve": (1, "improve"),
                    "new": (1, "new"),
                },
            )
            baseline = root / "baseline.json"
            summary = root / "summary.md"
            seeded = run("seed", *old.arguments(), "--baseline", str(baseline))
            self.assertEqual(seeded.returncode, 0, seeded.stderr)

            checked = run(
                "check",
                *current.arguments(),
                "--baseline",
                str(baseline),
                "--summary-output",
                str(summary),
            )

            self.assertEqual(checked.returncode, 1)
            for expected in (
                "new: new",
                "removed: removed",
                "worsened: worse",
                "same-count-different: changed",
                "improved: improve",
                "exact: 1 tests",
            ):
                self.assertIn(expected, checked.stderr)
            summary_text = summary.read_text(encoding="utf-8")
            self.assertIn("Passed: **1 / 6**", summary_text)
            self.assertIn("Baseline worsened: **1**", summary_text)
            self.assertIn("| `worse` | 4 | 2 |", summary_text)

    def test_update_only_accepts_a_strictly_smaller_surface(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            old_root = root / "old"
            new_root = root / "new"
            old_root.mkdir()
            new_root.mkdir()
            tests = ["improve", "removed", "exact"]
            old = Dataset(
                old_root,
                tests,
                {"improve": (2, "improve"), "removed": (1, "gone"), "exact": (1, "same")},
            )
            current = Dataset(
                new_root,
                tests,
                {"improve": (1, "improve"), "exact": (1, "same")},
            )
            baseline = root / "baseline.json"
            self.assertEqual(
                run("seed", *old.arguments(), "--baseline", str(baseline)).returncode,
                0,
            )

            updated = run("update", *current.arguments(), "--baseline", str(baseline))

            self.assertEqual(updated.returncode, 0, updated.stderr)
            document = json.loads(baseline.read_text(encoding="utf-8"))
            self.assertEqual(list(document["failures"]), ["exact", "improve"])
            checked = run("check", *current.arguments(), "--baseline", str(baseline))
            self.assertEqual(checked.returncode, 0, checked.stderr)
            no_op = run("update", *current.arguments(), "--baseline", str(baseline))
            self.assertEqual(no_op.returncode, 2)
            self.assertIn("no improved or removed failures", no_op.stderr)

    def test_rejects_incomplete_tap_and_seed_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset_root = root / "data"
            dataset_root.mkdir()
            dataset = Dataset(dataset_root, ["one", "two"], {})
            dataset.tap.write_text("ok 1 one 1 ms\n", encoding="utf-8")

            incomplete = run("generate", *dataset.arguments())

            self.assertEqual(incomplete.returncode, 2)
            self.assertIn("membership/order differs", incomplete.stderr)
            dataset.tap.write_text("ok 1 one 1 ms\nok 2 two 1 ms\n", encoding="utf-8")
            baseline = root / "baseline.json"
            self.assertEqual(
                run("seed", *dataset.arguments(), "--baseline", str(baseline)).returncode,
                0,
            )
            overwrite = run("seed", *dataset.arguments(), "--baseline", str(baseline))
            self.assertEqual(overwrite.returncode, 2)
            self.assertIn("refusing to overwrite", overwrite.stderr)


if __name__ == "__main__":
    unittest.main()
