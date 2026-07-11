import copy
import importlib.util
import pathlib
import subprocess
import tempfile
import unittest
from unittest import mock

PATH = pathlib.Path(__file__).parents[1] / "capture-gres-driver-goldens.py"
SPEC = importlib.util.spec_from_file_location("capture_gres_driver_goldens", PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class RecaptureSafetyTest(unittest.TestCase):
    def test_every_run_has_a_default_absolute_timeout(self):
        with mock.patch.object(MODULE.subprocess, "run") as run:
            MODULE.run("bounded-command")
        self.assertEqual(run.call_args.kwargs["timeout"], MODULE.SUBPROCESS_TIMEOUT)

    def test_expected_capture_rejects_deletion_reordering_and_mutation(self):
        document = {
            "drivers": [
                {
                    "driver": "tokio-postgres",
                    "startup_parameters": {"client_encoding": "UTF8"},
                    "pgdog_backend_startup_parameters": {"application_name": "PgDog", "client_encoding": "utf-8"},
                    "pgdog_backend_set_batches": [],
                },
                {
                    "driver": "sqlx",
                    "startup_parameters": {"DateStyle": "ISO, MDY", "TimeZone": "UTC", "client_encoding": "UTF8", "extra_float_digits": "2"},
                    "pgdog_backend_startup_parameters": {"application_name": "PgDog", "client_encoding": "utf-8"},
                    "pgdog_backend_set_batches": ["SET \"datestyle\" TO 'ISO, MDY'", "SET \"extra_float_digits\" TO '2'", "SET \"timezone\" TO 'UTC'"],
                },
                {
                    "driver": "psycopg",
                    "startup_parameters": {},
                    "pgdog_backend_startup_parameters": {"application_name": "PgDog", "client_encoding": "utf-8"},
                    "pgdog_backend_set_batches": [],
                },
            ]
        }
        MODULE.assert_expected_capture(document)
        mutations = []
        deleted = copy.deepcopy(document)
        deleted["drivers"][1]["pgdog_backend_set_batches"].pop()
        mutations.append(deleted)
        reordered = copy.deepcopy(document)
        reordered["drivers"][1]["pgdog_backend_set_batches"].reverse()
        mutations.append(reordered)
        emptied = copy.deepcopy(document)
        emptied["drivers"][0]["startup_parameters"] = {}
        mutations.append(emptied)
        backend_mutation = copy.deepcopy(document)
        backend_mutation["drivers"][2]["pgdog_backend_startup_parameters"] = {}
        mutations.append(backend_mutation)
        driver_reorder = copy.deepcopy(document)
        driver_reorder["drivers"].reverse()
        mutations.append(driver_reorder)
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                with self.assertRaises(RuntimeError):
                    MODULE.assert_expected_capture(mutation)

    def test_config_failure_after_recorder_spawn_reaps_recorder(self):
        recorder = mock.Mock()
        recorder.poll.return_value = None
        recorder.wait.return_value = -9
        with (
            tempfile.TemporaryDirectory() as directory,
            mock.patch.object(MODULE, "start_recorder", return_value=recorder),
            mock.patch.object(MODULE, "pgdog_files", side_effect=OSError("private path")),
        ):
            with self.assertRaisesRegex(MODULE.CaptureLifecycleError, "primary=OSError") as raised:
                MODULE.pgdog_capture("sqlx", 15432, pathlib.Path(directory))
        recorder.kill.assert_called_once()
        recorder.wait.assert_called()
        self.assertNotIn("private path", str(raised.exception))

    def test_docker_start_timeout_attempts_container_removal_and_reaps_recorder(self):
        recorder = mock.Mock()
        recorder.poll.return_value = None
        recorder.wait.return_value = -9
        calls = []

        def fake_run(*args, **kwargs):
            calls.append(args)
            if args[:3] == ("docker", "run", "-d"):
                raise subprocess.TimeoutExpired(args, 1)
            return subprocess.CompletedProcess(args, 0)

        with (
            tempfile.TemporaryDirectory() as directory,
            mock.patch.object(MODULE, "start_recorder", return_value=recorder),
            mock.patch.object(MODULE, "pgdog_files", return_value=pathlib.Path(directory)),
            mock.patch.object(MODULE, "run", side_effect=fake_run),
        ):
            with self.assertRaisesRegex(MODULE.CaptureLifecycleError, "primary=TimeoutExpired"):
                MODULE.pgdog_capture("sqlx", 15432, pathlib.Path(directory))
        removals = [args for args in calls if args[:3] == ("docker", "rm", "-f")]
        self.assertGreaterEqual(len(removals), 2, "ambiguous docker run must be followed by removal")
        recorder.kill.assert_called_once()
        recorder.wait.assert_called()

    def test_docker_rm_timeout_does_not_skip_other_container_cleanup(self):
        calls = []
        pgdog_removals = 0

        def fake_run(*args, **kwargs):
            nonlocal pgdog_removals
            calls.append(args)
            if args[:3] == ("docker", "rm", "-f") and args[3] == MODULE.PGDOG_CONTAINER:
                pgdog_removals += 1
                raise subprocess.TimeoutExpired(args, 1)
            return subprocess.CompletedProcess(args, 0)

        provenance = {"postgres": {}, "pgdog": {}}
        with (
            mock.patch.object(MODULE, "verify_environment", return_value=provenance),
            mock.patch.object(MODULE, "run", side_effect=fake_run),
            mock.patch.object(MODULE, "wait_postgres", side_effect=RuntimeError("private primary")),
        ):
            with self.assertRaisesRegex(MODULE.CaptureLifecycleError, "primary=RuntimeError; cleanup=PgDog container:TimeoutExpired") as raised:
                MODULE.capture()
        removals = [args[3] for args in calls if args[:3] == ("docker", "rm", "-f")]
        self.assertIn(MODULE.PGDOG_CONTAINER, removals)
        self.assertIn(MODULE.POSTGRES_CONTAINER, removals)
        self.assertNotIn("private primary", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
