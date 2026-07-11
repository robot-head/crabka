import copy
import importlib.util
import pathlib
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


if __name__ == "__main__":
    unittest.main()
