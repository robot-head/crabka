import importlib.util
import pathlib
import struct
import unittest

PATH = pathlib.Path(__file__).parents[1] / "gres-wire-recorder.py"
SPEC = importlib.util.spec_from_file_location("gres_wire_recorder", PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def startup(fields):
    body = struct.pack("!I", 196608)
    body += b"".join(key.encode() + b"\0" + value.encode() + b"\0" for key, value in fields)
    body += b"\0"
    return struct.pack("!I", len(body) + 4) + body


class RecorderSafetyTest(unittest.TestCase):
    def test_identity_is_dropped_and_settings_are_allowlisted(self):
        packet = startup([("user", "secret-user"), ("database", "secret-db"), ("client_encoding", "UTF8")])
        self.assertEqual(MODULE.startup_parameters(packet), {"client_encoding": "UTF8"})

    def test_unknown_startup_key_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "unexpected startup key"):
            MODULE.startup_parameters(startup([("search_path", "private")]))

    def test_only_pure_set_batches_are_retained(self):
        self.assertEqual(
            MODULE.safe_set_batch(b"SET datestyle TO 'ISO, MDY'\0"),
            "SET \"datestyle\" TO 'ISO, MDY'",
        )
        self.assertIsNone(MODULE.safe_set_batch(b"SELECT 'private payload'\0"))
        with self.assertRaises(ValueError):
            MODULE.safe_set_batch(b"SET x = 1; SELECT 2\0")

    def test_set_capture_is_exact_allowlisted_and_canonically_rendered(self):
        self.assertEqual(
            MODULE.safe_set_batch(b"  set \"datestyle\" to 'ISO, MDY'  \0"),
            "SET \"datestyle\" TO 'ISO, MDY'",
        )
        self.assertEqual(
            MODULE.safe_set_batch(b"SET extra_float_digits TO '2'\0"),
            "SET \"extra_float_digits\" TO '2'",
        )
        self.assertEqual(
            MODULE.safe_set_batch(b"SET timezone TO 'UTC'\0"),
            "SET \"timezone\" TO 'UTC'",
        )
        for unsafe in (
            b"SET ROLE private_role\0",
            b"SET search_path TO 'private_token'\0",
            b"SET timezone TO 'private_token'\0",
            b"SET timezone TO 'UTC'; SELECT 1\0",
            b"SET timezone TO 'UTC' /* private */\0",
            b"SET timezone TO 'UTC'; SET timezone TO 'UTC'\0",
            b"SET timezone TO 'UTC;private'\0",
        ):
            with self.subTest(unsafe=unsafe):
                with self.assertRaises(ValueError):
                    MODULE.safe_set_batch(unsafe)

    def test_duplicate_and_malformed_startup_fields_are_rejected(self):
        duplicate = startup([
            ("user", "identity"),
            ("client_encoding", "UTF8"),
            ("client_encoding", "UTF8"),
        ])
        with self.assertRaisesRegex(ValueError, "duplicate startup key"):
            MODULE.startup_parameters(duplicate)
        malformed = startup([("user", "identity")]) + b"\0"
        malformed = struct.pack("!I", len(malformed)) + malformed[4:]
        with self.assertRaises(ValueError):
            MODULE.startup_parameters(malformed)

    def test_startup_values_are_exactly_allowlisted(self):
        for key, value in (
            ("DateStyle", "private"),
            ("TimeZone", "private"),
            ("client_encoding", "private"),
            ("extra_float_digits", "3"),
            ("application_name", "private"),
            ("options", "-c password=private"),
        ):
            with self.subTest(key=key):
                with self.assertRaises(ValueError):
                    MODULE.startup_parameters(startup([("user", "identity"), (key, value)]))

    def test_record_has_an_absolute_accept_deadline(self):
        with self.assertRaises((TimeoutError, OSError)):
            MODULE.record(("127.0.0.1", 0), ("127.0.0.1", 1), deadline_seconds=0.01)

    def test_decoder_skips_ssl_request_before_startup(self):
        decoder = MODULE.Decoder()
        ssl_request = struct.pack("!II", 8, 80877103)
        decoder.feed(
            ssl_request
            + startup(
                [("user", "identity"), ("database", "identity"), ("client_encoding", "UTF8")]
            )
        )
        self.assertEqual(decoder.startup, {"client_encoding": "UTF8"})


if __name__ == "__main__":
    unittest.main()
