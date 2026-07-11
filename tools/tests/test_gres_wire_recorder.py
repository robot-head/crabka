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
        self.assertEqual(MODULE.safe_set_batch(b"SET client_encoding = 'UTF8';\0"), "SET client_encoding = 'UTF8';")
        self.assertIsNone(MODULE.safe_set_batch(b"SELECT 'private payload'\0"))
        self.assertIsNone(MODULE.safe_set_batch(b"SET x = 1; SELECT 2\0"))

    def test_decoder_skips_ssl_request_before_startup(self):
        decoder = MODULE.Decoder()
        ssl_request = struct.pack("!II", 8, 80877103)
        decoder.feed(ssl_request + startup([("client_encoding", "UTF8")]))
        self.assertEqual(decoder.startup, {"client_encoding": "UTF8"})


if __name__ == "__main__":
    unittest.main()
