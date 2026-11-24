"""Canonical encoding, digest and format-validation tests."""

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from helpers import Workspace, ROOT  # noqa: E402

from archrestore import format as fmt  # noqa: E402
from archrestore.errors import ManifestError, ReplicaError  # noqa: E402


class CanonicalValueTests(unittest.TestCase):
    def test_null_empty_and_string_are_distinct(self):
        self.assertNotEqual(fmt.encode_value(None, fmt.TYPE_STRING),
                            fmt.encode_value("", fmt.TYPE_STRING))
        self.assertNotEqual(fmt.encode_value("", fmt.TYPE_STRING),
                            fmt.encode_value(None, fmt.TYPE_INTEGER))
        self.assertEqual(fmt.encode_value(None, fmt.TYPE_STRING), b"\x00")
        self.assertEqual(fmt.encode_value("", fmt.TYPE_STRING),
                         b"\x01\x00\x00\x00\x00")

    def test_string_one_is_not_integer_one(self):
        self.assertNotEqual(fmt.encode_value("1", fmt.TYPE_STRING),
                            fmt.encode_value(1, fmt.TYPE_INTEGER))
        with self.assertRaises(TypeError):
            fmt.encode_value("1", fmt.TYPE_INTEGER)
        with self.assertRaises(TypeError):
            fmt.encode_value(1, fmt.TYPE_STRING)

    def test_integer_roundtrip_extremes(self):
        for n in (0, 1, -1, (1 << 63) - 1, -(1 << 63), 123456789012345):
            raw = fmt.encode_value(n, fmt.TYPE_INTEGER)
            self.assertEqual(len(raw), 9)
            self.assertEqual(fmt.decode_value_into(raw, fmt.TYPE_INTEGER), n)
        with self.assertRaises(TypeError):
            fmt.encode_value(1 << 63, fmt.TYPE_INTEGER)
        with self.assertRaises(TypeError):
            fmt.encode_value(-(1 << 63) - 1, fmt.TYPE_INTEGER)

    def test_bool_is_not_integer(self):
        with self.assertRaises(TypeError):
            fmt.encode_value(True, fmt.TYPE_INTEGER)

    def test_string_roundtrip_unicode_and_specials(self):
        for s in ("", "plain", "héllo", "行-é-世界", "\u0000\u001f", "a\"b\\c",
                  "😀" * 100):
            raw = fmt.encode_value(s, fmt.TYPE_STRING)
            self.assertEqual(fmt.decode_value_into(raw, fmt.TYPE_STRING), s)
            self.assertEqual(fmt.canonical_text(s, fmt.TYPE_STRING),
                             json.dumps(s, ensure_ascii=False))

    def test_surrogates_rejected(self):
        with self.assertRaises(TypeError):
            fmt.encode_value("\ud800", fmt.TYPE_STRING)

    def test_canonical_text_of_null_and_integer(self):
        self.assertEqual(fmt.canonical_text(None, fmt.TYPE_INTEGER), "null")
        self.assertEqual(fmt.canonical_text(-17, fmt.TYPE_INTEGER), "-17")

    def test_decode_rejects_trailing_and_bad_tags(self):
        with self.assertRaises(fmt.DecodeError):
            fmt.decode_value_into(b"\x00\x00", fmt.TYPE_STRING)
        with self.assertRaises(fmt.DecodeError):
            fmt.decode_value_into(b"\x07", fmt.TYPE_STRING)
        with self.assertRaises(fmt.DecodeError):
            fmt.decode_value_into(b"\x01\x05\x00\x00\x00abc", fmt.TYPE_STRING)

    def test_slot_digest_is_sensitive_to_every_part(self):
        base = fmt.compute_slot_digest("c", fmt.TYPE_STRING, 0, 2, ["a", "b"])
        variants = [
            fmt.compute_slot_digest("c", fmt.TYPE_STRING, 0, 2, ["b", "a"]),
            fmt.compute_slot_digest("c", fmt.TYPE_STRING, 0, 2, ["a", None]),
            fmt.compute_slot_digest("c", fmt.TYPE_STRING, 1, 3, ["a", "b"]),
            fmt.compute_slot_digest("d", fmt.TYPE_STRING, 0, 2, ["a", "b"]),
            fmt.compute_slot_digest("c", fmt.TYPE_INTEGER, 0, 2, [97, 98]),
        ]
        self.assertNotIn(base, variants)
        self.assertEqual(len(set(variants + [base])), 6)


class ManifestValidationTests(unittest.TestCase):
    def base(self):
        return {
            "archrestore_format": {"name": fmt.MANIFEST_KIND, "version": 1},
            "row_count": 10,
            "columns": [{"name": "a", "type": "string"},
                        {"name": "b", "type": "integer"}],
            "slots": [
                {"column": "a", "slot": 0, "start": 0, "end": 10,
                 "digest": "0" * 64},
                {"column": "b", "slot": 0, "start": 0, "end": 10,
                 "digest": "1" * 64},
            ],
        }

    def test_valid(self):
        m = fmt.parse_manifest(self.base())
        self.assertEqual(m.row_count, 10)
        self.assertEqual(len(m.slots), 2)

    def test_gap_is_bad_manifest(self):
        obj = self.base()
        obj["slots"][0]["end"] = 9
        with self.assertRaises(ManifestError):
            fmt.parse_manifest(obj)

    def test_overlap_is_bad_manifest(self):
        obj = self.base()
        obj["slots"].append({"column": "a", "slot": 1, "start": 9, "end": 10,
                             "digest": "2" * 64})
        with self.assertRaises(ManifestError):
            fmt.parse_manifest(obj)

    def test_duplicate_slot_id_is_bad_manifest(self):
        obj = self.base()
        obj["slots"].append({"column": "b", "slot": 0, "start": 9, "end": 10,
                             "digest": "2" * 64})
        with self.assertRaises(ManifestError):
            fmt.parse_manifest(obj)

    def test_unknown_type_is_bad_manifest(self):
        obj = self.base()
        obj["columns"][0]["type"] = "float"
        with self.assertRaises(ManifestError):
            fmt.parse_manifest(obj)

    def test_unknown_field_is_bad_manifest(self):
        obj = self.base()
        obj["extra"] = 1
        with self.assertRaises(ManifestError):
            fmt.parse_manifest(obj)

    def test_duplicate_column_name_is_bad_manifest(self):
        obj = self.base()
        obj["columns"][1]["name"] = "a"
        with self.assertRaises(ManifestError):
            fmt.parse_manifest(obj)

    def test_bad_digest_is_bad_manifest(self):
        obj = self.base()
        obj["slots"][0]["digest"] = "Z" * 64
        with self.assertRaises(ManifestError):
            fmt.parse_manifest(obj)

    def test_zero_rows_ok_but_slots_forbidden(self):
        obj = self.base()
        obj["row_count"] = 0
        obj["slots"] = []
        self.assertEqual(fmt.parse_manifest(obj).row_count, 0)
        obj["slots"] = [{"column": "a", "slot": 0, "start": 0, "end": 0,
                         "digest": "0" * 64}]
        with self.assertRaises(ManifestError):
            fmt.parse_manifest(obj)


class ReplicaValidationTests(unittest.TestCase):
    def setUp(self):
        self.manifest = fmt.parse_manifest({
            "archrestore_format": {"name": fmt.MANIFEST_KIND, "version": 1},
            "row_count": 5,
            "columns": [{"name": "a", "type": "string"}],
            "slots": [{"column": "a", "slot": 0, "start": 0, "end": 5,
                       "digest": "0" * 64}],
        })

    def page(self, **over):
        p = {"page": 0, "column": "a", "start": 0, "end": 5,
             "encoding": "plain", "byte_length": 9,
             "byte_digest": "a" * 64,
             "location": {"file": "data.bin", "offset": 0}}
        p.update(over)
        return p

    def test_valid_replica(self):
        rep, errors = fmt.parse_replica({
            "archrestore_format": {"name": fmt.REPLICA_KIND, "version": 1},
            "replica_id": "r", "pages": [self.page()],
        }, self.manifest)
        self.assertEqual(errors, [])
        self.assertEqual(rep.pages[0].column_type, "string")

    def test_unknown_column_is_page_error(self):
        rep, errors = fmt.parse_replica({
            "archrestore_format": {"name": fmt.REPLICA_KIND, "version": 1},
            "replica_id": "r", "pages": [self.page(column="zzz")],
        }, self.manifest)
        self.assertEqual(len(errors), 1)

    def test_path_escape_is_page_error(self):
        _rep, errors = fmt.parse_replica({
            "archrestore_format": {"name": fmt.REPLICA_KIND, "version": 1},
            "replica_id": "r",
            "pages": [self.page(location={"file": "../x", "offset": 0})],
        }, self.manifest)
        self.assertEqual(len(errors), 1)

    def test_dict_page_requires_dict_fields(self):
        _rep, errors = fmt.parse_replica({
            "archrestore_format": {"name": fmt.REPLICA_KIND, "version": 1},
            "replica_id": "r", "pages": [self.page(encoding="dict")],
        }, self.manifest)
        self.assertTrue(any("dict_digest" in e["detail"] for e in errors))

    def test_plain_page_rejects_dict_fields(self):
        _rep, errors = fmt.parse_replica({
            "archrestore_format": {"name": fmt.REPLICA_KIND, "version": 1},
            "replica_id": "r",
            "pages": [self.page(dict_id="d", dict_digest="b" * 64,
                                dict_byte_length=10,
                                dict_location={"file": "d", "offset": 0})],
        }, self.manifest)
        self.assertEqual(len(errors), 1)

    def test_duplicate_page_ids_is_page_error(self):
        _rep, errors = fmt.parse_replica({
            "archrestore_format": {"name": fmt.REPLICA_KIND, "version": 1},
            "replica_id": "r",
            "pages": [self.page(), self.page()],
        }, self.manifest)
        self.assertEqual(len(errors), 1)


class StrictJsonTests(unittest.TestCase):
    def test_duplicate_keys_rejected(self):
        with self.assertRaises(ValueError):
            fmt.loads_strict('{"a": 1, "a": 2}')

    def test_nan_rejected(self):
        with self.assertRaises(ValueError):
            fmt.loads_strict('{"a": NaN}')


if __name__ == "__main__":
    unittest.main()
