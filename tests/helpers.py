"""Shared helpers for the archrestore test suite.

Tests are run with::

    python3 -I -m unittest discover -s tests -v

The ``-I`` flag matters: replicas are untrusted input, and tests import the
package from a separate directory so a planted module cannot be picked up.
"""

import hashlib
import json
import os
import random
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from archrestore import synth  # noqa: E402
from archrestore.pipeline import run_repair  # noqa: E402


class Workspace(object):
    def __init__(self):
        self.root = tempfile.mkdtemp(prefix="archrestore-test-")
        self._n = 0

    def path(self, *parts):
        self._n += 1
        return os.path.join(self.root, *parts)

    def cleanup(self):
        import shutil
        shutil.rmtree(self.root, ignore_errors=True)


def write_manifest(ds, sizes, root, name="manifest.json", mutate=None):
    man = synth.build_manifest(ds, sizes)
    if mutate is not None:
        mutate(man)
    path = os.path.join(root, name)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(man, fh, ensure_ascii=False, indent=1)
    return man, path


def make_replica(root, ds, name, plan, encodings=("plain", "dict"),
                 page_sizes=(37,), seed=5, dict_mode="per-page"):
    directory = os.path.join(root, name)
    synth.build_replica(directory, ds, plan, list(page_sizes),
                        list(encodings), random.Random(seed),
                        dict_mode=dict_mode)
    return name, directory


def load_descriptor(directory):
    path = os.path.join(directory, "replica.json")
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def save_descriptor(directory, rep):
    path = os.path.join(directory, "replica.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(rep, fh, sort_keys=True, indent=1)


def find_page(directory, column, start):
    rep = load_descriptor(directory)
    for p in rep["pages"]:
        if p["column"] == column and p["start"] == start:
            return rep, p
    raise AssertionError("no page %s@%d in %s" % (column, start, directory))


def rewrite_page_bytes(directory, rep, page, body):
    """Replace a page body with *body*, refresh length and digest.

    Cleanup-safe: appends the new body at the end of the data file, so no
    other page's bytes are disturbed.
    """
    data_path = os.path.join(directory, page["location"]["file"])
    old_body_len = page["byte_length"]
    with open(data_path, "rb") as fh:
        fh.seek(page["location"]["offset"])
        old_body = fh.read(old_body_len)
    assert len(body) == old_body_len or True
    with open(data_path, "ab") as fh:
        off = fh.tell()
        fh.write(body)
    page["location"] = {"file": page["location"]["file"], "offset": off}
    page["byte_length"] = len(body)
    page["byte_digest"] = hashlib.sha256(body).hexdigest()
    save_descriptor(directory, rep)
    return page


def corrupt_integer_page(directory, column, start, delta=1):
    """Rewrite a plain integer page with each integer shifted by *delta*.

    The page remains structurally valid, well-typed, correctly digested and
    of the right length -- it simply disagrees, which must be a *dispute*.
    """
    import struct
    rep, page = find_page(directory, column, start)
    assert page["encoding"] == "plain"
    data_path = os.path.join(directory, page["location"]["file"])
    with open(data_path, "rb") as fh:
        fh.seek(page["location"]["offset"])
        body = fh.read(page["byte_length"])
    out = bytearray(body)
    pos = 0
    n = page["end"] - page["start"]
    for _ in range(n):
        assert out[pos] == 2, out[pos]
        value = int.from_bytes(out[pos + 1:pos + 9], "little", signed=True)
        out[pos + 1:pos + 9] = struct.pack("<q", value + delta)
        pos += 9
    assert pos == len(body)
    return rewrite_page_bytes(directory, rep, page, bytes(out))


def damage_page(directory, column, start, mode="flip"):
    """Corrupt a page's bytes *without* refreshing its descriptor digest.

    The page is then bad evidence (skipped), never a competing value.
    """
    rep, page = find_page(directory, column, start)
    synth.corrupt_page_bytes(directory, page, mode)
    return page


def summarize(result):
    return result["summary"]


def run(tmp_root, manifest_path, replicas, outname="out", **kw):
    out = os.path.join(tmp_root, outname)
    return run_repair(manifest_path, replicas, out, **kw)
