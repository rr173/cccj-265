"""archrestore -- offline repair of a logical tabular dataset from
unreliable physical replicas, driven by a trusted handover manifest.

Quick start::

    python3 -m archrestore restore --manifest manifest.json \\
        --replica r1=dir1 --replica r2=dir2 --out restored/

See FORMAT.md for the normative format and repair contract, and
docs/CONTRACT.md for the precise acceptance rules.
"""

__version__ = "1.0.0"

from .errors import (  # noqa: F401
    ArchError,
    ManifestError,
    ReplicaError,
    EvidenceError,
    DecodeError,
    PageRejected,
)
from .format import (  # noqa: F401
    FORMAT_NAME,
    FORMAT_VERSION,
    MANIFEST_KIND,
    REPLICA_KIND,
    TYPE_STRING,
    TYPE_INTEGER,
    ALL_TYPES,
    Column,
    Slot,
    Manifest,
    PageDesc,
    Replica,
    encode_value,
    canonical_text,
    sha256_hex,
    SlotHasher,
    loads_strict,
    load_json,
    parse_manifest,
    parse_replica,
)
