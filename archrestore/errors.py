"""Typed failures.  Each kind of damage has its own class so callers never
have to guess whether a problem is a bad manifest, a bad replica descriptor,
unusable evidence bytes, or merely missing coverage."""


class ArchError(Exception):
    """Base class for all typed failures."""


class ManifestError(ArchError):
    """The trusted handover manifest is malformed or internally inconsistent."""


class ReplicaError(ArchError):
    """A replica descriptor is structurally malformed at replica level."""


class EvidenceError(ArchError):
    """Page/dictionary bytes fail verification (length, digest, bounds)."""


class DecodeError(ArchError):
    """Bytes verified against a descriptor but do not decode canonically."""


class PageRejected(ArchError):
    """A page is not usable evidence.  ``reason`` is a stable short token."""

    def __init__(self, reason, detail):
        super().__init__("%s: %s" % (reason, detail))
        self.reason = reason
        self.detail = detail
