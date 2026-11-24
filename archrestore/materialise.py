"""British-spelling alias of :mod:`archrestore.materialize`."""

from .materialize import *  # noqa: F401,F403
from .materialize import (  # noqa: F401
    BaselineDigestError,
    IdentityConflictError,
    MaterializeError as MaterialiseError,
    MaterializeFailure as MaterialiseFailure,
    MaterializeVerificationError as MaterialiseVerificationError,
    PackageFormatError,
    archive_content_digest,
    materialize,
    materialize_archive,
)

materialise_archive = materialize_archive
materialise = materialize_archive
apply_change_package = materialize_archive

BaselineDigestMismatch = BaselineDigestError
IdentityConflict = IdentityConflictError
PackageFormat = PackageFormatError

__all__ = [name for name in globals() if not name.startswith("_")]


if __name__ == "__main__":  # pragma: no cover
    import sys
    from .cli import main as cli_main
    argv = list(sys.argv[1:])
    if not argv or argv[0] not in ("materialise", "materialize", "apply"):
        argv = ["materialise"] + argv
    raise SystemExit(cli_main(argv))
