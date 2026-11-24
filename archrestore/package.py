"""Package vocabulary alias for materialization."""

from .change_package import *  # noqa: F401,F403
from .materialize import (  # noqa: F401
    PackageFormatError,
    ingest_package,
    materialize_archive,
    write_change_package,
)

apply_package = materialize_archive
load_package = ingest_package
parse_package = ingest_package
