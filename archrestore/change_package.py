"""Format-name alias for change-package callers."""

from .materialize import *  # noqa: F401,F403
from .materialize import (  # noqa: F401
    CHANGE_PACKAGE_KIND,
    PackageFormatError,
    ingest_package,
    materialize_archive,
    normalise_operation,
    write_change_package,
)

ChangePackageError = PackageFormatError
apply_package = materialize_archive
parse_change_package = ingest_package
load_change_package = ingest_package
build_change_package = write_change_package
