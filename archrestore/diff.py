"""Compatibility alias for :mod:`archrestore.compare`.

The comparison feature is implemented in :mod:`archrestore.compare` and the
CLI exposes both ``compare`` and ``diff`` subcommands.  This module simply
re-exports the public API under the neighbouring module name.
"""

from .compare import (  # noqa: F401
    DEFAULT_FAN_IN,
    DEFAULT_PROBLEM_LIMIT,
    DEFAULT_RUN_BYTES,
    DEFAULT_RUN_RECORDS,
    CompareError,
    CompareFailure,
    CompareReport,
    CompareSchemaError,
    CompareSpec,
    CompareSpecError,
    archive_diff,
    coerce_compare_spec,
    compare,
    compare_archives,
    diff_archives,
    iter_compare_events,
    load_compare_spec,
    parse_compare_spec,
    read_ledger,
    run_compare,
)

compare_two_archives = compare_archives
diff = compare_archives

__all__ = [
    "compare_archives",
    "compare_two_archives",
    "diff_archives",
    "run_compare",
    "compare",
    "diff",
    "archive_diff",
    "iter_compare_events",
    "read_ledger",
    "parse_compare_spec",
    "load_compare_spec",
    "coerce_compare_spec",
    "CompareSpec",
    "CompareReport",
    "CompareError",
    "CompareSpecError",
    "CompareSchemaError",
    "CompareFailure",
    "DEFAULT_RUN_RECORDS",
    "DEFAULT_RUN_BYTES",
    "DEFAULT_FAN_IN",
    "DEFAULT_PROBLEM_LIMIT",
]
