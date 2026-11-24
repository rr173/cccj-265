"""Compatibility entry point for applying a JSONL change package."""

from .materialize import *  # noqa: F401,F403
from .materialize import (  # noqa: F401
    ApplyConflictError,
    ApplyError,
    ApplyFormatError,
    ApplyVerificationError,
    BaselineDigestError,
    BaselineDigestMismatch,
    ChangePackageFormatError,
    IdentityConflictError,
    MaterializeError,
    PackageFormatError,
    apply_change_package,
    apply_changes,
    apply_delta,
    apply_package,
    apply_patch,
    archive_content_digest,
    archive_digest_candidates,
    baseline_digest,
    materialize,
    materialize_archive,
)

apply = materialize_archive
run_apply = materialize_archive


def apply_changes(changes=None, baseline=None, out=None, *args, **kwargs):
    """Apply a package with package-first vocabulary.

    Both ``apply_changes(package, baseline, out)`` and the core
    ``materialize_archive(baseline, package, out)`` positional order are
    accepted; the core classifies existing file/directory arguments as a
    second guard.
    """
    if out is None and args:
        out = args[0]
    return materialize_archive(baseline=baseline, package=changes, out=out,
                               **kwargs)


def apply_package(package=None, baseline=None, out=None, *args, **kwargs):
    if out is None and args:
        out = args[0]
    return materialize_archive(baseline=baseline, package=package, out=out,
                               **kwargs)


apply_change_package = apply_package

__all__ = [name for name in globals() if not name.startswith("_")]


def _main():  # pragma: no cover
    import sys
    from .cli import main as cli_main
    argv = list(sys.argv[1:])
    if not argv or argv[0] not in ("apply", "materialize", "materialise",
                                   "apply-changes", "apply-package", "patch"):
        argv = ["apply"] + argv
    return cli_main(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_main())
