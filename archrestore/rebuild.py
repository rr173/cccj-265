"""Rebuild/next-archive vocabulary alias."""

from .materialize import *  # noqa: F401,F403
from .materialize import materialize_archive

rebuild_archive = materialize_archive
build_next_archive = materialize_archive
materialize = materialize_archive
