"""Materialize/reify synonym module."""

from .materialize import *  # noqa: F401,F403
from .materialize import materialize_archive as reify_archive  # noqa: F401
from .materialize import materialize_archive  # noqa: F401

reify = materialize_archive
