"""Neighbouring-name alias for :mod:`archrestore.materialize`."""

from .materialize import *  # noqa: F401,F403
from .materialize import materialize_archive  # noqa: F401
from .apply import apply_changes, apply_package  # noqa: F401

apply_change_package = apply_package
load_and_apply = apply_changes
