"""Patch vocabulary alias for materialization."""

from .materialize import *  # noqa: F401,F403
from .materialize import materialize_archive as apply_patch  # noqa: F401
from .materialize import materialize_archive as patch_archive  # noqa: F401
from .materialize import materialize_archive  # noqa: F401
