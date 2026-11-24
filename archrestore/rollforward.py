"""Roll-forward vocabulary alias for materialization."""

from .materialize import *  # noqa: F401,F403
from .materialize import materialize_archive

roll_forward = materialize_archive
rollforward_archive = materialize_archive
apply_change_set = materialize_archive
