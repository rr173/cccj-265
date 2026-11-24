"""Delta vocabulary alias for materialization."""

from .materialize import *  # noqa: F401,F403
from .materialize import materialize_archive as apply_delta  # noqa: F401
from .materialize import materialize_archive as delta_archive  # noqa: F401
from .materialize import materialize_archive  # noqa: F401
