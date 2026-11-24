"""Agent-noun alias for :mod:`archrestore.materialize`."""

from .materialize import *  # noqa: F401,F403
from .materialize import (  # noqa: F401
    MaterializeError,
    materialize,
    materialize_archive,
)

Materializer = type("Materializer", (), {
    "materialize": staticmethod(materialize_archive),
    "apply": staticmethod(materialize_archive),
})


def materializer(*args, **kwargs):
    return materialize_archive(*args, **kwargs)
