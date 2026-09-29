"""
Read-store failure taxonomy (Week 11 chaos engineering).

Chaos testing showed the read side conflated two very different conditions
under a bare `except Exception: return None`:

  1. the document genuinely is not projected yet (normal eventual-consistency
     404), and
  2. the read store itself is unreachable (a real outage).

Reporting (2) as (1) tells an on-call engineer that an order "does not exist"
during a full Elasticsearch outage — precisely when accurate information
matters most. `ReadStoreUnavailableError` gives case (2) its own type so the
API can answer 503 instead of a misleading 404.

This is deliberately *not* an `AppError`: the readstore layer is storage-
agnostic and must not know about HTTP. The orders context translates it into
`ServiceUnavailableError` at the query-handler boundary.
"""


class ReadStoreUnavailableError(Exception):
    """The read store could not be reached or returned a non-recoverable error.

    Never raised to mean "document absent" — that case returns ``None``.
    """

    def __init__(self, message: str, *, operation: str | None = None) -> None:
        self.operation = operation
        self.message = message
        super().__init__(message)
