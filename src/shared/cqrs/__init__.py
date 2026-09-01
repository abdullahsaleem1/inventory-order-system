"""
CQRS kernel (Week 7).

Two kinds of use-case messages, one in-process dispatcher:

* **Commands** — an intention to change state (the *write side*). A command
  is imperative (e.g. "confirm this order"), named after the task, and is
  executed by exactly one command handler that operates on the write model /
  write-optimized database.
* **Queries** — a question about data (the *read side*). A query is answered
  by exactly one query handler that reads from read-optimized projections
  (denormalized read models) instead of the normalized write tables.

`CqrsBus` is the mediator: controllers construct a bus, register concrete
handler instances (built with their repositories through FastAPI dependency
injection), and dispatch message objects by their type. Commands and queries
must be **frozen dataclasses** — they are immutable inputs, never mutated.

This gives the codebase an explicit, testable split between the write side and
the read side of every use case without pulling in a heavyweight framework.
Rules enforced at dispatch time:

* every command type has exactly one registered handler,
* every query type has exactly one registered handler,
* dispatching an unregistered message raises `CqrsMessageError`.
"""
from __future__ import annotations

from typing import Any, Awaitable, Callable, TypeVar

from dataclasses import dataclass

TMessage = TypeVar("TMessage")
TResult = TypeVar("TResult")

CommandHandler = Callable[[TMessage], Awaitable[TResult]]
QueryHandler = Callable[[TMessage], Awaitable[TResult]]


class CqrsError(Exception):
    """Base class for CQRS infrastructure errors."""


class CqrsMessageError(CqrsError):
    """Raised when a command/query is dispatched without a registered handler."""


@dataclass(frozen=True)
class Command:
    """Base class for write-side use-case messages. Subclass with payload fields."""


@dataclass(frozen=True)
class Query:
    """Base class for read-side use-case messages. Subclass with filter fields."""


class CqrsBus:
    """In-process command/query dispatcher (mediator).

    Handlers are registered per concrete message type and resolved at dispatch
    time. The bus itself keeps no state between requests, so a fresh instance
    per HTTP request (built inside the route's dependency-injection chain) is
    both safe and trivially testable.
    """

    def __init__(self) -> None:
        self._command_handlers: dict[type, Any] = {}
        self._query_handlers: dict[type, Any] = {}

    # -- registration --------------------------------------------------------

    def register_command(self, message_type: type, handler: CommandHandler) -> "CqrsBus":
        """Register a command handler.

        `handler` may be a handler *instance* (with a `.handle` method) or a
        bare async callable; both are accepted and resolved to an async
        callable bound to the message.
        """
        self._command_handlers[message_type] = self._as_callable(handler)
        return self

    def register_query(self, message_type: type, handler: QueryHandler) -> "CqrsBus":
        self._query_handlers[message_type] = self._as_callable(handler)
        return self

    @staticmethod
    def _as_callable(handler) -> Any:
        bound = getattr(handler, "handle", None)
        return bound if callable(bound) else handler

    # -- dispatch ------------------------------------------------------------

    async def dispatch_command(self, command: Command) -> TResult:
        call = self._command_handlers.get(type(command))
        if call is None:
            raise CqrsMessageError(f"No command handler registered for {type(command).__name__}")
        return await call(command)

    async def dispatch_query(self, query: Query) -> TResult:
        call = self._query_handlers.get(type(query))
        if call is None:
            raise CqrsMessageError(f"No query handler registered for {type(query).__name__}")
        return await call(query)