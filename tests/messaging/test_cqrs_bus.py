"""
Unit tests — the in-process CQRS command/query dispatcher (shared/cqrs).

Verifies the mediator contract:
  * a registered command handler is invoked with the command,
  * a registered query handler is invoked with the query,
  * unregistered dispatch raises CqrsMessageError,
  * handler instances (with a `.handle` method) and bare callables are both
    accepted.
"""
import pytest

from src.shared.cqrs import Command, CqrsBus, CqrsMessageError, Query


class AddOrderCommand(Command):
    pass


class GetOrderQuery(Query):
    pass


class RecordingHandler:
    def __init__(self, log: list) -> None:
        self._log = log

    async def handle(self, message):
        self._log.append(("handler", message))
        return "handled"


async def test_command_handler_instance_is_invoked() -> None:
    log: list = []
    bus = CqrsBus().register_command(AddOrderCommand, RecordingHandler(log))
    cmd = AddOrderCommand()
    result = await bus.dispatch_command(cmd)
    assert result == "handled"
    assert log == [("handler", cmd)]


async def test_query_handler_callable_is_invoked() -> None:
    async def handler(query):
        return f"query:{type(query).__name__}"

    bus = CqrsBus().register_query(GetOrderQuery, handler)
    assert await bus.dispatch_query(GetOrderQuery()) == "query:GetOrderQuery"


async def test_unregistered_command_raises() -> None:
    bus = CqrsBus()
    with pytest.raises(CqrsMessageError):
        await bus.dispatch_command(AddOrderCommand())


async def test_unregistered_query_raises() -> None:
    bus = CqrsBus()
    with pytest.raises(CqrsMessageError):
        await bus.dispatch_query(GetOrderQuery())


async def test_registration_is_per_concrete_type() -> None:
    class OtherCommand(Command):
        pass

    calls: list[str] = []

    async def add_handler(c):
        calls.append("add")

    async def other_handler(c):
        calls.append("other")

    bus = (
        CqrsBus()
        .register_command(AddOrderCommand, add_handler)
        .register_command(OtherCommand, other_handler)
    )
    await bus.dispatch_command(AddOrderCommand())
    await bus.dispatch_command(OtherCommand())
    assert calls == ["add", "other"]