"""
Identity bounded context — CQRS command handlers (Week 7).

The write side. Each handlers wraps the `AuthService` use-case engine behind a
`Command` object, so the controller can dispatch every auth action through the
bus. Reuses the exception vocabulary (`DuplicateEmailError`, ...) for
controller translation.
"""
from dataclasses import dataclass

from src.contexts.identity.commands import (
    LoginCommand,
    LogoutAllCommand,
    LogoutCommand,
    RefreshTokensCommand,
    RegisterUserCommand,
)
from src.contexts.identity.domain.user import User
from src.contexts.identity.services.auth_service import AuthService


@dataclass(frozen=True)
class RegisteredUserResult:
    user: User
    token_data: dict
    user: User
    token_data: dict


@dataclass(frozen=True)
class LoginResult:
    user: User
    token_data: dict


class RegisterUserCommandHandler:
    def __init__(self, auth_service: AuthService) -> None:
        self._service = auth_service

    async def handle(self, command: RegisterUserCommand) -> RegisteredUserResult:
        user = await self._service.register(
            email=command.email,
            full_name=command.full_name,
            password=command.password,
            role=command.role,
        )
        token_data = await self._service.create_token_pair(user)
        return RegisteredUserResult(user=user, token_data=token_data)


class LoginCommandHandler:
    def __init__(self, auth_service: AuthService) -> None:
        self._service = auth_service

    async def handle(self, command: LoginCommand) -> LoginResult:
        user = await self._service.login(email=command.email, password=command.password)
        token_data = await self._service.create_token_pair(user)
        return LoginResult(user=user, token_data=token_data)


class RefreshTokensCommandHandler:
    def __init__(self, auth_service: AuthService) -> None:
        self._service = auth_service

    async def handle(self, command: RefreshTokensCommand) -> dict:
        return await self._service.refresh_tokens(refresh_token=command.refresh_token)


class LogoutCommandHandler:
    def __init__(self, auth_service: AuthService) -> None:
        self._service = auth_service

    async def handle(self, command: LogoutCommand) -> None:
        await self._service.logout(
            access_token=command.access_token,
            refresh_token=command.refresh_token,
        )


class LogoutAllCommandHandler:
    def __init__(self, auth_service: AuthService) -> None:
        self._service = auth_service

    async def handle(self, command: LogoutAllCommand) -> None:
        if command.user_id is not None:
            await self._service.logout_all_for_user_id(user_id=command.user_id)