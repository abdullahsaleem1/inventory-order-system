"""
Identity bounded context — controller layer.

CQRS (Week 7): holds a `CqrsBus` and only translates API schemas to
`Command`/`Query` objects and maps exceptions onto the shared AppError
hierarchy. No auth logic lives here.
"""
from src.contexts.identity.api.schemas import (
    LoginRequest,
    LogoutRequest,
    MessageResponse,
    RefreshRequest,
    RegisterRequest,
    RegisterResponse,
    TokenResponse,
    UserResponse,
)
from src.contexts.identity.command_handlers import (
    LoginCommandHandler,
    LogoutAllCommandHandler,
    LogoutCommandHandler,
    RefreshTokensCommandHandler,
    RegisterUserCommandHandler,
)
from src.contexts.identity.commands import (
    LoginCommand,
    LogoutAllCommand,
    LogoutCommand,
    RefreshTokensCommand,
    RegisterUserCommand,
)
from src.contexts.identity.domain.errors import InvalidTokenError, TokenExpiredError
from src.contexts.identity.domain.user import Role
from src.contexts.identity.errors import (
    DuplicateEmailError,
    InactiveUserError,
    InvalidCredentialsError,
    InvalidPasswordError,
)
from src.contexts.identity.query_handlers import GetCurrentUserQueryHandler
from src.contexts.identity.queries import GetCurrentUserQuery
from src.shared.cqrs import CqrsBus
from src.shared.exceptions import BadRequestError, ConflictError, UnauthorizedError


class AuthController:
    def __init__(self, bus: CqrsBus) -> None:
        self._bus = bus

    async def register(self, payload: RegisterRequest) -> RegisterResponse:
        try:
            result = await self._bus.dispatch_command(
                RegisterUserCommand(
                    email=payload.email,
                    full_name=payload.full_name,
                    password=payload.password,
                    role=Role(payload.role) if payload.role else Role.CUSTOMER,
                )
            )
        except DuplicateEmailError as exc:
            raise ConflictError(str(exc), code="DUPLICATE_EMAIL") from exc
        except InvalidPasswordError as exc:
            raise BadRequestError(str(exc), code="INVALID_PASSWORD") from exc

        return RegisterResponse(
            user=UserResponse.model_validate(result.user, from_attributes=True),
            **result.token_data,
        )

    async def login(self, payload: LoginRequest) -> TokenResponse:
        return await self._authenticate(email=payload.email, password=payload.password)

    async def oauth2_token(
        self, *, grant_type: str, username: str, password: str
    ) -> TokenResponse:
        if grant_type == "refresh_token":
            raise BadRequestError(
                "Use POST /auth/refresh with a JSON body for refresh grants",
                code="unsupported_grant_type",
            )
        if grant_type != "password":
            raise BadRequestError(
                "Only the 'password' grant type is supported",
                code="unsupported_grant_type",
            )
        return await self._authenticate(email=username, password=password)

    async def _authenticate(self, *, email: str, password: str) -> TokenResponse:
        try:
            result = await self._bus.dispatch_command(LoginCommand(email, password))
        except (InvalidCredentialsError, InactiveUserError) as exc:
            raise UnauthorizedError(
                "Incorrect email or password",
                code="invalid_grant",
            ) from exc
        return TokenResponse(**result.token_data)

    async def me(self, token: str) -> UserResponse:
        try:
            user = await self._bus.dispatch_query(GetCurrentUserQuery(token))
        except TokenExpiredError as exc:
            raise UnauthorizedError("Access token has expired", code="token_expired") from exc
        except InvalidTokenError as exc:
            raise UnauthorizedError(str(exc), code="invalid_token") from exc
        except InactiveUserError as exc:
            raise UnauthorizedError(str(exc), code="account_disabled") from exc
        return UserResponse.model_validate(user, from_attributes=True)

    async def refresh(self, payload: RefreshRequest) -> TokenResponse:
        try:
            token_data = await self._bus.dispatch_command(
                RefreshTokensCommand(payload.refresh_token)
            )
        except TokenExpiredError as exc:
            raise UnauthorizedError("Refresh token has expired", code="token_expired") from exc
        except InvalidTokenError as exc:
            raise UnauthorizedError(str(exc), code="invalid_refresh_token") from exc
        except InactiveUserError as exc:
            raise UnauthorizedError(str(exc), code="account_disabled") from exc
        return TokenResponse(**token_data)

    async def logout(
        self, access_token: str | None, payload: LogoutRequest | None = None
    ) -> MessageResponse:
        refresh_token = payload.refresh_token if payload else None
        await self._bus.dispatch_command(
            LogoutCommand(access_token=access_token, refresh_token=refresh_token)
        )
        return MessageResponse(message="Logged out successfully")

    async def logout_all(self, token: str) -> MessageResponse:
        # Resolve the user from the token (ignore failures — logout-all is
        # best-effort and ends every session it can), then revoke all sessions.
        user_id = None
        try:
            user = await self._bus.dispatch_query(GetCurrentUserQuery(token))
            user_id = user.id
        except (TokenExpiredError, InvalidTokenError, InactiveUserError):
            pass
        await self._bus.dispatch_command(LogoutAllCommand(user_id=user_id))
        return MessageResponse(message="Logged out from all sessions")