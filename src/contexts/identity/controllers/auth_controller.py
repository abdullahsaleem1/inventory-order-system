"""
Identity bounded context — controller layer.
Sits between routes and the auth service: converts API schemas to service
calls and maps service/domain exceptions onto the shared AppError hierarchy,
which the global handlers serialize into the standardized error envelope.
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
from src.contexts.identity.domain.errors import InvalidTokenError, TokenExpiredError
from src.contexts.identity.domain.user import Role
from src.contexts.identity.services.auth_service import (
    AuthService,
    DuplicateEmailError,
    InactiveUserError,
    InvalidCredentialsError,
    InvalidPasswordError,
)
from src.shared.exceptions import BadRequestError, ConflictError, UnauthorizedError


class AuthController:
    def __init__(self, service: AuthService) -> None:
        self._service = service

    # ---- registration --------------------------------------------------------

    async def register(self, payload: RegisterRequest) -> RegisterResponse:
        try:
            user = await self._service.register(
                email=payload.email,
                full_name=payload.full_name,
                password=payload.password,
                role=Role(payload.role) if payload.role else Role.CUSTOMER,
            )
        except DuplicateEmailError as exc:
            raise ConflictError(str(exc), code="DUPLICATE_EMAIL") from exc
        except InvalidPasswordError as exc:
            raise BadRequestError(str(exc), code="INVALID_PASSWORD") from exc

        token_data = await self._service.create_token_pair(user)
        return RegisterResponse(
            user=UserResponse.model_validate(user, from_attributes=True),
            **token_data,
        )

    # ---- login / OAuth2 token ------------------------------------------------

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
            user = await self._service.login(email=email, password=password)
        except (InvalidCredentialsError, InactiveUserError) as exc:
            raise UnauthorizedError(
                "Incorrect email or password",
                code="invalid_grant",
            ) from exc
        token_data = await self._service.create_token_pair(user)
        return TokenResponse(**token_data)

    # ---- current user --------------------------------------------------------

    async def me(self, token: str) -> UserResponse:
        try:
            user = await self._service.get_user_from_token(token)
        except TokenExpiredError as exc:
            raise UnauthorizedError("Access token has expired", code="token_expired") from exc
        except InvalidTokenError as exc:
            raise UnauthorizedError(str(exc), code="invalid_token") from exc
        except InactiveUserError as exc:
            raise UnauthorizedError(str(exc), code="account_disabled") from exc
        return UserResponse.model_validate(user, from_attributes=True)

    # ---- refresh -------------------------------------------------------------

    async def refresh(self, payload: RefreshRequest) -> TokenResponse:
        try:
            token_data = await self._service.refresh_tokens(refresh_token=payload.refresh_token)
        except TokenExpiredError as exc:
            raise UnauthorizedError("Refresh token has expired", code="token_expired") from exc
        except InvalidTokenError as exc:
            raise UnauthorizedError(str(exc), code="invalid_refresh_token") from exc
        except InactiveUserError as exc:
            raise UnauthorizedError(str(exc), code="account_disabled") from exc
        return TokenResponse(**token_data)

    # ---- logout / logout-all -------------------------------------------------

    async def logout(
        self, access_token: str | None, payload: LogoutRequest | None = None
    ) -> MessageResponse:
        refresh_token = payload.refresh_token if payload else None
        await self._service.logout(access_token=access_token, refresh_token=refresh_token)
        return MessageResponse(message="Logged out successfully")

    async def logout_all(self, token: str) -> MessageResponse:
        try:
            user = await self._service.get_user_from_token(token)
        except (TokenExpiredError, InvalidTokenError, InactiveUserError):
            return MessageResponse(message="Logged out from all sessions")
        await self._service.logout_all(user=user)
        return MessageResponse(message="Logged out from all sessions")
