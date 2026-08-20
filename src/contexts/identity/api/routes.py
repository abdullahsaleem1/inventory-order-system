"""
Identity bounded context — routes layer.
Thin wiring only: defines HTTP endpoints, wires the dependency chain
(session -> repository -> service -> controller) and delegates immediately
to the controller. No business logic here.

Endpoints:
  POST /auth/register   — JSON registration, returns user + token pair
  POST /auth/login      — JSON login, returns access + refresh token pair
  POST /auth/refresh    — rotate a refresh token for a new token pair
  POST /auth/logout     — revoke current session (access + refresh token)
  POST /auth/logout-all — revoke ALL sessions for the user
  GET  /auth/me         — current user, requires Bearer access token
  POST /oauth/token     — OAuth2.0 password grant (form-encoded), returns token pair
"""
from fastapi import APIRouter, Depends, Form
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.ext.asyncio import AsyncSession

from src.contexts.identity.api.schemas import (
    ErrorResponse,
    LoginRequest,
    LogoutRequest,
    MessageResponse,
    RefreshRequest,
    RegisterRequest,
    RegisterResponse,
    TokenResponse,
    UserResponse,
)
from src.contexts.identity.controllers.auth_controller import AuthController
from src.contexts.identity.infrastructure.jwt_service import JwtTokenService
from src.contexts.identity.infrastructure.password_hasher import BcryptPasswordHasher
from src.contexts.identity.infrastructure.models import (
    BlacklistedTokenModel,
    RefreshTokenModel,
)
from src.contexts.identity.repositories.refresh_token_repository import (
    BlacklistedTokenRepository,
    RefreshTokenRepository,
)
from src.contexts.identity.repositories.user_repository import UserRepository
from src.contexts.identity.services.auth_service import AuthService
from src.core.config import get_settings
from src.shared.infrastructure.database import get_db_session

router = APIRouter(prefix="/auth", tags=["Authentication"])
oauth2_router = APIRouter(prefix="/oauth", tags=["Authentication"])

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/oauth/token")

optional_oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/oauth/token", auto_error=False)

settings = get_settings()


def get_auth_controller(session: AsyncSession = Depends(get_db_session)) -> AuthController:
    repository = UserRepository(session)
    hasher = BcryptPasswordHasher()
    tokens = JwtTokenService(
        secret_key=settings.JWT_SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM,
        issuer=settings.JWT_ISSUER,
        audience=settings.JWT_AUDIENCE,
        access_token_expire_minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES,
        refresh_token_expire_days=settings.REFRESH_TOKEN_EXPIRE_DAYS,
    )
    return AuthController(
        AuthService(
            repository,
            hasher,
            tokens,
            refresh_token_repo=RefreshTokenRepository(session),
            blocklist_repo=BlacklistedTokenRepository(session),
        )
    )


# ---- registration -----------------------------------------------------------


@router.post(
    "/register",
    response_model=RegisterResponse,
    status_code=201,
    summary="Register a new user",
    description=(
        "Creates a user account and returns the profile together with a "
        "short-lived Bearer access token and a long-lived refresh token."
    ),
    responses={
        400: {"model": ErrorResponse, "description": "Invalid password (e.g. exceeds bcrypt byte limit)"},
        409: {"model": ErrorResponse, "description": "A user with this email already exists"},
        422: {"model": ErrorResponse, "description": "Request validation failed (missing/invalid fields)"},
    },
)
async def register(
    payload: RegisterRequest,
    controller: AuthController = Depends(get_auth_controller),
) -> RegisterResponse:
    return await controller.register(payload)


# ---- login ------------------------------------------------------------------


@router.post(
    "/login",
    response_model=TokenResponse,
    summary="Login with email and password",
    description=(
        "Exchanges email + password for an access/refresh token pair. "
        "Save both tokens: use the access token for API calls and the "
        "refresh token to obtain a new pair before the access token expires."
    ),
    responses={
        401: {"model": ErrorResponse, "description": "Incorrect email or password, or account disabled"},
        422: {"model": ErrorResponse, "description": "Request validation failed"},
    },
)
async def login(
    payload: LoginRequest,
    controller: AuthController = Depends(get_auth_controller),
) -> TokenResponse:
    return await controller.login(payload)


# ---- refresh ----------------------------------------------------------------


@router.post(
    "/refresh",
    response_model=TokenResponse,
    summary="Rotate a refresh token for a new token pair",
    description=(
        "Accepts a valid (non-revoked, non-expired) refresh token and "
        "returns a fresh access/refresh token pair. The old refresh token "
        "is revoked (sliding window). If a revoked refresh token is "
        "presented, the entire token family is terminated — a defence "
        "against token theft / replay attacks."
    ),
    responses={
        401: {"model": ErrorResponse, "description": "Invalid, expired, or revoked refresh token"},
        422: {"model": ErrorResponse, "description": "Request validation failed"},
    },
)
async def refresh(
    payload: RefreshRequest,
    controller: AuthController = Depends(get_auth_controller),
) -> TokenResponse:
    return await controller.refresh(payload)


# ---- logout -----------------------------------------------------------------


@router.post(
    "/logout",
    response_model=MessageResponse,
    summary="Log out and revoke tokens",
    description=(
        "Revokes the provided refresh token and blacklists the current "
        "access token so it cannot be reused. The refresh token should be "
        "sent in the request body; the access token in the Authorization "
        "header."
    ),
    responses={
        401: {"model": ErrorResponse, "description": "Invalid or expired access token"},
    },
)
async def logout(
    token: str | None = Depends(optional_oauth2_scheme),
    payload: LogoutRequest | None = None,
    controller: AuthController = Depends(get_auth_controller),
) -> MessageResponse:
    return await controller.logout(access_token=token, payload=payload)


@router.post(
    "/logout-all",
    response_model=MessageResponse,
    summary="Log out from all sessions",
    description=(
        "Revokes every active refresh token for the authenticated user, "
        "ending all sessions. Existing access tokens will remain valid "
        "until they expire (default 15 minutes)."
    ),
    responses={
        401: {"model": ErrorResponse, "description": "Invalid or expired access token"},
    },
)
async def logout_all(
    token: str = Depends(oauth2_scheme),
    controller: AuthController = Depends(get_auth_controller),
) -> MessageResponse:
    return await controller.logout_all(token)


# ---- current user -----------------------------------------------------------


@router.get(
    "/me",
    response_model=UserResponse,
    summary="Get the authenticated user",
    description="Returns the profile of the currently authenticated user.",
    responses={
        401: {"model": ErrorResponse, "description": "Missing, expired, or invalid access token"},
    },
)
async def me(
    token: str = Depends(oauth2_scheme),
    controller: AuthController = Depends(get_auth_controller),
) -> UserResponse:
    return await controller.me(token)


# ---- OAuth2.0 password grant ------------------------------------------------


@oauth2_router.post(
    "/token",
    response_model=TokenResponse,
    summary="OAuth2.0 password grant token endpoint",
    description=(
        "Issues a short-lived Bearer access token and a long-lived refresh "
        "token. Accepts the standard OAuth2.0 "
        "`application/x-www-form-urlencoded` body with "
        "`grant_type=password`, `username` (the user's email) and `password`. "
        "This is the tokenUrl used by Swagger's Authorize button."
    ),
    responses={
        400: {"model": ErrorResponse, "description": "Unsupported grant type"},
        401: {"model": ErrorResponse, "description": "Incorrect email or password"},
        422: {"model": ErrorResponse, "description": "Request validation failed (missing fields)"},
    },
)
async def oauth2_token(
    grant_type: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    scope: str = Form(""),
    client_id: str | None = Form(None),
    client_secret: str | None = Form(None),
    controller: AuthController = Depends(get_auth_controller),
) -> TokenResponse:
    return await controller.oauth2_token(
        grant_type=grant_type, username=username, password=password
    )
