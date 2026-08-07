"""
Identity bounded context — routes layer.
Thin wiring only: defines HTTP endpoints, wires the dependency chain
(session -> repository -> service -> controller) and delegates immediately
to the controller. No business logic here.

Endpoints:
  POST /auth/register   — JSON registration, returns user + short-lived token
  POST /auth/login      — JSON login, returns short-lived access token
  POST /oauth/token     — OAuth2.0 password grant (form-encoded), returns token
  GET  /auth/me         — current user, requires Bearer access token
"""
from fastapi import APIRouter, Depends, Form
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.ext.asyncio import AsyncSession

from src.contexts.identity.api.schemas import (
    LoginRequest,
    RegisterRequest,
    RegisterResponse,
    TokenResponse,
    UserResponse,
)
from src.contexts.identity.controllers.auth_controller import AuthController
from src.contexts.identity.infrastructure.jwt_service import JwtTokenService
from src.contexts.identity.infrastructure.password_hasher import BcryptPasswordHasher
from src.contexts.identity.repositories.user_repository import UserRepository
from src.contexts.identity.services.auth_service import AuthService
from src.core.config import get_settings
from src.shared.infrastructure.database import get_db_session

router = APIRouter(prefix="/auth", tags=["Authentication"])
oauth2_router = APIRouter(prefix="/oauth", tags=["Authentication"])

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/oauth/token")

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
    )
    return AuthController(AuthService(repository, hasher, tokens))


@router.post(
    "/register",
    response_model=RegisterResponse,
    status_code=201,
    summary="Register a new user",
)
async def register(
    payload: RegisterRequest,
    controller: AuthController = Depends(get_auth_controller),
) -> RegisterResponse:
    return await controller.register(payload)


@router.post("/login", response_model=TokenResponse, summary="Login with email and password")
async def login(
    payload: LoginRequest,
    controller: AuthController = Depends(get_auth_controller),
) -> TokenResponse:
    return await controller.login(payload)


@oauth2_router.post(
    "/token",
    response_model=TokenResponse,
    summary="OAuth2.0 password grant token endpoint",
    description=(
        "Issues a short-lived Bearer access token. Accepts the standard "
        "OAuth2.0 `application/x-www-form-urlencoded` body with "
        "`grant_type=password`, `username` (the user's email) and `password`. "
        "This is the tokenUrl used by Swagger's Authorize button."
    ),
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
    # The standard OAuth2PasswordRequestForm hard-codes grant_type="password",
    # which would 422 on any other value. We parse the form ourselves so a
    # genuinely unsupported grant type can return the OAuth2 400 response.
    return await controller.oauth2_token(
        grant_type=grant_type, username=username, password=password
    )


@router.get("/me", response_model=UserResponse, summary="Get the authenticated user")
async def me(
    token: str = Depends(oauth2_scheme),
    controller: AuthController = Depends(get_auth_controller),
) -> UserResponse:
    return await controller.me(token)
