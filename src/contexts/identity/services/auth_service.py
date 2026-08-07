"""
Identity bounded context — service (application) layer.
Orchestrates auth use cases: registration, login, and token-based identity
resolution. Contains no business rules itself — password hashing/verification
is delegated to the injected PasswordHasher, token signing to JwtTokenService,
and persistence to the repository.
"""
from uuid import UUID

from src.contexts.identity.domain.errors import InvalidTokenError, TokenExpiredError
from src.contexts.identity.domain.user import Role, User
from src.contexts.identity.repositories.user_repository import UserRepository


class DuplicateEmailError(Exception):
    pass


class InvalidCredentialsError(Exception):
    pass


class InactiveUserError(Exception):
    pass


class InvalidPasswordError(Exception):
    pass


class AuthService:
    def __init__(self, repository: UserRepository, password_hasher, token_service) -> None:
        self._repo = repository
        self._hasher = password_hasher
        self._tokens = token_service

    @property
    def access_token_expires_in(self) -> int:
        return self._tokens.access_token_expires_in

    async def register(self, *, email: str, full_name: str, password: str, role: Role = Role.CUSTOMER) -> User:
        normalized_email = email.strip().lower()
        if await self._repo.get_by_email(normalized_email) is not None:
            raise DuplicateEmailError(f"An account with email '{normalized_email}' already exists")

        user = User(
            email=normalized_email,
            full_name=full_name.strip(),
            hashed_password="",  # set below via the hasher (keeps domain pure)
            role=role,
            is_active=True,
        )
        try:
            user.set_password(password, self._hasher)
        except ValueError as exc:
            raise InvalidPasswordError(str(exc)) from exc
        return await self._repo.add(user)

    async def login(self, *, email: str, password: str) -> User:
        user = await self._repo.get_by_email(email.strip().lower())
        if user is None or not user.verify_password(password, self._hasher):
            # Same message for unknown email vs wrong password — never reveal
            # whether an account exists.
            raise InvalidCredentialsError("Incorrect email or password")
        if not user.is_active:
            raise InactiveUserError("User account is disabled")
        return user

    def create_access_token(self, user: User) -> str:
        return self._tokens.create_access_token(user)

    async def get_user_from_token(self, token: str) -> User:
        claims = self._tokens.decode_access_token(token)
        user = await self._repo.get_by_id(UUID(claims["sub"]))
        if user is None:
            raise InvalidTokenError("User for this token no longer exists")
        if not user.is_active:
            raise InactiveUserError("User account is disabled")
        return user
