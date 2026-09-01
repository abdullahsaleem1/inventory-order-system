"""
Identity bounded context — CQRS query handlers (Week 7).

The read side. Answers auth reads: resolve the current user from a token (used
by the shared auth dependency and by `GET /auth/me`) and look up users.
"""
from src.contexts.identity.domain.user import User
from src.contexts.identity.errors import UserNotFoundError
from src.contexts.identity.queries import GetCurrentUserQuery, GetUserQuery
from src.contexts.identity.repositories.user_repository import UserRepository
from src.contexts.identity.services.auth_service import AuthService


class GetCurrentUserQueryHandler:
    def __init__(self, auth_service: AuthService) -> None:
        self._service = auth_service

    async def handle(self, query: GetCurrentUserQuery) -> User:
        return await self._service.get_user_from_token(query.token)


class GetUserQueryHandler:
    def __init__(self, user_repo: UserRepository) -> None:
        self._user_repo = user_repo

    async def handle(self, query: GetUserQuery) -> User:
        user = await self._user_repo.get_by_id(query.user_id)
        if user is None:
            raise UserNotFoundError(f"User {query.user_id} not found")
        return user