from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.contexts.identity.domain.user import Role, User
from src.contexts.identity.infrastructure.models import UserModel


class UserRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @staticmethod
    def _to_domain(row: UserModel) -> User:
        return User(
            id=row.id,
            email=row.email,
            full_name=row.full_name,
            hashed_password=row.hashed_password,
            role=Role(row.role),
            is_active=row.is_active,
        )

    async def get_by_email(self, email: str) -> User | None:
        result = await self._session.execute(select(UserModel).where(UserModel.email == email))
        row = result.scalar_one_or_none()
        return self._to_domain(row) if row else None

    async def add(self, entity: User) -> User:
        row = UserModel(
            id=entity.id,
            email=entity.email,
            full_name=entity.full_name,
            hashed_password=entity.hashed_password,
            role=entity.role.value,
            is_active=entity.is_active,
        )
        self._session.add(row)
        await self._session.flush()
        return self._to_domain(row)