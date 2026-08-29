"""Inventory bounded context — repository for the stock-reservation idempotency log."""
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.contexts.inventory.infrastructure.models import InventoryReservationLogModel


class InventoryReservationRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def exists(self, order_id: UUID) -> bool:
        result = await self._session.execute(
            select(InventoryReservationLogModel.id).where(
                InventoryReservationLogModel.order_id == order_id
            )
        )
        return result.scalar_one_or_none() is not None

    async def add(self, order_id: UUID, event_id: UUID, status: str = "RESERVED") -> None:
        self._session.add(
            InventoryReservationLogModel(order_id=order_id, event_id=event_id, status=status)
        )
        await self._session.flush()
