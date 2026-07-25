from uuid import UUID

from pydantic import BaseModel, Field


class OrderLineRequest(BaseModel):
    product_id: UUID
    quantity: int = Field(..., gt=0)
    unit_price_cents: int = Field(..., gt=0)


class OrderCreateRequest(BaseModel):
    customer_id: UUID
    lines: list[OrderLineRequest] = Field(..., min_length=1)


class OrderLineResponse(BaseModel):
    product_id: UUID
    quantity: int
    unit_price_cents: int
    subtotal_cents: int


class OrderResponse(BaseModel):
    id: UUID
    customer_id: UUID
    status: str
    lines: list[OrderLineResponse]
    total_cents: int
