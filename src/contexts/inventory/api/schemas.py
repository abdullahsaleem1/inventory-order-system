"""
Inventory bounded context — API layer schemas.
Pydantic models for request validation and response serialization.
Kept separate from the domain entity so the HTTP contract can evolve
independently of internal domain modeling.
"""
from uuid import UUID

from pydantic import BaseModel, Field


class ProductCreateRequest(BaseModel):
    sku: str = Field(..., min_length=1, max_length=64)
    name: str = Field(..., min_length=1, max_length=255)
    price_cents: int = Field(..., gt=0)
    quantity_on_hand: int = Field(0, ge=0)


class StockAdjustmentRequest(BaseModel):
    quantity: int = Field(..., gt=0)


class ProductResponse(BaseModel):
    id: UUID
    sku: str
    name: str
    price_cents: int
    quantity_on_hand: int

    model_config = {"from_attributes": True}
