"""
Identity bounded context — API layer schemas.
Pydantic models for auth request validation and response serialization,
kept separate from the domain User entity so the HTTP contract can evolve
independently.
"""
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field


class ErrorResponse(BaseModel):
    """Standardized error envelope returned by every endpoint on failure."""
    error: "ErrorDetail"
    status: int
    request_id: str | None = None
    path: str
    timestamp: str


class ErrorDetail(BaseModel):
    code: str
    message: str
    details: dict | list | None = None


# Rebuild forward ref
ErrorResponse.model_rebuild()


class RegisterRequest(BaseModel):
    email: EmailStr
    full_name: str = Field(..., min_length=1, max_length=255)
    password: str = Field(..., min_length=8, max_length=72)
    role: str | None = Field(
        None,
        pattern="^(ADMIN|MANAGER|STAFF|CUSTOMER)$",
        description="Optional role assignment. Defaults to CUSTOMER if omitted.",
    )


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=1, max_length=72)


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    refresh_expires_in: int


class UserResponse(BaseModel):
    id: UUID
    email: str
    full_name: str
    role: str
    is_active: bool

    model_config = {"from_attributes": True}


class RegisterResponse(BaseModel):
    user: UserResponse
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    refresh_expires_in: int


class RefreshRequest(BaseModel):
    refresh_token: str


class LogoutRequest(BaseModel):
    refresh_token: str | None = None


class MessageResponse(BaseModel):
    message: str
