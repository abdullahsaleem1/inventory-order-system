"""
Identity bounded context — domain layer.
Domain-level exceptions raised by token verification.
"""
from src.shared.domain.base import DomainError


class InvalidTokenError(DomainError):
    """Token is malformed, wrong signature, or otherwise unusable."""


class TokenExpiredError(DomainError):
    """Token's exp claim is in the past."""
