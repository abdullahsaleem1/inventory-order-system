"""
Unit tests for the bcrypt password hasher (infrastructure layer).
"""
import pytest

from src.contexts.identity.infrastructure.password_hasher import BcryptPasswordHasher


def test_hash_produces_bcrypt_format():
    hasher = BcryptPasswordHasher()
    hashed = hasher.hash_password("S3curePass!")
    assert hashed.startswith("$2b$12$")
    assert len(hashed) == 60


def test_verify_matches_and_rejects():
    hasher = BcryptPasswordHasher()
    hashed = hasher.hash_password("S3curePass!")
    assert hasher.verify_password("S3curePass!", hashed) is True
    assert hasher.verify_password("wrong-password", hashed) is False


def test_same_password_gets_different_salts():
    """Each hash embeds a fresh random salt, so identical inputs differ."""
    hasher = BcryptPasswordHasher()
    assert hasher.hash_password("S3curePass!") != hasher.hash_password("S3curePass!")


def test_verify_handles_malformed_hash_without_crashing():
    """Old seed placeholders / corrupt hashes must yield False, never raise."""
    hasher = BcryptPasswordHasher()
    assert hasher.verify_password("S3curePass!", "unset$replace-with-real-hash") is False
    assert hasher.verify_password("S3curePass!", "not-even-a-hash") is False


def test_hash_rejects_overlong_password():
    hasher = BcryptPasswordHasher()
    with pytest.raises(ValueError):
        hasher.hash_password("x" * 100)
