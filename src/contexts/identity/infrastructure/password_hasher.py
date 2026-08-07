"""
Identity bounded context — infrastructure layer.
Concrete bcrypt implementation of the domain's PasswordHasher protocol.

Salt management: bcrypt generates a fresh random 16-byte (128-bit) salt on
every hash call and embeds it in the returned 60-char hash string
(`$2b$<cost>$<22-char base64 salt><31-char base64 digest>`). No separate
salt column or manual salt bookkeeping is needed — the salt travels with the
hash and is re-read by `checkpw` at verification time.
"""
import bcrypt

from src.contexts.identity.domain.password import PasswordHasher


class BcryptPasswordHasher(PasswordHasher):
    # OWASP-recommended work factor. bcrypt 5.x caps passwords at 72 bytes.
    COST_FACTOR = 12
    MAX_PASSWORD_BYTES = 72

    def hash_password(self, plain: str) -> str:
        self._validate_length(plain)
        salt = bcrypt.gensalt(rounds=self.COST_FACTOR)
        return bcrypt.hashpw(plain.encode("utf-8"), salt).decode("utf-8")

    def verify_password(self, plain: str, hashed: str) -> bool:
        if self._exceeds_limit(plain):
            return False
        try:
            return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
        except (ValueError, TypeError):
            # Malformed stored hash (e.g. a seed placeholder) must never crash login.
            return False

    def _validate_length(self, plain: str) -> None:
        if self._exceeds_limit(plain):
            raise ValueError(
                f"Password must be at most {self.MAX_PASSWORD_BYTES} bytes "
                f"(bcrypt limit); got {len(plain.encode('utf-8'))}"
            )

    def _exceeds_limit(self, plain: str) -> bool:
        return len(plain.encode("utf-8")) > self.MAX_PASSWORD_BYTES
