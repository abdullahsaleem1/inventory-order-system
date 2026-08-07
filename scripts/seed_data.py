"""
Database seed script.

Generates realistic test data for local/dev environments:
- 60 products across a handful of categories, realistic SKUs/prices/stock
- 12 users spread across every role (ADMIN, MANAGER, STAFF, CUSTOMER)
- Passwords are hashed with real bcrypt (per-user salt); every seeded user
  logs in with the shared DEFAULT_SEED_PASSWORD (dev only, see below)

Usage:
    python -m scripts.seed_data
    python -m scripts.seed_data --reset   # wipe seeded tables first

Idempotent: re-running without --reset skips rows that already exist
(matched by SKU / email) instead of creating duplicates. Re-run with
--reset to re-seed rows that were created before real hashing existed.
"""
import argparse
import asyncio
import random

from faker import Faker
from sqlalchemy import delete

from src.contexts.identity.domain.user import Role, User
from src.contexts.identity.infrastructure.models import UserModel
from src.contexts.identity.infrastructure.password_hasher import BcryptPasswordHasher
from src.contexts.identity.repositories.user_repository import UserRepository
from src.contexts.inventory.domain.product import Product
from src.contexts.inventory.infrastructure.models import ProductModel
from src.contexts.inventory.repositories.product_repository import ProductRepository
from src.core.logging_config import configure_logging, get_logger
from src.shared.infrastructure.database import AsyncSessionLocal

configure_logging()
logger = get_logger("seed")
fake = Faker()

PRODUCT_CATEGORIES = ["Electronics", "Home & Kitchen", "Sporting Goods", "Office Supplies", "Toys"]

# role -> how many users to generate with that role
ROLE_DISTRIBUTION: dict[Role, int] = {
    Role.ADMIN: 2,
    Role.MANAGER: 3,
    Role.STAFF: 3,
    Role.CUSTOMER: 4,
}

# Every seeded account shares this development-only password so you can log in
# through the auth endpoints immediately. Change it if the seed data is ever
# exposed outside a throwaway local/dev database.
DEFAULT_SEED_PASSWORD = "Password123!"

password_hasher = BcryptPasswordHasher()


def _generate_sku(category: str, index: int) -> str:
    prefix = "".join(w[0] for w in category.split()).upper()
    return f"{prefix}-{index:04d}"


async def seed_products(session, count: int = 60) -> int:
    repo = ProductRepository(session)
    created = 0
    for i in range(1, count + 1):
        category = random.choice(PRODUCT_CATEGORIES)
        sku = _generate_sku(category, i)

        if await repo.get_by_sku(sku) is not None:
            continue  # idempotent: skip if already seeded

        product = Product(
            sku=sku,
            name=f"{fake.word().capitalize()} {category[:-1] if category.endswith('s') else category}",
            price_cents=random.randint(499, 49999),
            quantity_on_hand=random.randint(0, 500),
        )
        await repo.add(product)
        created += 1

    logger.info("products_seeded", extra={"created": created, "requested": count})
    return created


async def seed_users(session) -> int:
    repo = UserRepository(session)
    created = 0

    for role, count in ROLE_DISTRIBUTION.items():
        for _ in range(count):
            email = fake.unique.email()

            if await repo.get_by_email(email) is not None:
                continue  # idempotent: skip if already seeded

            user = User(
                email=email,
                full_name=fake.name(),
                hashed_password="",  # set via real bcrypt hasher below
                role=role,
                is_active=True,
            )
            user.set_password(DEFAULT_SEED_PASSWORD, password_hasher)
            await repo.add(user)
            created += 1

    logger.info("users_seeded", extra={"created": created})
    return created


async def reset(session) -> None:
    await session.execute(delete(UserModel))
    await session.execute(delete(ProductModel))
    logger.info("seed_data_reset")


async def main(do_reset: bool) -> None:
    async with AsyncSessionLocal() as session:
        if do_reset:
            await reset(session)
            await session.commit()

        products_created = await seed_products(session)
        users_created = await seed_users(session)
        await session.commit()

    logger.info(
        "seed_complete",
        extra={"products_created": products_created, "users_created": users_created},
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed the database with realistic test data.")
    parser.add_argument("--reset", action="store_true", help="Wipe seeded tables before seeding.")
    args = parser.parse_args()
    asyncio.run(main(do_reset=args.reset))