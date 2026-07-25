import pytest

from src.contexts.inventory.domain.product import InsufficientStockError, InvalidPriceError, Product


def test_product_reserve_stock_success():
    product = Product(sku="SKU-1", name="Widget", price_cents=1000, quantity_on_hand=10)
    product.reserve_stock(4)
    assert product.quantity_on_hand == 6


def test_product_reserve_stock_insufficient_raises():
    product = Product(sku="SKU-1", name="Widget", price_cents=1000, quantity_on_hand=2)
    with pytest.raises(InsufficientStockError):
        product.reserve_stock(5)


def test_product_invalid_price_raises():
    with pytest.raises(InvalidPriceError):
        Product(sku="SKU-1", name="Widget", price_cents=0, quantity_on_hand=1)


def test_product_restock_increases_quantity():
    product = Product(sku="SKU-1", name="Widget", price_cents=1000, quantity_on_hand=5)
    product.restock(10)
    assert product.quantity_on_hand == 15
