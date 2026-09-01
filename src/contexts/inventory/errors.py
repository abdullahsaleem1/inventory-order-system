"""
Inventory bounded context — application-level errors.

Moved here from the old `product_service.py` so both command and query
handlers (and the controller) share one import path.
"""


class ProductNotFoundError(Exception):
    pass


class DuplicateSkuError(Exception):
    pass