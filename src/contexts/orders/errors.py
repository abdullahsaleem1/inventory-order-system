"""
Orders bounded context — application-level errors.

Borrows the `events.py` precedent of a root-level module for the context.
These errors are raised by command/query handlers and translated into HTTP
responses by the controller.
"""


class OrderNotFoundError(Exception):
    pass