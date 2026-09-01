"""
Identity bounded context — application-level errors.

Shared by command and query handlers and translated into HTTP responses by the
auth controller / shared auth dependency.
"""


class DuplicateEmailError(Exception):
    pass


class InvalidCredentialsError(Exception):
    pass


class InactiveUserError(Exception):
    pass


class InvalidPasswordError(Exception):
    pass


class UserNotFoundError(Exception):
    pass