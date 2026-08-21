"""
Wiring between settings and the messaging layer.

- `build_event_publisher()` — returns a RabbitMQEventPublisher when BROKER_URL
  is configured, otherwise None (events disabled).
- `get_event_publisher()` — FastAPI dependency used by the orders routes.
  Raises ServiceUnavailableError (503 / EVENT_BROKER_UNAVAILABLE) when the
  broker is not configured so clients never silently lose events.
- `probe_broker()` — cheap connectivity check used by GET /ready.
- `close_event_publisher()` — called from app shutdown.
"""
import asyncio

from src.core.config import get_settings
from src.core.logging_config import get_logger
from src.shared.exceptions import ServiceUnavailableError
from src.shared.messaging.publisher import RabbitMQEventPublisher

logger = get_logger("messaging.provider")

_publisher: RabbitMQEventPublisher | None = None
_publisher_built = False


def build_event_publisher() -> RabbitMQEventPublisher | None:
    """Create (once) the process-wide publisher based on current settings."""
    global _publisher, _publisher_built
    if not _publisher_built:
        broker_url = (get_settings().BROKER_URL or "").strip()
        if broker_url:
            _publisher = RabbitMQEventPublisher(broker_url)
            logger.info("event_publisher_initialized", extra={"broker": "rabbitmq"})
        else:
            _publisher = None
            logger.warning("event_publisher_disabled", extra={"reason": "BROKER_URL is not set"})
        _publisher_built = True
    return _publisher


async def get_event_publisher() -> RabbitMQEventPublisher:
    publisher = build_event_publisher()
    if publisher is None:
        raise ServiceUnavailableError(
            "Event publishing is not configured: set BROKER_URL to a RabbitMQ URL",
            code="EVENT_BROKER_UNAVAILABLE",
        )
    return publisher


async def probe_broker() -> str:
    """Returns "up", "down" or "disabled" — never raises."""
    publisher = build_event_publisher()
    if publisher is None:
        return "disabled"
    try:
        await asyncio.wait_for(publisher._ensure_exchange(), timeout=3.0)  # noqa: SLF001
        return "up"
    except Exception:
        logger.exception("broker_probe_failed")
        return "down"


async def close_event_publisher() -> None:
    global _publisher_built
    if _publisher is not None:
        await _publisher.close()
        logger.info("event_publisher_closed")
    _publisher_built = False
