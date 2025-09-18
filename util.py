import asyncio
from functools import wraps
import logging
import random

logger = logging.getLogger(__name__)


def async_retry(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0, exceptions: tuple = (Exception,)):
    """异步重试装饰器"""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_retries:
                        logger.error(f"Function {func.__name__} failed after {max_retries + 1} attempts: {e}")
                        raise

                    wait_time = delay * (backoff**attempt) + random.uniform(0, 1)
                    logger.warning(f"Function {func.__name__} failed (attempt {attempt + 1}/{max_retries + 1}): {e}. Retrying in {wait_time:.2f}s...")
                    await asyncio.sleep(wait_time)

            raise last_exception  # type: ignore

        return wrapper

    return decorator
