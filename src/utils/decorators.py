from __future__ import annotations

import asyncio
import croniter
import logging
from asyncio import ensure_future
from datetime import datetime
from functools import wraps
from traceback import format_exception

from starlette.concurrency import run_in_threadpool

logger = logging.Logger(__name__)


def repeat_at(
        #*,
        cron_str: str
):
    def decorator(func):
        is_coroutine = asyncio.iscoroutinefunction(func)

        @wraps(func)
        async def wrapped() -> None:
            async def loop() -> None:
                now = datetime.now()
                cron = croniter.croniter(cron_str, now)
                dt = cron.get_next(datetime)
                await asyncio.sleep((dt-now).total_seconds())
                while True:
                    try:
                        if is_coroutine:
                            await func()
                        else:
                            await run_in_threadpool(func)
                    except Exception as exc:
                        formatted_exception = "".join(format_exception(type(exc), exc, exc.__traceback__))
                        logger.error(formatted_exception)
                    await asyncio.sleep((dt-now).total_seconds())
            ensure_future(loop())
        return wrapped
    return decorator
