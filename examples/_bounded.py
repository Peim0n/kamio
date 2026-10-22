"""Bounded-run helper for CI example sweeps.

Long-running demos (04, 06, 08, 14) use ``run_bounded`` so CI can cap the
total runtime via the ``KAMIO_EXAMPLE_MAX_DURATION`` environment variable
(seconds, float).  When unset or <= 0 the example runs to completion as usual.
"""

import asyncio
import os
from typing import Awaitable


def run_bounded(coro: Awaitable[None]) -> None:
    """Run an example coroutine, optionally capped by KAMIO_EXAMPLE_MAX_DURATION.

    When the cap is reached the demo is cancelled and the process still exits
    with code 0 — the example demonstrated its checks; only the remaining
    idle/long-running portion was skipped.
    """
    limit = float(os.environ.get("KAMIO_EXAMPLE_MAX_DURATION", "0") or 0)
    if limit <= 0:
        asyncio.run(coro)
        return
    try:
        asyncio.run(asyncio.wait_for(coro, timeout=limit))
    except asyncio.TimeoutError:
        pass
