from __future__ import annotations

import asyncio
import logging
import signal
from typing import Any, List, Optional

logger = logging.getLogger("Kamio.app")


class LifecycleMixin:
    """Application lifecycle: start, stop, run, and signal handling."""

    _is_running: bool
    _loop: Optional[asyncio.AbstractEventLoop]

    #: Default per-stage shutdown timeout (seconds).  Override on KamioApp to
    #: tune graceful-shutdown deadlines for large device fleets.
    shutdown_timeout: float = 5.0

    @property
    def is_running(self: Any) -> bool:
        """Returns True if the application is currently running."""
        result: bool = self._is_running
        return result

    async def _run_async(self: Any):
        """Internal async runner with signal handling."""
        loop = asyncio.get_running_loop()
        registered_signals: List[signal.Signals] = []
        stop_task: Optional[asyncio.Task] = None

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:

                def _signal_handler():
                    nonlocal stop_task
                    if stop_task is None or stop_task.done():
                        stop_task = loop.create_task(self.stop())

                loop.add_signal_handler(sig, _signal_handler)
                registered_signals.append(sig)
            except (NotImplementedError, ValueError):
                pass

        try:
            await self.start()
            while self._is_running:
                await asyncio.sleep(1)
        finally:
            for sig in registered_signals:
                try:
                    loop.remove_signal_handler(sig)
                except Exception:
                    pass
            await self.stop()

    def run(self: Any) -> None:
        """
        Start the application and block until shutdown.

        Handles ``SIGINT`` / ``SIGTERM`` for graceful shutdown.
        """
        try:
            asyncio.run(self._run_async())
        except (KeyboardInterrupt, SystemExit):
            logger.info("Application interrupted")
        except Exception as e:
            logger.exception(f"Application crashed: {e}")

    async def start(self: Any):
        """Start the application: connect to MQTT broker and initialize all nodes."""
        if self._is_running:
            return

        self._loop = asyncio.get_running_loop()
        # Ensure the background-task sets exist so attribute access is uniform.
        for attr in ("_bg_tasks", "_mqtt_bg_tasks"):
            if getattr(self, attr, None) is None:
                setattr(self, attr, set())
        logger.info("KamioApp starting...")
        await self.hooks.trigger("on_before_start")

        if not self.mqtt_client.is_connected:
            if self._mqtt_conn is None:
                raise RuntimeError("MQTT client is not connected and no MqttConnection configured")
            await self._mqtt_conn.connect()

        await self.server_node.start()

        # Snapshot device nodes so concurrent add/remove does not mutate the
        # dict while we iterate during startup.
        for node in list(self._device_nodes.values()):
            await node.start()

        await self.rules.start()
        await self.custom_nodes.start_all()
        # Mark as running only after all components have started successfully.
        self._is_running = True
        logger.info("KamioApp started")
        await self.hooks.trigger("on_after_start")
        await self.event_bus.publish("app_start", {})

    async def stop(self: Any):
        """Stop the application gracefully."""
        if not self._is_running:
            return

        logger.info("KamioApp stopping...")
        await self.hooks.trigger("on_before_stop")
        self._is_running = False
        timeout = getattr(self, "shutdown_timeout", self.shutdown_timeout)

        # Each cleanup step is wrapped in its own try/except so that a
        # failure in one step does not skip the remaining cleanup, which
        # would leak resources (tasks, connections, subscriptions).
        async def _safe(step: str, coro):
            try:
                await asyncio.wait_for(coro, timeout=timeout)
            except Exception as e:
                logger.error(f"Error during shutdown step '{step}': {e}", exc_info=True)

        await _safe("rules.stop", self.rules.stop())
        await _safe("custom_nodes.stop_all", self.custom_nodes.stop_all())

        if self._device_nodes:
            stop_tasks = [node.stop() for node in list(self._device_nodes.values())]
            await _safe("device_nodes.stop", asyncio.gather(*stop_tasks, return_exceptions=True))

        await _safe("server_node.stop", self.server_node.stop())

        # Await any lingering background tasks scheduled by the MQTT
        # dispatcher / fire-and-forget helpers so they do not outlive the
        # app (and so their resources are released before disconnect).
        await _safe("bg_tasks", self._await_bg_tasks(timeout=timeout))

        if self._mqtt_conn is not None:
            await _safe("mqtt.disconnect", self._mqtt_conn.disconnect())
        elif self.mqtt_client.is_connected:
            await _safe("mqtt_client.disconnect", self.mqtt_client.disconnect())

        logger.info("KamioApp stopped")
        try:
            await self.hooks.trigger("on_after_stop")
            await self.event_bus.publish("app_stop", {})
        except Exception as e:
            logger.error(f"Error during post-stop hooks/events: {e}", exc_info=True)

    async def _await_bg_tasks(self: Any, timeout: float) -> None:
        """Wait for background tasks in ``_bg_tasks`` / ``_mqtt_bg_tasks`` to finish.

        Tasks that do not complete within ``timeout`` are cancelled and awaited
        so they cannot leak past shutdown.
        """
        pending: list = []
        for attr in ("_bg_tasks", "_mqtt_bg_tasks"):
            tasks = getattr(self, attr, None)
            if not tasks:
                continue
            pending.extend(t for t in list(tasks) if not t.done())
        if not pending:
            return
        try:
            await asyncio.wait_for(
                asyncio.gather(*pending, return_exceptions=True), timeout=timeout
            )
        except asyncio.TimeoutError:
            for task in pending:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
