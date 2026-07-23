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

    @property
    def is_running(self: Any) -> bool:
        """Returns True if the application is currently running."""
        return self._is_running

    async def _run_async(self: Any):
        """Internal async runner with signal handling."""
        loop = asyncio.get_running_loop()
        registered_signals: List[signal.Signals] = []

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))
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
        logger.info("KamioApp starting...")
        await self.hooks.trigger("on_before_start")

        if not self.mqtt_client.is_connected:
            if self._mqtt_conn is None:
                raise RuntimeError("MQTT client is not connected and no MqttConnection configured")
            await self._mqtt_conn.connect()

        self._is_running = True
        await self.server_node.start()

        for node in self._device_nodes.values():
            await node.start()

        await self.rules.start()
        await self.custom_nodes.start_all()
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

        try:
            await asyncio.wait_for(self.rules.stop(), timeout=5.0)
            await asyncio.wait_for(self.custom_nodes.stop_all(), timeout=5.0)

            if self._device_nodes:
                stop_tasks = [node.stop() for node in self._device_nodes.values()]
                await asyncio.wait_for(
                    asyncio.gather(*stop_tasks, return_exceptions=True), timeout=5.0
                )

            await asyncio.wait_for(self.server_node.stop(), timeout=5.0)

            if self._mqtt_conn is not None:
                await self._mqtt_conn.disconnect()
            elif self.mqtt_client.is_connected:
                await self.mqtt_client.disconnect()

            logger.info("KamioApp stopped")
            await self.hooks.trigger("on_after_stop")
            await self.event_bus.publish("app_stop", {})
        except Exception as e:
            logger.error(f"Error during shutdown: {e}", exc_info=True)
