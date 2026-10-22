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
            # If a signal handler scheduled stop() concurrently, wait for it
            # to finish — otherwise returning here lets asyncio.run() cancel
            # the in-flight shutdown mid-flight.
            if stop_task is not None and not stop_task.done():
                try:
                    await asyncio.shield(stop_task)
                except asyncio.CancelledError:
                    # We were cancelled but the shielded stop keeps running;
                    # drain it so shutdown completes.
                    await asyncio.gather(stop_task, return_exceptions=True)
            await self.stop()

    def run(self: Any) -> None:
        """
        Start the application and block until shutdown.

        Handles ``SIGINT`` / ``SIGTERM`` for graceful shutdown.
        """
        coro = self._run_async()
        try:
            asyncio.run(coro)
        except (KeyboardInterrupt, SystemExit):
            logger.info("Application interrupted")
        except Exception as e:
            logger.exception(f"Application crashed: {e}")
        finally:
            # asyncio.run() may fail before ever scheduling the coroutine
            # (e.g. a loop is already running) — close it to avoid
            # "coroutine was never awaited" warnings.
            coro.close()

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

        try:
            if not self.mqtt_client.is_connected:
                if self._mqtt_conn is None:
                    raise RuntimeError(
                        "MQTT client is not connected and no MqttConnection configured"
                    )
                await self._mqtt_conn.connect()

            await self.server_node.start()
            # The app owns the shared broadcast wildcard for this client.
            await self._subscribe_broadcast()

            # Snapshot device nodes so concurrent add/remove does not mutate the
            # dict while we iterate during startup.
            for node in list(self._device_nodes.values()):
                await node.start()

            await self.rules.start()
            await self.custom_nodes.start_all()
            # Activate hot reload if enable() was deferred because no event
            # loop was running yet (SCAN-11).
            self.hot_reload.activate_if_pending()
        except Exception as e:
            # Roll back whatever did start: stop nodes, unload plugins,
            # disconnect MQTT — a half-started app must not leak resources.
            logger.error(f"KamioApp startup failed, rolling back: {e}", exc_info=True)
            await self._teardown()
            raise

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
        await self._teardown()

        logger.info("KamioApp stopped")
        try:
            await self.hooks.trigger("on_after_stop")
            await self.event_bus.publish("app_stop", {})
        except Exception as e:
            logger.error(f"Error during post-stop hooks/events: {e}", exc_info=True)

    async def _teardown(self: Any) -> None:
        """Stop every started component and disconnect MQTT.

        Shared by ``stop()`` and the ``start()`` rollback path; works even
        when the app never reached the running state.  Each cleanup step is
        wrapped individually so a failure in one step cannot skip the
        remaining cleanup and leak resources (tasks, connections,
        subscriptions).
        """
        timeout = getattr(self, "shutdown_timeout", self.shutdown_timeout)

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

        # Unload plugins after nodes stop but before MQTT disconnect so
        # PluginContext tasks/subscriptions do not outlive shutdown.
        await _safe("plugins.unload_all", self.plugin_loader.unload_all())

        # Stop file watchers activated by a deferred hot-reload enable.
        await _safe("hot_reload.disable", self.hot_reload.disable())

        # Await any lingering background tasks scheduled by the MQTT
        # dispatcher / fire-and-forget helpers so they do not outlive the
        # app (and so their resources are released before disconnect).
        await _safe("bg_tasks", self._await_bg_tasks(timeout=timeout))

        if self._mqtt_conn is not None:
            await _safe("mqtt.disconnect", self._mqtt_conn.disconnect())
        elif self.mqtt_client.is_connected:
            await _safe("mqtt_client.disconnect", self.mqtt_client.disconnect())

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
