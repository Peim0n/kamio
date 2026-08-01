from __future__ import annotations

try:
    import aiohttp
except ImportError:
    aiohttp = None  # type: ignore

from typing import Any, Dict, Optional

from .base import BaseDriver


class HTTPDeviceDriver(BaseDriver):
    """
    HTTP driver for IP cameras, smart displays, and REST APIs.

    Network and HTTP errors are raised (``aiohttp.ClientError`` /
    :class:`aiohttp.ClientResponseError`) rather than masked as a ``dict`` so
    callers can rely on the standard ``try/except`` contract of
    :class:`BaseDriver`.
    """

    def __init__(
        self, base_url: str, headers: Optional[Dict[str, str]] = None, timeout: float = 10.0
    ):
        super().__init__()
        self.base_url = base_url.rstrip("/")
        self.headers = headers or {}
        self.timeout = timeout
        self.session: Optional["aiohttp.ClientSession"] = None

    async def connect(self):
        """Create the aiohttp.ClientSession. Called automatically on first use if not called explicitly."""
        if aiohttp is None:
            raise ImportError(
                "aiohttp is required for HTTPDeviceDriver. Install it: pip install aiohttp"
            )
        self.session = aiohttp.ClientSession(
            headers=self.headers, timeout=aiohttp.ClientTimeout(total=self.timeout)
        )
        self.logger.info(f"HTTP session initialized for {self.base_url}")

    async def disconnect(self):
        """Close the aiohttp.ClientSession if open."""
        if self.session:
            try:
                await self.session.close()
            except Exception as e:
                self.logger.warning(f"Error closing HTTP session: {e}")
            finally:
                self.session = None
                self.logger.info("HTTP session closed")

    async def read(self, field_name: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Perform an HTTP GET request and return the parsed response."""
        if not self.session:
            raise RuntimeError("HTTP session not connected")
        params = params or {}
        path = params.get("path", field_name).lstrip("/")
        url = f"{self.base_url}/{path}" if path else self.base_url
        async with self.session.request("GET", url) as response:
            response.raise_for_status()
            content_type = getattr(response, "content_type", "") or ""
            is_json = content_type.startswith("application/json")
            result = await response.json() if is_json else await response.text()
            return {
                "status": "ok",
                "code": response.status,
                "field": field_name,
                "data": result,
            }

    async def execute(self, command_name: str, params: dict) -> dict:
        """Perform an HTTP request (GET/POST/etc.) and return the parsed response."""
        if not self.session:
            raise RuntimeError("HTTP session not connected")

        params = params or {}
        method = params.get("method", "POST").upper()
        path = params.get("path", command_name).lstrip("/")
        url = f"{self.base_url}/{path}"
        data = params.get("data")
        json_data = params.get("json") if "json" in params else params.get("value")

        async with self.session.request(
            method, url, data=data, json=json_data if isinstance(json_data, dict) else None
        ) as response:
            response.raise_for_status()
            content_type = getattr(response, "content_type", "") or ""
            is_json = content_type.startswith("application/json")
            result = await response.json() if is_json else await response.text()
            return {
                "status": "ok",
                "code": response.status,
                "command": command_name,
                "data": result,
            }
