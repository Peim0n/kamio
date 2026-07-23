from __future__ import annotations
try:
    import aiohttp
except ImportError:
    aiohttp = None  # type: ignore

from typing import Any, Optional, Dict
from .base import BaseDriver

class HTTPDeviceDriver(BaseDriver):
    """
    HTTP driver for IP cameras, smart displays, and REST APIs.
    """
    def __init__(self, base_url: str, headers: Optional[Dict[str, str]] = None, timeout: float = 10.0):
        super().__init__()
        self.base_url = base_url.rstrip("/")
        self.headers = headers or {}
        self.timeout = timeout
        self.session: Optional["aiohttp.ClientSession"] = None

    async def connect(self):
        if aiohttp is None:
            raise ImportError("aiohttp is required for HTTPDeviceDriver. Install it: pip install aiohttp")
        self.session = aiohttp.ClientSession(headers=self.headers, timeout=aiohttp.ClientTimeout(total=self.timeout))
        self.logger.info(f"HTTP session initialized for {self.base_url}")

    async def disconnect(self):
        if self.session:
            await self.session.close()
            self.session = None
            self.logger.info("HTTP session closed")

    async def read(self, field_name: str, params: Optional[Dict[str, Any]] = None) -> Any:
        if not self.session:
            raise RuntimeError("HTTP session not connected")
        params = params or {}
        path = params.get("path", field_name).lstrip("/")
        url = f"{self.base_url}/{path}" if path else self.base_url
        try:
            async with self.session.request("GET", url) as response:
                result = await response.json() if response.content_type == "application/json" else await response.text()
                return {
                    "status": "ok" if response.status < 400 else "error",
                    "code": response.status,
                    "field": field_name,
                    "data": result
                }
        except Exception as e:
            self.logger.error(f"HTTP read failed: {e}")
            return {"status": "error", "field": field_name, "message": str(e)}

    async def execute(self, command_name: str, params: dict) -> dict:
        if not self.session:
            raise RuntimeError("HTTP session not connected")

        params = params or {}
        method = params.get("method", "POST").upper()
        path = params.get("path", command_name).lstrip("/")
        url = f"{self.base_url}/{path}"
        data = params.get("data")
        json_data = params.get("json") if "json" in params else params.get("value")

        try:
            async with self.session.request(method, url, data=data, json=json_data if isinstance(json_data, dict) else None) as response:
                result = await response.json() if response.content_type == "application/json" else await response.text()
                return {
                    "status": "ok" if response.status < 400 else "error",
                    "code": response.status,
                    "command": command_name,
                    "data": result
                }
        except Exception as e:
            self.logger.error(f"HTTP request failed: {e}")
            return {"status": "error", "command": command_name, "message": str(e)}
