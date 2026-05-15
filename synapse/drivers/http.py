import aiohttp
import logging
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
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.session: Optional[aiohttp.ClientSession] = None
        self.logger = logging.getLogger("synapse.driver.http")

    async def connect(self):
        self.session = aiohttp.ClientSession(headers=self.headers, timeout=self.timeout)
        self.logger.info(f"HTTP session initialized for {self.base_url}")

    async def disconnect(self):
        if self.session:
            await self.session.close()
            self.session = None
            self.logger.info("HTTP session closed")

    async def execute(self, command_name: str, params: dict) -> dict:
        if not self.session:
            raise RuntimeError("HTTP session not connected")
        
        method = params.get("method", "POST").upper()
        path = params.get("path", "").lstrip("/")
        url = f"{self.base_url}/{path}"
        data = params.get("data")
        json_data = params.get("json")

        try:
            async with self.session.request(method, url, data=data, json=json_data) as response:
                result = await response.json() if response.content_type == "application/json" else await response.text()
                return {
                    "status": "ok" if response.status < 400 else "error",
                    "code": response.status,
                    "data": result
                }
        except Exception as e:
            self.logger.error(f"HTTP request failed: {e}")
            return {"status": "error", "message": str(e)}

    async def read(self, field_name: str) -> Any:
        # Assuming field_name maps to a GET endpoint
        result = await self.execute("read", {"method": "GET", "path": field_name})
        if result["status"] == "ok":
            return result["data"]
        return None
