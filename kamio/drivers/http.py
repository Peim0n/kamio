from __future__ import annotations

try:
    import aiohttp
except ImportError:
    aiohttp = None  # type: ignore

from typing import Any, Dict, Iterable, Optional

from .base import BaseDriver


class HTTPDeviceDriver(BaseDriver):
    """
    HTTP driver for IP cameras, smart displays, and REST APIs.

    Network and HTTP errors are raised (``aiohttp.ClientError`` /
    :class:`aiohttp.ClientResponseError`) rather than masked as a ``dict`` so
    callers can rely on the standard ``try/except`` contract of
    :class:`BaseDriver`.
    """

    COMMANDS = ("request", "get", "post", "put", "patch", "delete")

    def __init__(
        self,
        base_url: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: float = 10.0,
        *,
        commands: Optional[Iterable[str]] = None,
    ):
        super().__init__(commands=commands)
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
        """GET ``params["path"]`` and return the parsed response."""
        params = params or {}
        if "path" not in params:
            raise NotImplementedError("HTTPDeviceDriver.read requires params['path']")
        code, result = await self._request("GET", self._build_url(params))
        return {"status": "ok", "code": code, "field": field_name, "data": result}

    async def execute(self, command_name: str, params: dict) -> dict:
        """Transport commands: ``get``/``post``/``put``/``patch``/``delete``, or
        ``request`` with ``params["method"]``.

        ``params``: ``path`` (relative to ``base_url``), ``json`` (dict body),
        ``data`` (raw body).
        """
        params = params or {}
        if command_name == "request":
            method = str(params.get("method", "")).upper()
            if not method:
                raise ValueError("'request' requires params['method']")
        else:
            method = command_name.upper()
        code, result = await self._request(
            method,
            self._build_url(params),
            data=params.get("data"),
            json=params.get("json"),
        )
        return {"status": "ok", "code": code, "command": command_name, "data": result}

    def _build_url(self, params: Dict[str, Any]) -> str:
        """Join ``base_url`` with ``params["path"]`` (default: ``base_url`` itself)."""
        path = str(params.get("path", "")).lstrip("/")
        return f"{self.base_url}/{path}" if path else self.base_url

    async def _request(self, method: str, url: str, **kwargs: Any) -> tuple[int, Any]:
        """Issue the request and return ``(status_code, parsed_body)``.

        The body is decoded as JSON when the content type is ``application/json``
        and as text otherwise. HTTP errors are raised via ``raise_for_status``.
        """
        if not self.session:
            raise RuntimeError("HTTP session not connected")
        async with self.session.request(method, url, **kwargs) as response:
            response.raise_for_status()
            content_type = getattr(response, "content_type", "") or ""
            is_json = content_type.startswith("application/json")
            result = await response.json() if is_json else await response.text()
            return response.status, result
