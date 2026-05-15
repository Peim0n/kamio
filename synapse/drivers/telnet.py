import asyncio
import logging
from typing import Any, Optional
from .base import BaseDriver

class TelnetDriver(BaseDriver):
    """
    Telnet driver for legacy industrial equipment.
    """
    def __init__(self, host: str, port: int = 23, timeout: float = 5.0):
        super().__init__()
        self.host = host
        self.port = port
        self.timeout = timeout
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.logger = logging.getLogger("synapse.driver.telnet")

    async def connect(self):
        try:
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port),
                timeout=self.timeout
            )
            self.logger.info(f"Connected to Telnet {self.host}:{self.port}")
        except Exception as e:
            self.logger.error(f"Telnet connection failed to {self.host}:{self.port}: {e}")
            raise

    async def disconnect(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
            self.reader = None
            self.writer = None
            self.logger.info("Telnet disconnected")

    async def execute(self, command_name: str, params: dict) -> dict:
        if not self.writer:
            raise RuntimeError("Telnet not connected")
        
        cmd = params.get("command", "")
        if not cmd.endswith("\n"):
            cmd += "\n"
            
        self.writer.write(cmd.encode())
        await self.writer.drain()
        
        # Read response if expected
        response = ""
        if params.get("wait_response", True):
            try:
                line = await asyncio.wait_for(self.reader.readline(), timeout=self.timeout)
                response = line.decode().strip()
            except asyncio.TimeoutError:
                self.logger.warning("Telnet read timeout")
        
        return {"status": "ok", "response": response}

    async def read(self, field_name: str) -> Any:
        # For telnet, reading usually involves sending a query command
        # This is a simplified implementation
        result = await self.execute("query", {"command": f"GET {field_name}", "wait_response": True})
        return result.get("response")
