import os
import json
import logging
from typing import Any, Dict, Optional

class Config:
    """
    Configuration management for Synapse Core.
    Supports environment variables and JSON config files.
    """
    def __init__(self, config_path: Optional[str] = None):
        self.data: Dict[str, Any] = {}
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                self.data = json.load(f)
        
        self.logger = logging.getLogger("synapse.config")

    def get(self, key: str, default: Any = None) -> Any:
        # Priority: Environment Variable > Config File > Default
        env_key = f"SYNAPSE_{key.upper()}"
        if env_key in os.environ:
            return os.environ[env_key]
        
        return self.data.get(key, default)

    @property
    def mqtt_broker(self) -> str:
        return self.get("mqtt_broker", "mqtt://localhost:1883")

    @property
    def log_level(self) -> int:
        level_str = self.get("log_level", "INFO").upper()
        return getattr(logging, level_str, logging.INFO)
