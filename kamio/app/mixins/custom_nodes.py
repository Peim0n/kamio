from __future__ import annotations
from typing import Any, List, Optional

from kamio.core.custom_nodes import CustomNode


class CustomNodeFacadeMixin:
    """Shortcuts for custom MQTT node registration."""

    def register_custom_node(self: Any, name: str, node: CustomNode) -> None:
        """Register a custom MQTT node."""
        self.custom_nodes.register_node(name, node)

    def unregister_custom_node(self: Any, name: str) -> None:
        """Unregister a custom MQTT node by name."""
        self.custom_nodes.unregister_node(name)

    def get_custom_node(self: Any, name: str) -> Optional[CustomNode]:
        """Return a registered custom node by name, or None."""
        return self.custom_nodes.get_node(name)

    def list_custom_nodes(self: Any) -> List[str]:
        """Return names of all registered custom nodes."""
        return self.custom_nodes.list_nodes()
