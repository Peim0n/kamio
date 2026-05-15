# CHANGELOG

## [1.1.0] - 2026-05-14

### Fixed
- **MQTT Topic Mismatch**: Fixed issue where nodes subscribed to legacy topics but published to current ones. Now nodes subscribe to both formats for backward compatibility.
- **Command Invocation TypeError**: Fixed `TypeError` when calling device commands that don't accept the `node` keyword argument. Added signature inspection to safely pass `node` only when requested.
- **Missing Imports**: Fixed `synapse/core/__init__.py` to correctly export all internal components required by `SynapseApp`.
- **Global Dispatcher Inefficiency**: Added topic-based filtering in `BaseNode` to prevent every device from processing every message received by the application.
- **Dependency Management**: Added `pyproject.toml` for proper installation and dependency tracking.

### Added
- **Comprehensive Test Suite**: Added unit and integration tests covering core functionality, metaclasses, command handling, and rules.
- **New Examples**: Added `examples/smart_home.py` demonstrating multi-device interaction.
- **Russian Documentation**: Added complete professional documentation in Russian.

### Improved
- **Metaclass Resilience**: Improved `DeviceMeta` to better handle inheritance and type hints.
- **Error Logging**: Enhanced logging across the framework for better debugging.
- **Graceful Shutdown**: Improved task cancellation and MQTT disconnection logic.
