"""
Request logging utility for MCP tools.

Logs all tool invocations to JSON files for audit and analytics purposes.
"""

import json
import logging
import time
import pathlib
from datetime import datetime, timezone
from typing import Any, Dict

logger = logging.getLogger(__name__)


def log_request(
    requests_dir: pathlib.Path,
    requester: str,
    tool_name: str,
    input_params: Dict[str, Any],
    output_result: Any
) -> pathlib.Path:
    """
    Log a tool request to a JSON file.

    Args:
        requests_dir: Directory to store request logs
        requester: Identifier of who called the tool
        tool_name: Name of the MCP tool invoked
        input_params: Dictionary of input parameters passed to the tool
        output_result: The result returned by the tool

    Returns:
        Path to the created log file
    """
    # Generate timestamp in milliseconds
    datetime_ms = int(time.time() * 1000)

    # Sanitize requester for filename (remove unsafe characters)
    safe_requester = "".join(c if c.isalnum() or c in "-_" else "_" for c in requester)

    # Build filename
    filename = f"{datetime_ms}-{tool_name}-{safe_requester}.json"
    filepath = requests_dir / filename

    # Build log record
    record = {
        "timestamp_ms": datetime_ms,
        "timestamp_iso": datetime.now(timezone.utc).isoformat(),
        "requester": requester,
        "tool_name": tool_name,
        "input_params": input_params,
        "output_result": _serialize_output(output_result)
    }

    # Write to file
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(record, f, indent=2, default=str, ensure_ascii=False)
        logger.debug(f"Logged request to {filename}")
    except Exception as e:
        logger.error(f"Failed to log request: {e}")
        # Don't raise - logging failure shouldn't break the tool

    return filepath


def _serialize_output(output: Any) -> Any:
    """
    Serialize output for JSON storage.

    Handles special cases like large strings, lists with ImageContent, etc.
    """
    if output is None:
        return None

    if isinstance(output, str):
        # Truncate very large strings to prevent huge log files
        max_length = 50000  # 50KB max for output strings
        if len(output) > max_length:
            return output[:max_length] + f"\n... [TRUNCATED, total length: {len(output)}]"
        return output

    if isinstance(output, list):
        # Handle list outputs (e.g., tools returning [ImageContent, str])
        serialized = []
        for item in output:
            if hasattr(item, '__class__') and 'ImageContent' in item.__class__.__name__:
                serialized.append({"type": "ImageContent", "note": "Image data omitted"})
            elif isinstance(item, str):
                serialized.append(_serialize_output(item))
            else:
                serialized.append(str(item))
        return serialized

    if isinstance(output, dict):
        return output

    # Fallback: convert to string
    return str(output)
