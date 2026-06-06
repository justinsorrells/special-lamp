"""Shared v1 newline-JSON protocol contract primitives.

This module intentionally contains no socket, Redis, firmware, or GUI
integration. It defines the frozen message vocabulary, validation helpers,
framing limits, sequence-number helpers, and pop-wins pending resolution used by
later controller components.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import json
from typing import Any, MutableMapping, TypeVar


PROTOCOL_VERSION = "1"
CONTROLLER_MAX_LINE_BYTES = 8 * 1024
BOARD_MAX_LINE_BYTES = 1 * 1024


class MessageType(StrEnum):
    COMMAND = "command"
    RESPONSE = "response"
    TELEMETRY = "telemetry"
    SCHEMA = "schema"
    EVENT = "event"
    ESTOP = "estop"
    ESTOP_RESET = "estop_reset"
    HEARTBEAT = "heartbeat"


class TerminalStatus(StrEnum):
    OK = "ok"
    ERROR = "error"
    TIMEOUT = "timeout"


class ErrorCode(StrEnum):
    INVALID_JSON = "INVALID_JSON"
    MISSING_FIELD = "MISSING_FIELD"
    INVALID_TYPE = "INVALID_TYPE"
    UNKNOWN_TARGET = "UNKNOWN_TARGET"
    UNKNOWN_COMMAND = "UNKNOWN_COMMAND"
    INVALID_ARGUMENT = "INVALID_ARGUMENT"
    BOARD_UNAVAILABLE = "BOARD_UNAVAILABLE"
    BOARD_BUSY = "BOARD_BUSY"
    COMMAND_TIMEOUT = "COMMAND_TIMEOUT"
    ESTOP_ACTIVE = "ESTOP_ACTIVE"
    PROTOCOL_VERSION_MISMATCH = "PROTOCOL_VERSION_MISMATCH"
    CONTROLLER_SHUTDOWN = "CONTROLLER_SHUTDOWN"
    INTERNAL_ERROR = "INTERNAL_ERROR"


TERMINAL_STATUSES = frozenset(status.value for status in TerminalStatus)
ERROR_CODES = frozenset(code.value for code in ErrorCode)


@dataclass(frozen=True)
class ErrorObject:
    code: ErrorCode
    message: str

    def as_dict(self) -> dict[str, str]:
        return {"code": self.code.value, "message": self.message}


class ProtocolValidationError(ValueError):
    """Validation failure represented as a v1 structured error object."""

    def __init__(self, code: ErrorCode, message: str):
        super().__init__(message)
        self.error = ErrorObject(code, message)


@dataclass(frozen=True)
class ParseResult:
    message: dict[str, Any] | None
    error: ErrorObject | None

    @property
    def ok(self) -> bool:
        return self.error is None


T = TypeVar("T")


def make_error(code: ErrorCode, message: str) -> dict[str, str]:
    return ErrorObject(code, message).as_dict()


def require_terminal_status(value: Any) -> TerminalStatus:
    try:
        return TerminalStatus(value)
    except ValueError as exc:
        raise ProtocolValidationError(
            ErrorCode.INVALID_TYPE,
            f"invalid terminal status {value!r}",
        ) from exc


def require_error_code(value: Any) -> ErrorCode:
    try:
        return ErrorCode(value)
    except ValueError as exc:
        raise ProtocolValidationError(
            ErrorCode.INVALID_TYPE,
            f"invalid error code {value!r}",
        ) from exc


def parse_line(line: bytes, *, max_line_bytes: int = CONTROLLER_MAX_LINE_BYTES) -> dict[str, Any]:
    """Parse one newline-terminated JSON object within the receiver's line limit."""

    if len(line) > max_line_bytes:
        raise ProtocolValidationError(
            ErrorCode.INVALID_JSON,
            f"line exceeds receive limit of {max_line_bytes} bytes",
        )
    if not line.endswith(b"\n"):
        raise ProtocolValidationError(
            ErrorCode.INVALID_JSON,
            "line is not newline terminated",
        )
    try:
        decoded = line[:-1].decode("utf-8")
        message = json.loads(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolValidationError(
            ErrorCode.INVALID_JSON,
            f"invalid JSON line: {exc}",
        ) from exc
    if not isinstance(message, dict):
        raise ProtocolValidationError(
            ErrorCode.INVALID_TYPE,
            "message must be a JSON object",
        )
    return message


def parse_message(
    line: bytes,
    *,
    max_line_bytes: int = CONTROLLER_MAX_LINE_BYTES,
) -> ParseResult:
    try:
        message = parse_line(line, max_line_bytes=max_line_bytes)
        validate_message(message)
    except ProtocolValidationError as exc:
        return ParseResult(message=None, error=exc.error)
    return ParseResult(message=message, error=None)


def serialize_message(
    message: dict[str, Any],
    *,
    max_line_bytes: int | None = None,
) -> bytes:
    validate_message(message)
    encoded = (json.dumps(message, separators=(",", ":")) + "\n").encode("utf-8")
    if max_line_bytes is not None and len(encoded) > max_line_bytes:
        raise ProtocolValidationError(
            ErrorCode.INVALID_JSON,
            f"serialized message exceeds receiver limit of {max_line_bytes} bytes",
        )
    return encoded


def validate_message(message: dict[str, Any]) -> None:
    msg_type = _required_str(message, "type")
    try:
        kind = MessageType(msg_type)
    except ValueError as exc:
        raise ProtocolValidationError(
            ErrorCode.INVALID_TYPE,
            f"unsupported message type {msg_type!r}",
        ) from exc

    if kind is MessageType.COMMAND:
        _validate_command(message)
    elif kind is MessageType.RESPONSE:
        _validate_response(message)
    elif kind is MessageType.TELEMETRY:
        _validate_telemetry(message)
    elif kind is MessageType.SCHEMA:
        _validate_schema(message)
    elif kind is MessageType.EVENT:
        _validate_event(message)
    elif kind is MessageType.ESTOP:
        _validate_estop(message)
    elif kind is MessageType.ESTOP_RESET:
        _validate_estop_reset(message)
    elif kind is MessageType.HEARTBEAT:
        _validate_heartbeat(message)


def command_blocked_by_estop(command_meta: dict[str, Any]) -> bool:
    """Return the fail-safe e-stop gate value for one schema command entry."""

    value = command_meta.get("blocked_by_estop", True)
    if not isinstance(value, bool):
        raise ProtocolValidationError(
            ErrorCode.INVALID_TYPE,
            "blocked_by_estop must be a boolean when present",
        )
    return value


def extract_blocked_by_estop(schema_message: dict[str, Any]) -> dict[str, bool]:
    validate_message(schema_message)
    commands = schema_message["schema"].get("commands", {})
    if not isinstance(commands, dict):
        raise ProtocolValidationError(
            ErrorCode.INVALID_TYPE,
            "schema.commands must be an object",
        )
    return {
        command_name: command_blocked_by_estop(command_meta)
        for command_name, command_meta in commands.items()
        if isinstance(command_name, str) and isinstance(command_meta, dict)
    }


def check_protocol_version(
    schema_message: dict[str, Any],
    *,
    expected_version: str = PROTOCOL_VERSION,
) -> None:
    """Validate a schema message against the controller's expected protocol version."""

    validate_message(schema_message)
    if schema_message["type"] != MessageType.SCHEMA.value:
        raise ProtocolValidationError(ErrorCode.INVALID_TYPE, "expected schema message")
    actual_version = schema_message["protocol_version"]
    if actual_version != expected_version:
        raise ProtocolValidationError(
            ErrorCode.PROTOCOL_VERSION_MISMATCH,
            f"protocol version mismatch: expected {expected_version}, got {actual_version}",
        )


def build_board_command(
    client_command: dict[str, Any],
    *,
    board_seq: int,
    board_id: str,
    controller_ts: float,
    controller_source: str = "controller",
) -> dict[str, Any]:
    """Rewrite a client command onto the board-facing hop."""

    validate_message(client_command)
    if client_command["type"] != MessageType.COMMAND.value:
        raise ProtocolValidationError(ErrorCode.INVALID_TYPE, "expected command message")
    _validate_uint64(board_seq, "board_seq")
    if not isinstance(controller_ts, int | float):
        raise ProtocolValidationError(ErrorCode.INVALID_TYPE, "controller_ts must be numeric")
    return {
        "type": MessageType.COMMAND.value,
        "seq": board_seq,
        "controller_ts": controller_ts,
        "source": controller_source,
        "target": board_id,
        "command": client_command["command"],
        "args": client_command["args"],
    }


def build_client_response(
    board_response: dict[str, Any],
    *,
    client_seq: int,
    client_target: str,
    board_seq: int,
    latency_ms: float | None = None,
) -> dict[str, Any]:
    """Restore the client seq and expose the controller-owned board_seq."""

    validate_message(board_response)
    if board_response["type"] != MessageType.RESPONSE.value:
        raise ProtocolValidationError(ErrorCode.INVALID_TYPE, "expected response message")
    _validate_uint64(client_seq, "client_seq")
    _validate_uint64(board_seq, "board_seq")

    status = TerminalStatus(board_response["status"])
    if status is TerminalStatus.OK:
        result = board_response.get("result")
        if result is None:
            result = {}
        elif not isinstance(result, dict):
            raise ProtocolValidationError(ErrorCode.INVALID_TYPE, "response result must be an object or null")
        result = dict(result)
        result["board_seq"] = board_seq
        if latency_ms is not None:
            result["latency_ms"] = latency_ms
    else:
        result = None

    return {
        "type": MessageType.RESPONSE.value,
        "seq": client_seq,
        "controller_ts": board_response.get("controller_ts"),
        "source": "controller",
        "target": client_target,
        "status": status.value,
        "result": result,
        "error": board_response.get("error"),
    }


def build_error_response(
    *,
    seq: int,
    target: str,
    code: ErrorCode,
    message: str,
    status: TerminalStatus = TerminalStatus.ERROR,
) -> dict[str, Any]:
    if status is TerminalStatus.OK:
        raise ProtocolValidationError(ErrorCode.INVALID_TYPE, "error responses cannot use ok status")
    if status is TerminalStatus.TIMEOUT and code is not ErrorCode.COMMAND_TIMEOUT:
        raise ProtocolValidationError(ErrorCode.INVALID_TYPE, "timeout status must carry COMMAND_TIMEOUT")
    if code is ErrorCode.COMMAND_TIMEOUT:
        status = TerminalStatus.TIMEOUT
    return {
        "type": MessageType.RESPONSE.value,
        "seq": seq,
        "source": "controller",
        "target": target,
        "status": status.value,
        "result": None,
        "error": make_error(code, message),
    }


def build_estop_message(*, target: str, source: str = "controller") -> dict[str, str]:
    return {
        "type": MessageType.ESTOP.value,
        "source": source,
        "target": target,
    }


def is_estop_ack_event(message: dict[str, Any]) -> bool:
    validate_message(message)
    return (
        message["type"] == MessageType.EVENT.value
        and message.get("event") == "estop_ack"
        and message.get("details", {}).get("state") == "safe"
    )


def pop_pending(pending: MutableMapping[int, T], board_seq: int) -> T | None:
    """Atomic pop-wins helper.

    Callers must not insert any await between selecting a board_seq and this
    call. Unknown, late, and duplicate board_seq values return None and are
    expected to be dropped/logged by the caller.
    """

    return pending.pop(board_seq, None)


def _validate_command(message: dict[str, Any]) -> None:
    _required_uint64(message, "seq")
    _required_str(message, "source")
    _required_str(message, "target")
    _required_str(message, "command")
    _required_object(message, "args")
    if "controller_ts" in message and not isinstance(message["controller_ts"], int | float):
        raise ProtocolValidationError(ErrorCode.INVALID_TYPE, "controller_ts must be numeric")


def _validate_response(message: dict[str, Any]) -> None:
    _required_uint64(message, "seq")
    _required_str(message, "source")
    _required_str(message, "target")
    status = require_terminal_status(_required_str(message, "status"))
    if status is TerminalStatus.OK and message.get("error") is not None:
        raise ProtocolValidationError(ErrorCode.INVALID_TYPE, "ok response error must be null")
    if status is not TerminalStatus.OK:
        error = _required_object(message, "error")
        code = require_error_code(_required_str(error, "code"))
        _required_str(error, "message")
        if status is TerminalStatus.TIMEOUT and code is not ErrorCode.COMMAND_TIMEOUT:
            raise ProtocolValidationError(ErrorCode.INVALID_TYPE, "timeout status must carry COMMAND_TIMEOUT")
        if status is TerminalStatus.ERROR and code is ErrorCode.COMMAND_TIMEOUT:
            raise ProtocolValidationError(ErrorCode.INVALID_TYPE, "COMMAND_TIMEOUT must use timeout status")
    if "result" in message and message["result"] is not None and not isinstance(message["result"], dict):
        raise ProtocolValidationError(ErrorCode.INVALID_TYPE, "result must be an object or null")
    if "controller_ts" in message and message["controller_ts"] is not None and not isinstance(message["controller_ts"], int | float):
        raise ProtocolValidationError(ErrorCode.INVALID_TYPE, "controller_ts must be numeric")


def _validate_telemetry(message: dict[str, Any]) -> None:
    _required_uint64(message, "seq")
    _required_str(message, "source")
    _required_str(message, "target")
    _required_object(message, "telemetry")


def _validate_schema(message: dict[str, Any]) -> None:
    _required_uint64(message, "seq")
    _required_str(message, "source")
    _required_str(message, "target")
    _required_str(message, "protocol_version")
    schema = _required_object(message, "schema")
    commands = schema.get("commands", {})
    if not isinstance(commands, dict):
        raise ProtocolValidationError(ErrorCode.INVALID_TYPE, "schema.commands must be an object")
    for name, meta in commands.items():
        if not isinstance(name, str):
            raise ProtocolValidationError(ErrorCode.INVALID_TYPE, "schema command names must be strings")
        if not isinstance(meta, dict):
            raise ProtocolValidationError(ErrorCode.INVALID_TYPE, "schema command entries must be objects")
        args = meta.get("args", {})
        if not isinstance(args, dict):
            raise ProtocolValidationError(ErrorCode.INVALID_TYPE, "schema command args must be an object")
        command_blocked_by_estop(meta)


def _validate_event(message: dict[str, Any]) -> None:
    _required_str(message, "source")
    _required_str(message, "event")
    if "target" in message and not isinstance(message["target"], str):
        raise ProtocolValidationError(ErrorCode.INVALID_TYPE, "target must be a string")
    if "event_id" in message:
        _validate_uint64(message["event_id"], "event_id")
    if "details" in message and not isinstance(message["details"], dict):
        raise ProtocolValidationError(ErrorCode.INVALID_TYPE, "details must be an object")
    if message["event"] == "estop_ack":
        details = _required_object(message, "details")
        if details.get("state") != "safe":
            raise ProtocolValidationError(
                ErrorCode.INVALID_TYPE,
                "estop_ack details.state must be 'safe'",
            )


def _validate_estop(message: dict[str, Any]) -> None:
    _required_str(message, "source")
    _required_str(message, "target")


def _validate_estop_reset(message: dict[str, Any]) -> None:
    _required_uint64(message, "seq")
    _required_str(message, "source")
    _required_str(message, "target")


def _validate_heartbeat(message: dict[str, Any]) -> None:
    if "seq" in message:
        _validate_uint64(message["seq"], "seq")
    _required_str(message, "source")
    _required_str(message, "target")


def _required_object(message: dict[str, Any], field: str) -> dict[str, Any]:
    if field not in message:
        raise ProtocolValidationError(ErrorCode.MISSING_FIELD, f"missing required field {field}")
    value = message[field]
    if not isinstance(value, dict):
        raise ProtocolValidationError(ErrorCode.INVALID_TYPE, f"{field} must be an object")
    return value


def _required_str(message: dict[str, Any], field: str) -> str:
    if field not in message:
        raise ProtocolValidationError(ErrorCode.MISSING_FIELD, f"missing required field {field}")
    value = message[field]
    if not isinstance(value, str):
        raise ProtocolValidationError(ErrorCode.INVALID_TYPE, f"{field} must be a string")
    return value


def _required_uint64(message: dict[str, Any], field: str) -> int:
    if field not in message:
        raise ProtocolValidationError(ErrorCode.MISSING_FIELD, f"missing required field {field}")
    value = message[field]
    _validate_uint64(value, field)
    return value


def _validate_uint64(value: Any, field: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0 or value > 2**64 - 1:
        raise ProtocolValidationError(ErrorCode.INVALID_TYPE, f"{field} must be a uint64 integer")
