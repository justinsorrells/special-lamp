import asyncio
import unittest

from interfaces import SerializedBoardWriter, send_estop
from protocol import (
    BOARD_MAX_LINE_BYTES,
    CONTROLLER_MAX_LINE_BYTES,
    ERROR_CODES,
    TERMINAL_STATUSES,
    ErrorCode,
    MessageType,
    ProtocolValidationError,
    TerminalStatus,
    build_board_command,
    build_client_response,
    build_error_response,
    check_protocol_version,
    extract_blocked_by_estop,
    is_estop_ack_event,
    parse_message,
    pop_pending,
    serialize_message,
)
from state import BoardConnState, BoardSeqCounter, BoardState, BoardStateRecord, SystemState


class ProtocolContractTests(unittest.TestCase):
    def test_terminal_statuses_are_frozen(self):
        self.assertEqual(TERMINAL_STATUSES, {"ok", "error", "timeout"})
        with self.assertRaises(ProtocolValidationError):
            build_error_response(
                seq=1,
                target="gui",
                code=ErrorCode.INTERNAL_ERROR,
                message="bad",
                status=TerminalStatus.OK,
            )

    def test_error_codes_are_frozen(self):
        self.assertEqual(
            ERROR_CODES,
            {
                "INVALID_JSON",
                "MISSING_FIELD",
                "INVALID_TYPE",
                "UNKNOWN_TARGET",
                "UNKNOWN_COMMAND",
                "INVALID_ARGUMENT",
                "BOARD_UNAVAILABLE",
                "BOARD_BUSY",
                "COMMAND_TIMEOUT",
                "ESTOP_ACTIVE",
                "PROTOCOL_VERSION_MISMATCH",
                "CONTROLLER_SHUTDOWN",
                "INTERNAL_ERROR",
            },
        )

    def test_command_timeout_uses_timeout_status_and_error_code(self):
        response = build_error_response(
            seq=7,
            target="gui",
            code=ErrorCode.COMMAND_TIMEOUT,
            message="command timed out",
        )
        self.assertEqual(response["status"], "timeout")
        self.assertEqual(response["error"]["code"], "COMMAND_TIMEOUT")

    def test_seq_and_board_seq_are_not_conflated_across_round_trip(self):
        client_command = {
            "type": "command",
            "seq": 12,
            "source": "gui",
            "target": "motor_controller",
            "command": "set_speed",
            "args": {"rpm": 1200},
        }
        board_command = build_board_command(
            client_command,
            board_seq=1042,
            board_id="motor_controller",
            controller_ts=123.5,
        )
        self.assertEqual(board_command["seq"], 1042)
        self.assertEqual(board_command["source"], "controller")
        self.assertEqual(board_command["target"], "motor_controller")

        board_response = {
            "type": "response",
            "seq": 1042,
            "controller_ts": 123.5,
            "source": "motor_controller",
            "target": "controller",
            "status": "ok",
            "result": {"accepted": True},
            "error": None,
        }
        client_response = build_client_response(
            board_response,
            client_seq=12,
            client_target="gui",
            board_seq=1042,
            latency_ms=20.4,
        )
        self.assertEqual(client_response["seq"], 12)
        self.assertEqual(client_response["result"]["board_seq"], 1042)
        self.assertNotEqual(client_response["seq"], client_response["result"]["board_seq"])

    def test_relayed_error_response_keeps_result_null(self):
        board_response = {
            "type": "response",
            "seq": 1042,
            "source": "motor_controller",
            "target": "controller",
            "status": "error",
            "result": None,
            "error": {"code": "UNKNOWN_COMMAND", "message": "unknown command"},
        }
        client_response = build_client_response(
            board_response,
            client_seq=12,
            client_target="gui",
            board_seq=1042,
        )
        self.assertIsNone(client_response["result"])
        self.assertEqual(client_response["error"]["code"], "UNKNOWN_COMMAND")

    def test_two_clients_may_reuse_same_client_seq_with_distinct_board_seqs(self):
        counter = BoardSeqCounter()
        self.assertEqual(counter.next(), 1)
        self.assertEqual(counter.next(), 2)
        self.assertNotEqual(1, 2)

    def test_controller_line_limit_accepts_large_schema_that_board_limit_rejects(self):
        schema = {
            "type": "schema",
            "seq": 1,
            "source": "motor_controller",
            "target": "controller",
            "protocol_version": "1",
            "schema": {
                "commands": {
                    f"cmd_{idx}": {
                        "args": {"value": "int"},
                        "blocked_by_estop": idx % 2 == 0,
                    }
                    for idx in range(45)
                },
                "telemetry": {"payload": "string"},
                "state": {"mode": "string"},
            },
        }
        line = serialize_message(schema, max_line_bytes=CONTROLLER_MAX_LINE_BYTES)
        self.assertGreater(len(line), BOARD_MAX_LINE_BYTES)
        self.assertLessEqual(len(line), CONTROLLER_MAX_LINE_BYTES)
        self.assertTrue(parse_message(line, max_line_bytes=CONTROLLER_MAX_LINE_BYTES).ok)
        board_result = parse_message(line, max_line_bytes=BOARD_MAX_LINE_BYTES)
        self.assertFalse(board_result.ok)
        self.assertEqual(board_result.error.code, ErrorCode.INVALID_JSON)

    def test_parse_message_returns_structured_errors(self):
        result = parse_message(b'{"type":"command"}\n')
        self.assertFalse(result.ok)
        self.assertEqual(result.error.code, ErrorCode.MISSING_FIELD)

        bad_json = parse_message(b'{"type":\n')
        self.assertFalse(bad_json.ok)
        self.assertEqual(bad_json.error.code, ErrorCode.INVALID_JSON)

    def test_board_outbound_response_must_fit_controller_limit(self):
        response = {
            "type": "response",
            "seq": 1,
            "source": "motor_controller",
            "target": "controller",
            "status": "ok",
            "result": {"payload": "x" * CONTROLLER_MAX_LINE_BYTES},
            "error": None,
        }
        with self.assertRaises(ProtocolValidationError):
            serialize_message(response, max_line_bytes=CONTROLLER_MAX_LINE_BYTES)

    def test_blocked_by_estop_defaults_to_true(self):
        schema = {
            "type": "schema",
            "seq": 1,
            "source": "motor_controller",
            "target": "controller",
            "protocol_version": "1",
            "schema": {
                "commands": {
                    "set_speed": {"args": {"rpm": "int"}},
                    "get_status": {"args": {}, "blocked_by_estop": False},
                }
            },
        }
        gates = extract_blocked_by_estop(schema)
        self.assertTrue(gates["set_speed"])
        self.assertFalse(gates["get_status"])

    def test_protocol_version_mismatch_is_explicit_error_code(self):
        schema = {
            "type": "schema",
            "seq": 1,
            "source": "motor_controller",
            "target": "controller",
            "protocol_version": "2",
            "schema": {"commands": {}},
        }
        with self.assertRaises(ProtocolValidationError) as ctx:
            check_protocol_version(schema)
        self.assertEqual(ctx.exception.error.code, ErrorCode.PROTOCOL_VERSION_MISMATCH)

    def test_estop_ack_shape(self):
        message = {
            "type": "event",
            "source": "motor_controller",
            "target": "controller",
            "event": "estop_ack",
            "details": {"state": "safe"},
        }
        self.assertTrue(is_estop_ack_event(message))

    def test_pop_wins_first_consumer_gets_entry_late_duplicate_get_none(self):
        pending = {42: "entry"}
        self.assertEqual(pop_pending(pending, 42), "entry")
        self.assertIsNone(pop_pending(pending, 42))
        self.assertIsNone(pop_pending(pending, 99))


class StateContractTests(unittest.TestCase):
    def test_connection_and_safety_state_are_separate_fields(self):
        board = BoardState(board_id="motor_controller")
        board.conn_state = BoardConnState.REGISTERED
        board.mark_estop_sent()

        record = BoardStateRecord.from_board_state(board).as_hash()
        self.assertEqual(record["conn_state"], "REGISTERED")
        self.assertIn("estop_ack", record)
        self.assertFalse(record["estop_ack"])

        system = SystemState()
        system.latch_estop()
        self.assertTrue(system.estop_active)
        self.assertEqual(board.conn_state, BoardConnState.REGISTERED)


class InterfaceContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_send_estop_uses_serialized_writer_path(self):
        writes = []

        async def write_bytes(data):
            writes.append(data)

        writer = SerializedBoardWriter(
            board_id="motor_controller",
            write_bytes=write_bytes,
            max_line_bytes=BOARD_MAX_LINE_BYTES,
        )
        await send_estop("motor_controller", writer)

        self.assertEqual(len(writes), 1)
        parsed = parse_message(writes[0], max_line_bytes=BOARD_MAX_LINE_BYTES)
        self.assertTrue(parsed.ok)
        self.assertEqual(parsed.message["type"], MessageType.ESTOP.value)
        self.assertEqual(parsed.message["target"], "motor_controller")

    async def test_serialized_writer_does_not_interleave_concurrent_writes(self):
        writes = []

        async def write_bytes(data):
            await asyncio.sleep(0)
            writes.append(data)

        writer = SerializedBoardWriter(
            board_id="motor_controller",
            write_bytes=write_bytes,
            max_line_bytes=BOARD_MAX_LINE_BYTES,
        )
        command = {
            "type": "command",
            "seq": 1,
            "source": "controller",
            "target": "motor_controller",
            "command": "get_status",
            "args": {},
        }
        await asyncio.gather(writer.write_message(command), send_estop("motor_controller", writer))

        self.assertEqual(len(writes), 2)
        for line in writes:
            self.assertTrue(parse_message(line, max_line_bytes=BOARD_MAX_LINE_BYTES).ok)


if __name__ == "__main__":
    unittest.main()
