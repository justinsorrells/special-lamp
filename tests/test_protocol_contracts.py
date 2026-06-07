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
    parse_line,
    parse_message,
    pop_pending,
    serialize_message,
)
from state import (
    DEFAULT_COMMAND_FIFO_DEPTH,
    MAX_COMMAND_TIMEOUT_S,
    BoardConnState,
    BoardSeqCounter,
    BoardState,
    BoardStateRecord,
    PendingCommand,
    SystemState,
    SystemStateRecord,
)


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

    def test_rejected_and_disconnected_are_not_terminal_statuses(self):
        for forbidden in ("rejected", "disconnected", "busy", "unavailable"):
            response = {
                "type": "response",
                "seq": 1,
                "source": "controller",
                "target": "gui",
                "status": forbidden,
                "result": None,
                "error": {"code": "BOARD_UNAVAILABLE", "message": "bad status"},
            }
            result = parse_message(serialize_unvalidated(response))
            self.assertFalse(result.ok)
            self.assertEqual(result.error.code, ErrorCode.INVALID_TYPE)

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

    def test_timeout_status_cannot_carry_non_timeout_error_code(self):
        with self.assertRaises(ProtocolValidationError):
            build_error_response(
                seq=7,
                target="gui",
                code=ErrorCode.BOARD_UNAVAILABLE,
                message="board down",
                status=TerminalStatus.TIMEOUT,
            )

        response = {
            "type": "response",
            "seq": 7,
            "source": "controller",
            "target": "gui",
            "status": "timeout",
            "result": None,
            "error": {"code": "BOARD_UNAVAILABLE", "message": "board down"},
        }
        parsed = parse_message(serialize_unvalidated(response))
        self.assertFalse(parsed.ok)
        self.assertEqual(parsed.error.code, ErrorCode.INVALID_TYPE)

    def test_command_timeout_error_code_cannot_use_error_status(self):
        response = {
            "type": "response",
            "seq": 7,
            "source": "controller",
            "target": "gui",
            "status": "error",
            "result": None,
            "error": {"code": "COMMAND_TIMEOUT", "message": "timed out"},
        }
        parsed = parse_message(serialize_unvalidated(response))
        self.assertFalse(parsed.ok)
        self.assertEqual(parsed.error.code, ErrorCode.INVALID_TYPE)

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

    def test_board_seq_counter_is_per_board_not_client_seq(self):
        board_a = BoardSeqCounter()
        board_b = BoardSeqCounter()
        self.assertEqual(board_a.next(), 1)
        self.assertEqual(board_a.next(), 2)
        self.assertEqual(board_b.next(), 1)

    def test_uint64_sequence_validation_rejects_bool_negative_and_overflow(self):
        for bad_seq in (True, -1, 2**64):
            command = {
                "type": "command",
                "seq": bad_seq,
                "source": "gui",
                "target": "motor_controller",
                "command": "set_speed",
                "args": {"rpm": 1200},
            }
            parsed = parse_message(serialize_unvalidated(command))
            self.assertFalse(parsed.ok)
            self.assertEqual(parsed.error.code, ErrorCode.INVALID_TYPE)

    def test_newline_json_requires_terminating_newline_and_object(self):
        no_newline = parse_message(b'{"type":"estop","source":"controller","target":"board"}')
        self.assertFalse(no_newline.ok)
        self.assertEqual(no_newline.error.code, ErrorCode.INVALID_JSON)

        not_object = parse_message(b'["not","an","object"]\n')
        self.assertFalse(not_object.ok)
        self.assertEqual(not_object.error.code, ErrorCode.INVALID_TYPE)

    def test_invalid_utf8_is_structured_invalid_json(self):
        result = parse_message(b"\xff\n")
        self.assertFalse(result.ok)
        self.assertEqual(result.error.code, ErrorCode.INVALID_JSON)

    def test_receive_line_limit_allows_exact_size_and_rejects_one_byte_over(self):
        prefix = b'{"type":"estop","source":"controller","target":"'
        suffix = b'"}\n'
        fill = b"a" * (BOARD_MAX_LINE_BYTES - len(prefix) - len(suffix))
        exact_line = prefix + fill + suffix
        self.assertEqual(len(exact_line), BOARD_MAX_LINE_BYTES)
        self.assertEqual(parse_line(exact_line, max_line_bytes=BOARD_MAX_LINE_BYTES)["type"], "estop")

        too_large = prefix + fill + b"a" + suffix
        with self.assertRaises(ProtocolValidationError) as ctx:
            parse_line(too_large, max_line_bytes=BOARD_MAX_LINE_BYTES)
        self.assertEqual(ctx.exception.error.code, ErrorCode.INVALID_JSON)

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

    def test_missing_and_invalid_fields_return_contract_error_codes(self):
        cases = [
            ({"seq": 1}, ErrorCode.MISSING_FIELD),
            (
                {"type": "command", "seq": 1, "source": "gui", "target": "board", "command": "x", "args": []},
                ErrorCode.INVALID_TYPE,
            ),
            (
                {"type": "response", "seq": 1, "source": "board", "target": "controller", "status": "error", "result": None, "error": {"code": "NOT_A_CODE", "message": "bad"}},  # noqa: E501
                ErrorCode.INVALID_TYPE,
            ),
            (
                {"type": "schema", "seq": 1, "source": "board", "target": "controller", "protocol_version": "1", "schema": {"commands": {"move": {"args": {}, "blocked_by_estop": "yes"}}}},  # noqa: E501
                ErrorCode.INVALID_TYPE,
            ),
            (
                {"type": "event", "source": "board", "target": "controller", "event": "estop_ack", "details": {"state": "unsafe"}},  # noqa: E501
                ErrorCode.INVALID_TYPE,
            ),
        ]
        for message, expected_code in cases:
            with self.subTest(message=message):
                parsed = parse_message(serialize_unvalidated(message))
                self.assertFalse(parsed.ok)
                self.assertEqual(parsed.error.code, expected_code)

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

    def test_empty_schema_command_metadata_defaults_to_blocked_by_estop(self):
        gates = extract_blocked_by_estop(
            {
                "type": "schema",
                "seq": 1,
                "source": "motor_controller",
                "target": "controller",
                "protocol_version": "1",
                "schema": {"commands": {"legacy_motion": {}}},
            }
        )
        self.assertEqual(gates, {"legacy_motion": True})

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

    def test_late_duplicate_response_has_no_pending_entry_to_resolve(self):
        loop = asyncio.new_event_loop()
        self.addCleanup(loop.close)
        future = loop.create_future()
        pending = {42: future}
        winner = pop_pending(pending, 42)
        winner.set_result("ok")

        duplicate = pop_pending(pending, 42)
        late = pop_pending(pending, 99)

        self.assertEqual(future.result(), "ok")
        self.assertIsNone(duplicate)
        self.assertIsNone(late)
        self.assertEqual(pending, {})


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

    def test_board_connection_states_are_only_connection_axis(self):
        self.assertEqual(
            {state.value for state in BoardConnState},
            {"DISCONNECTED", "CONNECTING", "CONNECTED", "REGISTERED", "FAULTED"},
        )
        self.assertNotIn("ESTOPPED", {state.value for state in BoardConnState})

    def test_estop_latch_does_not_depend_on_per_board_ack(self):
        system = SystemState()
        board = BoardState(board_id="motor_controller", conn_state=BoardConnState.DISCONNECTED)

        system.latch_estop()
        self.assertTrue(system.estop_active)
        self.assertFalse(board.estop_ack)
        self.assertEqual(board.conn_state, BoardConnState.DISCONNECTED)

        board.mark_estop_ack()
        self.assertTrue(board.estop_ack)
        self.assertTrue(system.estop_active)

    def test_one_command_in_flight_state_model_and_timeout_ceiling(self):
        self.assertEqual(DEFAULT_COMMAND_FIFO_DEPTH, 6)
        board = BoardState(board_id="motor_controller")
        board.in_flight_board_seq = 10
        board.queue_depth = DEFAULT_COMMAND_FIFO_DEPTH
        self.assertEqual(board.in_flight_board_seq, 10)
        self.assertEqual(board.queue_depth, 6)

        loop = asyncio.new_event_loop()
        self.addCleanup(loop.close)
        future = loop.create_future()
        pending = PendingCommand(
            board_id="motor_controller",
            board_seq=10,
            client_seq=5,
            client=None,
            future=future,
            command={"command": "set_speed"},
            enqueued_at=100.0,
            execution_timeout_s=MAX_COMMAND_TIMEOUT_S,
        )
        self.assertFalse(pending.queue_residency_expired(109.0))
        self.assertTrue(pending.queue_residency_expired(111.0))

        with self.assertRaises(ValueError):
            PendingCommand(
                board_id="motor_controller",
                board_seq=11,
                client_seq=6,
                client=None,
                future=future,
                command={"command": "set_speed"},
                enqueued_at=100.0,
                execution_timeout_s=MAX_COMMAND_TIMEOUT_S + 0.001,
            )

    def test_redis_state_records_are_read_replica_snapshots(self):
        board = BoardState(
            board_id="motor_controller",
            conn_state=BoardConnState.REGISTERED,
            estop_ack=True,
            last_telemetry={"rpm": 100},
            last_seen=123.0,
            queue_depth=2,
            in_flight_board_seq=9,
        )
        record = BoardStateRecord.from_board_state(board)
        board.conn_state = BoardConnState.FAULTED
        board.estop_ack = False

        self.assertEqual(record.as_hash()["conn_state"], "REGISTERED")
        self.assertTrue(record.as_hash()["estop_ack"])
        self.assertEqual(record.as_hash()["in_flight_board_seq"], 9)

        system = SystemState(estop_active=True, connected_count=1)
        system_record = SystemStateRecord.from_system_state(system)
        system.operator_reset_estop()
        self.assertTrue(system_record.as_hash()["estop_active"])


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


def serialize_unvalidated(message):
    import json

    return (json.dumps(message, separators=(",", ":")) + "\n").encode("utf-8")


if __name__ == "__main__":
    unittest.main()
