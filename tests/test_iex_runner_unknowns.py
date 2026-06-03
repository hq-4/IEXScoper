from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import utils.iex_parser_repo_runner as runner
from utils.iex_runner_unknowns import detect_unknown_message_type, unknown_message_detail


class UnknownMessageError(Exception):
    pass


class FakeIterator:
    def __init__(self, steps: list[object]) -> None:
        self.steps = steps
        self.index = 0
        self.message_type = 0
        self.message_binary = b"\x10\x20\x30"
        self.messages_left = 7
        self.bytes_read = 321
        self.cur_stream_offset = 654
        self.first_sequence_number = 987
        self.cur_send_time = None

    def __iter__(self) -> FakeIterator:
        return self

    def __next__(self):
        if self.index >= len(self.steps):
            raise StopIteration
        step = self.steps[self.index]
        self.index += 1
        if isinstance(step, Exception):
            raise step
        return step


class FakeStreamWriters:
    def __init__(self, *_args) -> None:
        self.main_rows: list[dict[str, object]] = []
        self.quote_rows: list[dict[str, object]] = []
        self.main_count = 0
        self.quote_count = 0
        self.write_seconds = 0.0

    def add(self, target: str, row: dict[str, object]) -> None:
        if target == "quote":
            self.quote_rows.append(row)
            self.quote_count += 1
        else:
            self.main_rows.append(row)
            self.main_count += 1

    def close(self) -> None:
        return


def test_detect_unknown_message_type() -> None:
    assert detect_unknown_message_type(UnknownMessageError("Unknown message type: (244,)")) == 244
    assert detect_unknown_message_type(RuntimeError("other failure")) is None


def test_unknown_message_detail_extracts_iterator_context() -> None:
    detail = unknown_message_detail(
        FakeIterator([]),
        unknown_type=45,
        processed_messages=12,
        parse_seconds=1.234567,
    )
    assert detail["message_type_hex"] == "0x2d"
    assert detail["message_body_prefix_hex"] == "102030"
    assert detail["current_stream_offset"] == 654


def test_runner_unknown_defaults_allow_forward_compatible_bursts() -> None:
    assert runner.DEFAULT_UNKNOWN_MESSAGE_THRESHOLD_COUNT == 10_000
    assert runner.DEFAULT_UNKNOWN_MESSAGE_THRESHOLD_CONSECUTIVE == 10_000


def test_runner_quarantines_single_unknown_and_succeeds(
    monkeypatch, tmp_path: Path, caplog
) -> None:
    result_path = tmp_path / "result.json"
    monkeypatch.setattr(runner, "setup_logging", lambda _path: None)
    monkeypatch.setattr(runner, "get_logger", lambda _name: logging.getLogger("runner-test"))
    monkeypatch.setattr(runner, "StreamWriters", FakeStreamWriters)
    monkeypatch.setattr(
        runner,
        "_open_iterator",
        lambda _repo, _input_path, _stack: FakeIterator(
            ["first", UnknownMessageError("Unknown message type: (0,)"), "second"]
        ),
    )
    monkeypatch.setattr(runner, "_normalize", lambda _repo, message: ("main", {"type": message}))
    monkeypatch.setattr(runner, "artifact", lambda path: {"path": str(path), "exists": False})
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "iex_parser_repo_runner.py",
            "--repo",
            "hq-4",
            "--repo-path",
            str(tmp_path),
            "--input-path",
            "input.pcap.gz",
            "--main-output",
            str(tmp_path / "main.parquet"),
            "--quote-output",
            str(tmp_path / "quote.parquet"),
            "--compression",
            "snappy",
            "--result-path",
            str(result_path),
            "--log-jsonl",
            str(tmp_path / "runner.jsonl"),
            "--unknown-message-threshold-count",
            "5",
            "--unknown-message-threshold-consecutive",
            "2",
        ],
    )

    with caplog.at_level(logging.WARNING):
        assert runner.main() == 0

    result = json.loads(result_path.read_text(encoding="utf-8"))
    quarantine_path = Path(result["unknown_message_quarantine_path"])
    quarantine_rows = [json.loads(line) for line in quarantine_path.read_text().splitlines()]
    assert result["status"] == "succeeded"
    assert result["processed_messages"] == 2
    assert result["unknown_message_count"] == 1
    assert result["unknown_message_types"] == {"0": 1}
    assert quarantine_rows[0]["message_type"] == 0
    assert "unknown message quarantined" in caplog.text


def test_runner_fails_when_unknown_threshold_exceeded(monkeypatch, tmp_path: Path) -> None:
    result_path = tmp_path / "result.json"
    monkeypatch.setattr(runner, "setup_logging", lambda _path: None)
    monkeypatch.setattr(runner, "get_logger", lambda _name: logging.getLogger("runner-test"))
    monkeypatch.setattr(runner, "StreamWriters", FakeStreamWriters)
    monkeypatch.setattr(
        runner,
        "_open_iterator",
        lambda _repo, _input_path, _stack: FakeIterator(
            [
                UnknownMessageError("Unknown message type: (0,)"),
                UnknownMessageError("Unknown message type: (45,)"),
            ]
        ),
    )
    monkeypatch.setattr(runner, "_normalize", lambda _repo, message: ("main", {"type": message}))
    monkeypatch.setattr(runner, "artifact", lambda path: {"path": str(path), "exists": False})
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "iex_parser_repo_runner.py",
            "--repo",
            "hq-4",
            "--repo-path",
            str(tmp_path),
            "--input-path",
            "input.pcap.gz",
            "--main-output",
            str(tmp_path / "main.parquet"),
            "--quote-output",
            str(tmp_path / "quote.parquet"),
            "--compression",
            "snappy",
            "--result-path",
            str(result_path),
            "--log-jsonl",
            str(tmp_path / "runner.jsonl"),
            "--unknown-message-threshold-count",
            "1",
            "--unknown-message-threshold-consecutive",
            "5",
        ],
    )

    assert runner.main() == 1
    result = json.loads(result_path.read_text(encoding="utf-8"))
    assert result["status"] == "failed"
    assert "threshold exceeded" in result["error"]
    assert result["unknown_message_count"] == 2
    assert result["unknown_message_types"] == {"0": 1, "45": 1}
