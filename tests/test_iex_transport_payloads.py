from __future__ import annotations

import gzip
import logging
import struct
from pathlib import Path

import utils.iex_transport_payloads as transport
from utils.iex_transport_payloads import (
    PayloadExtractionResult,
    detect_capture_format,
    extract_udp_payloads,
    prepare_parser_input_for_repo,
    prepare_iextools_input,
)


def test_extract_udp_payloads_from_pcapng_gzip(tmp_path: Path) -> None:
    source = tmp_path / "sample.pcap.gz"
    payload = _iex_payload(message_count=1)
    source.write_bytes(gzip.compress(_pcapng_bytes(payload)))

    assert detect_capture_format(source) == "pcapng"

    result = prepare_iextools_input(source, tmp_path / "payloads.bin")

    assert result.capture_format == "pcapng"
    assert result.udp_payloads_written == 1
    assert result.output_path is not None
    assert result.output_path.read_bytes() == payload


def test_extract_udp_payloads_from_classic_pcap(tmp_path: Path) -> None:
    source = tmp_path / "sample.pcap"
    payload = _iex_payload(message_count=1)
    source.write_bytes(_pcap_bytes(payload))

    result = extract_udp_payloads(source, tmp_path / "payloads.bin")

    assert result.capture_format == "pcap"
    assert result.udp_payloads_written == 1
    assert (tmp_path / "payloads.bin").read_bytes() == payload


def test_prepare_iextools_input_extracts_classic_pcap_gzip(tmp_path: Path) -> None:
    source = tmp_path / "sample.pcap.gz"
    payload = _iex_payload(message_count=1)
    source.write_bytes(gzip.compress(_pcap_bytes(payload)))

    result = prepare_iextools_input(source, tmp_path / "payloads.bin", tops_version="1.6")

    assert result.capture_format == "pcap"
    assert result.udp_payloads_written == 1
    assert result.output_path is not None
    assert result.output_path.read_bytes() == payload


def test_extract_udp_payloads_from_tops_1_5_pcap(tmp_path: Path) -> None:
    source = tmp_path / "sample.pcap"
    payload = _iex_payload(message_count=1, tops_version="1.5")
    source.write_bytes(_pcap_bytes(payload))

    result = extract_udp_payloads(source, tmp_path / "payloads.bin", tops_version="1.5")

    assert result.capture_format == "pcap"
    assert result.udp_payloads_written == 1
    assert (tmp_path / "payloads.bin").read_bytes() == payload


def test_extract_udp_payloads_skips_zero_message_iex_segments(tmp_path: Path) -> None:
    source = tmp_path / "sample.pcap"
    source.write_bytes(_pcap_bytes(_iex_payload(message_count=0)))

    result = extract_udp_payloads(source, tmp_path / "payloads.bin")

    assert result.udp_payloads_written == 0
    assert (tmp_path / "payloads.bin").read_bytes() == b""


def test_prepare_iextools_input_leaves_unknown_files_direct(tmp_path: Path) -> None:
    source = tmp_path / "raw.bin"
    source.write_bytes(b"not a capture")

    result = prepare_iextools_input(source, tmp_path / "payloads.bin")

    assert result.capture_format == "unknown"
    assert result.output_path is None
    assert not (tmp_path / "payloads.bin").exists()


def test_prepare_parser_input_extracts_pcapng_for_hq4(monkeypatch, tmp_path: Path) -> None:
    input_path = tmp_path / "input.pcap.gz"
    output_path = tmp_path / "result_udp_payloads.bin"

    def fake_prepare(
        _input: Path, _output: Path, _tops_version: str | None = None
    ) -> PayloadExtractionResult:
        return PayloadExtractionResult(
            input_path=input_path,
            output_path=output_path,
            capture_format="pcapng",
            packets_seen=2,
            udp_payloads_written=2,
            bytes_written=10,
        )

    monkeypatch.setattr(transport, "prepare_iextools_input", fake_prepare)

    effective, detail = prepare_parser_input_for_repo(
        "hq-4", input_path, str(tmp_path / "result.json"), logging.getLogger("transport-test")
    )

    assert effective == output_path
    assert detail["input_mode"] == "udp_payload_stream"
    assert detail["capture_format"] == "pcapng"
    assert detail["tops_version"] is None
    assert detail["steps"][0]["action"] == "extract_udp_payloads"


def _pcapng_bytes(payload: bytes) -> bytes:
    blocks = [_section_header_block(), _interface_description_block()]
    packet = _ethernet_ipv4_udp_packet(payload)
    body = struct.pack("<IIIII", 0, 0, 0, len(packet), len(packet)) + _pad4(packet)
    blocks.append(_block(6, body))
    return b"".join(blocks)


def _pcap_bytes(payload: bytes) -> bytes:
    packet = _ethernet_ipv4_udp_packet(payload)
    global_header = struct.pack("<IHHIIII", 0xA1B2C3D4, 2, 4, 0, 0, 65535, 1)
    packet_header = struct.pack("<IIII", 0, 0, len(packet), len(packet))
    return global_header + packet_header + packet


def _section_header_block() -> bytes:
    body = b"\x4d\x3c\x2b\x1a" + struct.pack("<HHq", 1, 0, -1)
    return _block(0x0A0D0D0A, body)


def _interface_description_block() -> bytes:
    return _block(1, struct.pack("<HHI", 1, 0, 65535))


def _block(block_type: int, body: bytes) -> bytes:
    padded = _pad4(body)
    total_len = 12 + len(padded)
    return struct.pack("<II", block_type, total_len) + padded + struct.pack("<I", total_len)


def _pad4(data: bytes) -> bytes:
    return data + (b"\x00" * ((4 - len(data) % 4) % 4))


def _ethernet_ipv4_udp_packet(payload: bytes) -> bytes:
    ethernet = b"\x00" * 12 + b"\x08\x00"
    udp_len = 8 + len(payload)
    total_len = 20 + udp_len
    ipv4 = bytearray(20)
    ipv4[0] = 0x45
    ipv4[2:4] = total_len.to_bytes(2, "big")
    ipv4[8] = 64
    ipv4[9] = 17
    ipv4[12:16] = b"\x0a\x00\x00\x01"
    ipv4[16:20] = b"\x0a\x00\x00\x02"
    udp = b"\x1f\x90\x1f\x91" + udp_len.to_bytes(2, "big") + b"\x00\x00"
    return ethernet + bytes(ipv4) + udp + payload


def _iex_payload(message_count: int, tops_version: str = "1.6") -> bytes:
    protocol_ids = {"1.5": b"\x02\x80", "1.6": b"\x03\x80"}
    header_prefix = b"\x01\x00" + protocol_ids[tops_version] + b"\x01\x00\x00\x00"
    session_id = b"\x00\x00\x00\x00"
    segment = struct.pack("<hhqqq", 4, message_count, 0, 1, 0)
    return header_prefix + session_id + segment + (b"\x04\x00Tabc" if message_count else b"")
