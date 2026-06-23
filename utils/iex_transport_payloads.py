from __future__ import annotations

import gzip
import logging
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

PCAPNG_MAGIC = b"\x0a\x0d\x0d\x0a"
PCAP_MAGIC_ENDIAN = {
    b"\xd4\xc3\xb2\xa1": "<",
    b"\xa1\xb2\xc3\xd4": ">",
    b"\x4d\x3c\xb2\xa1": "<",
    b"\xa1\xb2\x3c\x4d": ">",
}
PCAPNG_BYTE_ORDER_MAGIC = {
    b"\x4d\x3c\x2b\x1a": "<",
    b"\x1a\x2b\x3c\x4d": ">",
}
PCAPNG_SECTION_HEADER = 0x0A0D0D0A
PCAPNG_INTERFACE_DESCRIPTION = 0x00000001
PCAPNG_SIMPLE_PACKET = 0x00000003
PCAPNG_ENHANCED_PACKET = 0x00000006
LINKTYPE_ETHERNET = 1
LINKTYPE_RAW = 101
ETHERTYPE_IPV4 = 0x0800
ETHERTYPE_IPV6 = 0x86DD
ETHERTYPE_VLAN_TAGS = {0x8100, 0x88A8, 0x9100}
IPPROTO_UDP = 17
IEX_TOPS_PROTOCOL_PREFIXES = {
    "1.5": b"\x01\x00\x02\x80\x01\x00\x00\x00",
    "1.6": b"\x01\x00\x03\x80\x01\x00\x00\x00",
}
IEX_TP_MESSAGE_COUNT_OFFSET = 14
IEX_TP_MIN_HEADER_SIZE = 40


@dataclass(frozen=True)
class PayloadExtractionResult:
    input_path: Path
    output_path: Path | None
    capture_format: str
    packets_seen: int
    udp_payloads_written: int
    bytes_written: int


def prepare_iextools_input(
    input_path: Path, output_path: Path, tops_version: str | None = None
) -> PayloadExtractionResult:
    capture_format = detect_capture_format(input_path)
    if capture_format in {"pcap", "pcapng"}:
        return extract_udp_payloads(input_path, output_path, capture_format, tops_version)
    return PayloadExtractionResult(input_path, None, capture_format, 0, 0, 0)


def prepare_parser_input_for_repo(
    repo: str,
    input_path: Path,
    result_path: str,
    logger: logging.Logger,
    tops_version: str | None = None,
) -> tuple[Path, dict[str, object]]:
    if repo != "hq-4":
        return input_path, {
            "input_mode": "pcap.gz_direct",
            "tops_version": tops_version,
            "steps": [],
        }
    payload_path = Path(result_path).with_name(f"{Path(result_path).stem}_udp_payloads.bin")
    logger.info(
        "transport preprocessing start",
        extra={
            "event": "iex_benchmark_transport_preprocess_start",
            "detail": {"input_path": str(input_path), "payload_path": str(payload_path)},
        },
    )
    extraction = prepare_iextools_input(input_path, payload_path, tops_version)
    detail: dict[str, object] = {
        "input_mode": "udp_payload_stream" if extraction.output_path else "pcap.gz_direct",
        "capture_format": extraction.capture_format,
        "tops_version": tops_version,
        "steps": [],
    }
    if extraction.output_path:
        detail["steps"] = [
            {
                "action": "extract_udp_payloads",
                "input_path": str(extraction.input_path),
                "output_path": str(extraction.output_path),
                "packets_seen": extraction.packets_seen,
                "udp_payloads_written": extraction.udp_payloads_written,
                "bytes_written": extraction.bytes_written,
            }
        ]
        logger.info(
            "transport preprocessing complete",
            extra={"event": "iex_benchmark_transport_preprocessed", "detail": detail},
        )
        return extraction.output_path, detail
    logger.info(
        "transport preprocessing skipped",
        extra={"event": "iex_benchmark_transport_preprocess_skipped", "detail": detail},
    )
    return input_path, detail


def detect_capture_format(path: Path) -> str:
    with _open_capture(path) as handle:
        prefix = handle.read(4)
    if prefix == PCAPNG_MAGIC:
        return "pcapng"
    if prefix in PCAP_MAGIC_ENDIAN:
        return "pcap"
    return "unknown"


def extract_udp_payloads(
    input_path: Path,
    output_path: Path,
    capture_format: str | None = None,
    tops_version: str | None = None,
) -> PayloadExtractionResult:
    detected = capture_format or detect_capture_format(input_path)
    protocol_prefixes = _protocol_prefixes(tops_version)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with _open_capture(input_path) as source, output_path.open("wb") as target:
        if detected == "pcapng":
            packets, payloads, written = _extract_pcapng(source, target, protocol_prefixes)
        elif detected == "pcap":
            packets, payloads, written = _extract_pcap(source, target, protocol_prefixes)
        else:
            raise ValueError(f"unsupported capture format: {detected}")
    return PayloadExtractionResult(input_path, output_path, detected, packets, payloads, written)


def _extract_pcapng(
    source: BinaryIO, target: BinaryIO, protocol_prefixes: tuple[bytes, ...]
) -> tuple[int, int, int]:
    endian = "<"
    linktypes: dict[int, int] = {}
    packets = payloads = written = 0
    while header := source.read(8):
        if len(header) != 8:
            raise ValueError("truncated pcapng block header")
        block_type = struct.unpack(endian + "I", header[:4])[0]
        block_len = struct.unpack(endian + "I", header[4:8])[0]
        if block_type == PCAPNG_SECTION_HEADER:
            body, endian = _read_section_body(source, header)
            continue
        if block_len < 12:
            raise ValueError(f"invalid pcapng block length: {block_len}")
        body = source.read(block_len - 12)
        trailer = source.read(4)
        if len(body) != block_len - 12 or len(trailer) != 4:
            raise ValueError("truncated pcapng block")
        if struct.unpack(endian + "I", trailer)[0] != block_len:
            raise ValueError("pcapng block length trailer mismatch")
        if block_type == PCAPNG_INTERFACE_DESCRIPTION:
            if len(body) < 2:
                raise ValueError("truncated pcapng interface description")
            iface_id = len(linktypes)
            linktypes[iface_id] = struct.unpack(endian + "H", body[:2])[0]
        elif block_type == PCAPNG_ENHANCED_PACKET:
            packet, iface_id = _pcapng_enhanced_packet(body, endian)
            packets += 1
            count, size = _write_payload(
                target, packet, linktypes.get(iface_id, LINKTYPE_ETHERNET), protocol_prefixes
            )
            payloads += count
            written += size
        elif block_type == PCAPNG_SIMPLE_PACKET:
            packet = _pcapng_simple_packet(body, endian)
            packets += 1
            count, size = _write_payload(
                target, packet, linktypes.get(0, LINKTYPE_ETHERNET), protocol_prefixes
            )
            payloads += count
            written += size
    return packets, payloads, written


def _read_section_body(source: BinaryIO, header: bytes) -> tuple[bytes, str]:
    block_len = int.from_bytes(header[4:8], "little")
    body = source.read(block_len - 12)
    trailer = source.read(4)
    if len(body) != block_len - 12 or len(trailer) != 4:
        raise ValueError("truncated pcapng section header")
    endian = PCAPNG_BYTE_ORDER_MAGIC.get(body[:4])
    if endian is None:
        raise ValueError("unsupported pcapng byte order")
    if struct.unpack(endian + "I", trailer)[0] != block_len:
        raise ValueError("pcapng section length trailer mismatch")
    return body, endian


def _pcapng_enhanced_packet(body: bytes, endian: str) -> tuple[bytes, int]:
    if len(body) < 20:
        raise ValueError("truncated pcapng enhanced packet")
    iface_id, _ts_high, _ts_low, cap_len, _orig_len = struct.unpack(endian + "IIIII", body[:20])
    return body[20 : 20 + cap_len], iface_id


def _pcapng_simple_packet(body: bytes, endian: str) -> bytes:
    if len(body) < 4:
        raise ValueError("truncated pcapng simple packet")
    packet_len = struct.unpack(endian + "I", body[:4])[0]
    return body[4 : 4 + packet_len]


def _extract_pcap(
    source: BinaryIO, target: BinaryIO, protocol_prefixes: tuple[bytes, ...]
) -> tuple[int, int, int]:
    header = source.read(24)
    if len(header) != 24:
        raise ValueError("truncated pcap global header")
    endian = PCAP_MAGIC_ENDIAN.get(header[:4])
    if endian is None:
        raise ValueError("unsupported pcap magic")
    linktype = struct.unpack(endian + "I", header[20:24])[0]
    packets = payloads = written = 0
    while record_header := source.read(16):
        if len(record_header) != 16:
            raise ValueError("truncated pcap packet header")
        _ts_sec, _ts_usec, incl_len, _orig_len = struct.unpack(endian + "IIII", record_header)
        packet = source.read(incl_len)
        if len(packet) != incl_len:
            raise ValueError("truncated pcap packet")
        packets += 1
        count, size = _write_payload(target, packet, linktype, protocol_prefixes)
        payloads += count
        written += size
    return packets, payloads, written


def _write_payload(
    target: BinaryIO, packet: bytes, linktype: int, protocol_prefixes: tuple[bytes, ...]
) -> tuple[int, int]:
    payload = _udp_payload(packet, linktype)
    if payload is None:
        return 0, 0
    iex_payload = _iex_payload_with_messages(payload, protocol_prefixes)
    if iex_payload is None:
        return 0, 0
    target.write(iex_payload)
    return 1, len(iex_payload)


def _iex_payload_with_messages(
    payload: bytes, protocol_prefixes: tuple[bytes, ...]
) -> bytes | None:
    offset = _first_protocol_offset(payload, protocol_prefixes)
    if offset is None or len(payload) < offset + IEX_TP_MIN_HEADER_SIZE:
        return None
    message_count_offset = offset + IEX_TP_MESSAGE_COUNT_OFFSET
    message_count = struct.unpack("<h", payload[message_count_offset : message_count_offset + 2])[0]
    if message_count <= 0:
        return None
    return payload[offset:]


def _first_protocol_offset(payload: bytes, protocol_prefixes: tuple[bytes, ...]) -> int | None:
    offsets = [offset for prefix in protocol_prefixes if (offset := payload.find(prefix)) >= 0]
    return min(offsets) if offsets else None


def _protocol_prefixes(tops_version: str | None) -> tuple[bytes, ...]:
    if tops_version is None:
        return tuple(IEX_TOPS_PROTOCOL_PREFIXES.values())
    normalized = str(tops_version)
    try:
        return (IEX_TOPS_PROTOCOL_PREFIXES[normalized],)
    except KeyError as exc:
        supported = ", ".join(sorted(IEX_TOPS_PROTOCOL_PREFIXES))
        raise ValueError(
            f"unsupported TOPS version {tops_version!r}; supported: {supported}"
        ) from exc


def _udp_payload(packet: bytes, linktype: int) -> bytes | None:
    if linktype == LINKTYPE_ETHERNET:
        return _ethernet_udp_payload(packet)
    if linktype == LINKTYPE_RAW:
        return _ip_udp_payload(packet)
    return None


def _ethernet_udp_payload(packet: bytes) -> bytes | None:
    if len(packet) < 14:
        return None
    offset = 12
    ethertype = int.from_bytes(packet[offset : offset + 2], "big")
    offset = 14
    while ethertype in ETHERTYPE_VLAN_TAGS and len(packet) >= offset + 4:
        ethertype = int.from_bytes(packet[offset + 2 : offset + 4], "big")
        offset += 4
    if ethertype == ETHERTYPE_IPV4:
        return _ipv4_udp_payload(packet[offset:])
    if ethertype == ETHERTYPE_IPV6:
        return _ipv6_udp_payload(packet[offset:])
    return None


def _ip_udp_payload(packet: bytes) -> bytes | None:
    if not packet:
        return None
    version = packet[0] >> 4
    if version == 4:
        return _ipv4_udp_payload(packet)
    if version == 6:
        return _ipv6_udp_payload(packet)
    return None


def _ipv4_udp_payload(packet: bytes) -> bytes | None:
    if len(packet) < 28 or packet[0] >> 4 != 4:
        return None
    ihl = (packet[0] & 0x0F) * 4
    if ihl < 20 or len(packet) < ihl + 8 or packet[9] != IPPROTO_UDP:
        return None
    total_len = int.from_bytes(packet[2:4], "big")
    udp_start = ihl
    udp_len = int.from_bytes(packet[udp_start + 4 : udp_start + 6], "big")
    packet_end = min(len(packet), total_len) if total_len else len(packet)
    payload_start = udp_start + 8
    payload_end = min(packet_end, udp_start + udp_len)
    if payload_end < payload_start:
        return None
    return packet[payload_start:payload_end]


def _ipv6_udp_payload(packet: bytes) -> bytes | None:
    if len(packet) < 48 or packet[0] >> 4 != 6 or packet[6] != IPPROTO_UDP:
        return None
    payload_len = int.from_bytes(packet[4:6], "big")
    udp_start = 40
    udp_len = int.from_bytes(packet[udp_start + 4 : udp_start + 6], "big")
    packet_end = min(len(packet), 40 + payload_len)
    payload_start = udp_start + 8
    payload_end = min(packet_end, udp_start + udp_len)
    if payload_end < payload_start:
        return None
    return packet[payload_start:payload_end]


def _open_capture(path: Path) -> BinaryIO:
    if path.suffix == ".gz":
        return gzip.open(path, "rb")
    return path.open("rb")
