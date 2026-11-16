#!/usr/bin/env python3
"""Batch-convert STDF V4 test files into one flat CSV per input file."""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import os
import struct
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple


VERSION = "1.0.0"
STDF_EXTENSIONS = (".stdf.gz", ".std.gz", ".stdf", ".std")

CSV_COLUMNS = [
    "source_file",
    "source_record",
    "source_record_index",
    "stdf_version",
    "cpu_type",
    "lot_id",
    "part_type",
    "node_name",
    "tester_type",
    "job_name",
    "job_rev",
    "sublot_id",
    "oper_name",
    "exec_type",
    "exec_ver",
    "test_code",
    "test_temperature",
    "package_type",
    "family_id",
    "facility_id",
    "process_id",
    "flow_id",
    "setup_id",
    "design_rev",
    "engineering_id",
    "wafer_id",
    "wafer_index",
    "start_time_unix",
    "start_time_utc",
    "head_num",
    "site_num",
    "part_index",
    "part_id",
    "part_result",
    "part_flags",
    "hard_bin",
    "soft_bin",
    "x_coord",
    "y_coord",
    "num_tests",
    "part_test_time_ms",
    "test_num",
    "test_name",
    "test_type",
    "test_result",
    "result",
    "result_raw",
    "result_scale",
    "units",
    "lo_limit",
    "lo_limit_raw",
    "lo_limit_scale",
    "hi_limit",
    "hi_limit_raw",
    "hi_limit_scale",
    "lo_spec",
    "hi_spec",
    "test_flags",
    "param_flags",
    "result_valid",
    "return_state",
    "pin_index",
    "result_index",
    "vector_name",
    "time_set",
    "op_code",
    "program_text",
    "result_text",
    "res_format",
    "lo_limit_format",
    "hi_limit_format",
    "used_defaults",
]

MIR_STRING_FIELDS = [
    "lot_id",
    "part_type",
    "node_name",
    "tester_type",
    "job_name",
    "job_rev",
    "sublot_id",
    "oper_name",
    "exec_type",
    "exec_ver",
    "test_code",
    "test_temperature",
    "user_text",
    "aux_file",
    "package_type",
    "family_id",
    "date_code",
    "facility_id",
    "floor_id",
    "process_id",
    "operation_frequency",
    "spec_name",
    "spec_version",
    "flow_id",
    "setup_id",
    "design_rev",
    "engineering_id",
    "rom_code",
    "serial_number",
    "supervisor_name",
]

PRR_STRING_FIELDS = [
    "part_id",
    "part_text",
    "part_fix",
]

PTR_DEFAULT_FIELDS = [
    "test_name",
    "units",
    "res_format",
    "lo_limit_format",
    "hi_limit_format",
    "res_scal",
    "llm_scal",
    "hlm_scal",
    "lo_limit",
    "hi_limit",
    "lo_spec",
    "hi_spec",
]

FTR_DEFAULT_FIELDS = [
    "test_name",
    "vector_name",
    "time_set",
    "op_code",
    "program_text",
    "result_text",
]

RECORD_NAMES = {
    (0, 10): "FAR",
    (0, 20): "ATR",
    (0, 30): "VUR",
    (1, 10): "MIR",
    (1, 20): "MRR",
    (1, 30): "PCR",
    (1, 40): "HBR",
    (1, 50): "SBR",
    (1, 60): "PMR",
    (1, 62): "PGR",
    (1, 63): "PLR",
    (1, 70): "RDR",
    (1, 80): "SDR",
    (1, 90): "PSR",
    (1, 91): "NMR",
    (1, 92): "CNR",
    (1, 93): "SSR",
    (1, 94): "SCR",
    (2, 10): "WIR",
    (2, 20): "WRR",
    (2, 30): "WCR",
    (5, 10): "PIR",
    (5, 20): "PRR",
    (10, 30): "TSR",
    (15, 10): "PTR",
    (15, 15): "MPR",
    (15, 20): "FTR",
    (15, 30): "STR",
    (20, 10): "BPS",
    (20, 20): "EPS",
    (50, 10): "GDR",
    (50, 30): "DTR",
}


class StdfError(Exception):
    """Base error for malformed or unsupported STDF input."""


class TruncatedRecord(StdfError):
    """The current record ended before a requested field was complete."""


class UnsupportedFloatEncoding(StdfError):
    """The file uses a floating-point representation we cannot safely decode."""


class Reader:
    def __init__(self, data: bytes, endian: str, cpu_type: int) -> None:
        self.data = data
        self.endian = endian
        self.byteorder = "little" if endian == "<" else "big"
        self.cpu_type = cpu_type
        self.pos = 0

    @property
    def remaining(self) -> int:
        return len(self.data) - self.pos

    def _take(self, size: int) -> bytes:
        if size < 0 or self.remaining < size:
            raise TruncatedRecord(
                f"needed {size} bytes at offset {self.pos}, only {self.remaining} remain"
            )
        start = self.pos
        self.pos += size
        return self.data[start : self.pos]

    def u1(self) -> int:
        return self._take(1)[0]

    def u2(self) -> int:
        return int.from_bytes(self._take(2), self.byteorder, signed=False)

    def u4(self) -> int:
        return int.from_bytes(self._take(4), self.byteorder, signed=False)

    def i1(self) -> int:
        return int.from_bytes(self._take(1), self.byteorder, signed=True)

    def i2(self) -> int:
        return int.from_bytes(self._take(2), self.byteorder, signed=True)

    def i4(self) -> int:
        return int.from_bytes(self._take(4), self.byteorder, signed=True)

    def r4(self) -> float:
        if self.cpu_type == 0:
            raise UnsupportedFloatEncoding(
                "CPU_TYPE=0 uses legacy DEC VAX floating-point data; "
                "this converter supports IEEE R*4 files written by CPU_TYPE 1 or 2"
            )
        return struct.unpack(self.endian + "f", self._take(4))[0]

    def c1(self) -> str:
        return self._take(1).decode("latin-1").replace("\x00", "").strip()

    def cn(self) -> str:
        size = self.u1()
        return self._take(size).decode("latin-1").replace("\x00", "").strip()

    def nibbles(self, count: int) -> List[int]:
        packed = self._take((count + 1) // 2)
        values: List[int] = []
        for byte in packed:
            values.append(byte & 0x0F)
            if len(values) < count:
                values.append((byte >> 4) & 0x0F)
        return values[:count]

    def dn_hex(self) -> str:
        bit_count = self.u2()
        return self._take((bit_count + 7) // 8).hex().upper()


@dataclass
class PartContext:
    head_num: Any
    site_num: Any
    rows: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ConversionState:
    input_path: Path
    stdf_version: int = 0
    cpu_type: int = -1
    lot: Dict[str, Any] = field(default_factory=dict)
    wafer: Dict[str, Any] = field(default_factory=dict)
    contexts: Dict[Tuple[Any, Any], PartContext] = field(default_factory=dict)
    test_defaults: Dict[Tuple[str, int], Dict[str, Any]] = field(default_factory=dict)
    record_counts: Dict[str, int] = field(default_factory=dict)
    wafer_counter: int = 0
    part_counter: int = 0
    row_count: int = 0
    warning_count: int = 0
    warnings: List[str] = field(default_factory=list)
    orphan_sites: set = field(default_factory=set)

    def warn(self, message: str) -> None:
        self.warning_count += 1
        if len(self.warnings) < 30:
            self.warnings.append(message)

    def count_record(self, name: str) -> None:
        self.record_counts[name] = self.record_counts.get(name, 0) + 1


def detect_endianness(header: bytes) -> str:
    if len(header) != 4:
        raise StdfError("input is shorter than an STDF record header")
    for endian in ("<", ">"):
        rec_len, rec_typ, rec_sub = struct.unpack(endian + "HBB", header)
        if rec_len == 2 and rec_typ == 0 and rec_sub == 10:
            return endian
    raise StdfError("first record is not a valid STDF FAR record")


def is_gzip_file(path: Path) -> bool:
    if path.name.lower().endswith(".gz"):
        return True
    try:
        with path.open("rb") as stream:
            return stream.read(2) == b"\x1f\x8b"
    except OSError:
        return False


def open_input(path: Path):
    if is_gzip_file(path):
        return gzip.open(path, "rb")
    return path.open("rb")


def read_optional_cn(reader: Reader, warnings: List[str], field_name: str) -> Tuple[bool, Any]:
    if reader.remaining == 0:
        return False, None
    start = reader.pos
    try:
        return True, reader.cn()
    except TruncatedRecord:
        reader.pos = start
        warnings.append(f"optional {field_name} is truncated")
        return False, None


def read_optional(reader: Reader, fn: Callable[[], Any]) -> Tuple[bool, Any]:
    if reader.remaining == 0:
        return False, None
    start = reader.pos
    try:
        return True, fn()
    except TruncatedRecord:
        reader.pos = start
        return False, None


def parse_mir(reader: Reader) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    data["setup_time_unix"] = reader.u4()
    data["start_time_unix"] = reader.u4()
    reader.u1()  # STAT_NUM
    reader.c1()  # MODE_COD
    reader.c1()  # RTST_COD
    reader.c1()  # PROT_COD
    reader.u2()  # BURN_TIM
    reader.c1()  # CMOD_COD

    warnings: List[str] = []
    for field_name in MIR_STRING_FIELDS:
        ok, value = read_optional_cn(reader, warnings, field_name)
        if not ok:
            break
        data[field_name] = value
    if warnings:
        data["_warnings"] = warnings
    return data


def parse_wir(reader: Reader) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    data["head_num"] = reader.u1()
    reader.u1()  # SITE_GRP
    data["start_time_unix"] = reader.u4()
    if reader.remaining:
        data["wafer_id"] = reader.cn()
    return data


def parse_prr(reader: Reader) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    data["head_num"] = reader.u1()
    data["site_num"] = reader.u1()
    data["part_flg"] = reader.u1()
    data["num_tests"] = reader.u2()
    data["hard_bin"] = reader.u2()
    data["soft_bin"] = reader.u2()
    data["x_coord"] = reader.i2()
    data["y_coord"] = reader.i2()
    data["part_test_time_ms"] = reader.u4()

    warnings: List[str] = []
    for field_name in PRR_STRING_FIELDS:
        ok, value = read_optional_cn(reader, warnings, field_name)
        if not ok:
            break
        data[field_name] = value
    if warnings:
        data["_warnings"] = warnings
    return data


def parse_ptr(reader: Reader) -> Dict[str, Any]:
    data: Dict[str, Any] = {
        "test_num": reader.u4(),
        "head_num": reader.u1(),
        "site_num": reader.u1(),
        "test_flg": reader.u1(),
        "parm_flg": reader.u1(),
        "result_raw": reader.r4(),
        "test_name": reader.cn(),
    }

    warnings: List[str] = []
    ok, data["alarm_id"] = read_optional_cn(reader, warnings, "ALARM_ID")
    if not ok:
        data["_warnings"] = warnings
        return data

    fields: List[Tuple[str, Callable[[], Any]]] = [
        ("opt_flag", reader.u1),
        ("res_scal", reader.i1),
        ("llm_scal", reader.i1),
        ("hlm_scal", reader.i1),
        ("lo_limit", reader.r4),
        ("hi_limit", reader.r4),
        ("units", reader.cn),
        ("res_format", reader.cn),
        ("lo_limit_format", reader.cn),
        ("hi_limit_format", reader.cn),
        ("lo_spec", reader.r4),
        ("hi_spec", reader.r4),
    ]
    for field_name, fn in fields:
        ok, value = read_optional(reader, fn)
        if not ok:
            if reader.remaining:
                warnings.append(f"optional {field_name} is truncated")
            break
        data[field_name] = value
    if warnings:
        data["_warnings"] = warnings
    return data


def parse_mpr(reader: Reader) -> Dict[str, Any]:
    data: Dict[str, Any] = {
        "test_num": reader.u4(),
        "head_num": reader.u1(),
        "site_num": reader.u1(),
        "test_flg": reader.u1(),
        "parm_flg": reader.u1(),
    }
    rtn_icnt = reader.u2()
    rslt_cnt = reader.u2()
    data["rtn_icnt"] = rtn_icnt
    data["rslt_cnt"] = rslt_cnt
    data["rtn_stat"] = reader.nibbles(rtn_icnt)
    data["rtn_rslt"] = [reader.r4() for _ in range(rslt_cnt)]
    data["test_name"] = reader.cn()

    warnings: List[str] = []
    ok, data["alarm_id"] = read_optional_cn(reader, warnings, "ALARM_ID")
    if not ok:
        data["_warnings"] = warnings
        return data

    fields: List[Tuple[str, Callable[[], Any]]] = [
        ("opt_flag", reader.u1),
        ("res_scal", reader.i1),
        ("llm_scal", reader.i1),
        ("hlm_scal", reader.i1),
        ("lo_limit", reader.r4),
        ("hi_limit", reader.r4),
        ("start_in", reader.c1),
        ("increment", reader.r4),
    ]
    for field_name, fn in fields:
        ok, value = read_optional(reader, fn)
        if not ok:
            if reader.remaining:
                warnings.append(f"optional {field_name} is truncated")
            data["_warnings"] = warnings
            return data
        data[field_name] = value

    if rtn_icnt and reader.remaining:
        try:
            data["rtn_indx"] = [reader.u2() for _ in range(rtn_icnt)]
        except TruncatedRecord:
            warnings.append("optional RTN_INDX is truncated")
            data["_warnings"] = warnings
            return data

    more_fields: List[Tuple[str, Callable[[], Any]]] = [
        ("units", reader.cn),
        ("units_in", reader.c1),
        ("res_format", reader.cn),
        ("lo_limit_format", reader.cn),
        ("hi_limit_format", reader.cn),
        ("lo_spec", reader.r4),
        ("hi_spec", reader.r4),
    ]
    for field_name, fn in more_fields:
        ok, value = read_optional(reader, fn)
        if not ok:
            if reader.remaining:
                warnings.append(f"optional {field_name} is truncated")
            break
        data[field_name] = value
    if warnings:
        data["_warnings"] = warnings
    return data


def parse_ftr(reader: Reader) -> Dict[str, Any]:
    data: Dict[str, Any] = {
        "test_num": reader.u4(),
        "head_num": reader.u1(),
        "site_num": reader.u1(),
        "test_flg": reader.u1(),
    }

    warnings: List[str] = []
    fields: List[Tuple[str, Callable[[], Any]]] = [
        ("opt_flag", reader.u1),
        ("cycl_cnt", reader.u4),
        ("rel_vadr", reader.u4),
        ("rept_cnt", reader.u4),
        ("num_fail", reader.u4),
        ("xfail_ad", reader.i4),
        ("yfail_ad", reader.i4),
        ("vect_off", reader.i2),
        ("rtn_icnt", reader.u2),
        ("pgm_icnt", reader.u2),
    ]
    for field_name, fn in fields:
        ok, value = read_optional(reader, fn)
        if not ok:
            if reader.remaining:
                warnings.append(f"optional {field_name} is truncated")
            data["_warnings"] = warnings
            return data
        data[field_name] = value

    try:
        data["rtn_indx"] = [reader.u2() for _ in range(int(data["rtn_icnt"]))]
        data["rtn_stat"] = reader.nibbles(int(data["rtn_icnt"]))
        data["pgm_indx"] = [reader.u2() for _ in range(int(data["pgm_icnt"]))]
        data["pgm_stat"] = reader.nibbles(int(data["pgm_icnt"]))
        data["fail_pin"] = reader.dn_hex()
    except TruncatedRecord:
        warnings.append("optional FTR pin arrays are truncated")
        data["_warnings"] = warnings
        return data

    text_fields: List[Tuple[str, Callable[[], Any]]] = [
        ("vector_name", reader.cn),
        ("time_set", reader.cn),
        ("op_code", reader.cn),
        ("test_name", reader.cn),
        ("alarm_id", reader.cn),
        ("program_text", reader.cn),
        ("result_text", reader.cn),
        ("patg_num", reader.u1),
        ("spin_map", reader.dn_hex),
    ]
    for field_name, fn in text_fields:
        ok, value = read_optional(reader, fn)
        if not ok:
            if reader.remaining:
                warnings.append(f"optional {field_name} is truncated")
            break
        data[field_name] = value
    if warnings:
        data["_warnings"] = warnings
    return data


def flags_hex(value: Any) -> str:
    return f"0x{int(value):02X}" if value is not None else ""


def test_result_from_flags(test_flg: Any) -> str:
    if test_flg is None:
        return ""
    if int(test_flg) & 0x40:
        return ""
    return "FAIL" if int(test_flg) & 0x80 else "PASS"


def numeric_result_valid(test_flg: Any, parm_flg: Any) -> bool:
    if test_flg is None:
        return False
    # Alarm, invalid result, unreliable, timeout, not executed, or abort.
    test_unusable = 0x01 | 0x02 | 0x04 | 0x08 | 0x10 | 0x20
    # Scale error, drift, or oscillation.
    parm_unusable = 0x01 | 0x02 | 0x04
    return not (int(test_flg) & test_unusable or int(parm_flg or 0) & parm_unusable)


def resolve_test_defaults(
    record: Dict[str, Any], kind: str, state: ConversionState
) -> Dict[str, Any]:
    opt_flag = int(record.get("opt_flag") or 0)
    missing: set = set()

    if kind in ("PTR", "MPR"):
        if "res_scal" not in record or opt_flag & 0x01:
            missing.add("res_scal")
        if "llm_scal" not in record or opt_flag & 0x50:
            missing.add("llm_scal")
        if "hlm_scal" not in record or opt_flag & 0xA0:
            missing.add("hlm_scal")
        if "lo_limit" not in record or opt_flag & 0x50:
            missing.add("lo_limit")
        if "hi_limit" not in record or opt_flag & 0xA0:
            missing.add("hi_limit")
        if "lo_spec" not in record or opt_flag & 0x04:
            missing.add("lo_spec")
        if "hi_spec" not in record or opt_flag & 0x08:
            missing.add("hi_spec")

        if opt_flag & 0x40:
            record["lo_limit"] = None
            record["llm_scal"] = None
        if opt_flag & 0x80:
            record["hi_limit"] = None
            record["hlm_scal"] = None
        if opt_flag & 0x04:
            record["lo_spec"] = None
        if opt_flag & 0x08:
            record["hi_spec"] = None

    default_fields = PTR_DEFAULT_FIELDS if kind in ("PTR", "MPR") else FTR_DEFAULT_FIELDS
    for field_name in default_fields:
        value = record.get(field_name)
        if value is None or (isinstance(value, str) and value == ""):
            missing.add(field_name)

    key = (kind, int(record.get("test_num") or 0))
    defaults = state.test_defaults.setdefault(key, {})
    used: List[str] = []

    for field_name in default_fields:
        if field_name in missing and field_name in defaults:
            record[field_name] = defaults[field_name]
            used.append(field_name)

    for field_name in default_fields:
        value = record.get(field_name)
        if field_name not in missing and value is not None and not (
            isinstance(value, str) and value == ""
        ):
            defaults[field_name] = value

    if used:
        record["used_defaults"] = ";".join(sorted(used))
    return record


def scaled_value(raw: Any, exponent: Any) -> Any:
    if raw is None:
        return None
    try:
        raw_float = float(raw)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(raw_float):
        return None
    try:
        scale = int(exponent) if exponent is not None else 0
        scaled = raw_float * (10.0 ** scale)
    except (OverflowError, TypeError, ValueError):
        return None
    return scaled if math.isfinite(scaled) else None


def format_number(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            return ""
        return format(value, ".15g")
    return value


def make_test_rows(record: Dict[str, Any], kind: str, record_index: int) -> List[Dict[str, Any]]:
    base: Dict[str, Any] = {
        "source_record": kind,
        "source_record_index": record_index,
        "head_num": record.get("head_num"),
        "site_num": record.get("site_num"),
        "test_num": record.get("test_num"),
        "test_name": record.get("test_name"),
        "test_type": kind,
        "test_result": test_result_from_flags(record.get("test_flg")),
        "test_flags": flags_hex(record.get("test_flg")),
        "param_flags": flags_hex(record.get("parm_flg")),
        "alarm_id": record.get("alarm_id"),
        "vector_name": record.get("vector_name"),
        "time_set": record.get("time_set"),
        "op_code": record.get("op_code"),
        "program_text": record.get("program_text"),
        "result_text": record.get("result_text"),
        "res_format": record.get("res_format"),
        "lo_limit_format": record.get("lo_limit_format"),
        "hi_limit_format": record.get("hi_limit_format"),
        "used_defaults": record.get("used_defaults"),
    }

    if kind in ("PTR", "MPR"):
        result_valid = numeric_result_valid(record.get("test_flg"), record.get("parm_flg"))
        res_scale = record.get("res_scal")
        llm_scale = record.get("llm_scal")
        hlm_scale = record.get("hlm_scal")
        base.update(
            {
                "result_scale": res_scale,
                "units": record.get("units"),
                "lo_limit": format_number(scaled_value(record.get("lo_limit"), llm_scale)),
                "lo_limit_raw": format_number(record.get("lo_limit")),
                "lo_limit_scale": llm_scale,
                "hi_limit": format_number(scaled_value(record.get("hi_limit"), hlm_scale)),
                "hi_limit_raw": format_number(record.get("hi_limit")),
                "hi_limit_scale": hlm_scale,
                "lo_spec": format_number(record.get("lo_spec")),
                "hi_spec": format_number(record.get("hi_spec")),
                "result_valid": "1" if result_valid else "0",
            }
        )

        if kind == "PTR":
            raw = record.get("result_raw")
            base.update(
                {
                    "result": (
                        format_number(scaled_value(raw, res_scale)) if result_valid else ""
                    ),
                    "result_raw": format_number(raw),
                }
            )
            return [base]

        results = list(record.get("rtn_rslt") or [])
        states = list(record.get("rtn_stat") or [])
        indexes = list(record.get("rtn_indx") or [])
        if not results:
            return [base]
        rows: List[Dict[str, Any]] = []
        for index, raw in enumerate(results, start=1):
            row = dict(base)
            row.update(
                {
                    "result": (
                        format_number(scaled_value(raw, res_scale)) if result_valid else ""
                    ),
                    "result_raw": format_number(raw),
                    "result_index": index,
                    "pin_index": indexes[index - 1] if index <= len(indexes) else "",
                    "return_state": (
                        f"0x{states[index - 1]:X}" if index <= len(states) else ""
                    ),
                }
            )
            rows.append(row)
        return rows

    base["result_valid"] = "1" if not (int(record.get("test_flg") or 0) & 0x40) else "0"
    return [base]


def part_result(part_flg: Any) -> str:
    if part_flg is None or not (int(part_flg) & 0x10):
        return ""
    return "FAIL" if int(part_flg) & 0x08 else "PASS"


def utc_timestamp(timestamp: Any) -> str:
    if timestamp in (None, 0, 0xFFFFFFFF):
        return ""
    try:
        return datetime.fromtimestamp(int(timestamp), tz=timezone.utc).isoformat().replace(
            "+00:00", "Z"
        )
    except (OverflowError, OSError, ValueError):
        return ""


def base_context(state: ConversionState) -> Dict[str, Any]:
    start_time = state.wafer.get("start_time_unix", state.lot.get("start_time_unix"))
    return {
        "source_file": state.input_path.name,
        "stdf_version": state.stdf_version,
        "cpu_type": state.cpu_type,
        "lot_id": state.lot.get("lot_id", ""),
        "part_type": state.lot.get("part_type", ""),
        "node_name": state.lot.get("node_name", ""),
        "tester_type": state.lot.get("tester_type", ""),
        "job_name": state.lot.get("job_name", ""),
        "job_rev": state.lot.get("job_rev", ""),
        "sublot_id": state.lot.get("sublot_id", ""),
        "oper_name": state.lot.get("oper_name", ""),
        "exec_type": state.lot.get("exec_type", ""),
        "exec_ver": state.lot.get("exec_ver", ""),
        "test_code": state.lot.get("test_code", ""),
        "test_temperature": state.lot.get("test_temperature", ""),
        "package_type": state.lot.get("package_type", ""),
        "family_id": state.lot.get("family_id", ""),
        "facility_id": state.lot.get("facility_id", ""),
        "process_id": state.lot.get("process_id", ""),
        "flow_id": state.lot.get("flow_id", ""),
        "setup_id": state.lot.get("setup_id", ""),
        "design_rev": state.lot.get("design_rev", ""),
        "engineering_id": state.lot.get("engineering_id", ""),
        "wafer_id": state.wafer.get("wafer_id", ""),
        "wafer_index": state.wafer.get("wafer_index", ""),
        "start_time_unix": start_time if start_time is not None else "",
        "start_time_utc": utc_timestamp(start_time),
    }


def write_context_rows(
    state: ConversionState,
    context: PartContext,
    prr: Optional[Dict[str, Any]],
    writer: csv.DictWriter,
    incomplete: bool = False,
) -> int:
    if not context.rows:
        return 0

    state.part_counter += 1
    ctx = base_context(state)
    prr = prr or {}

    hard_bin = prr.get("hard_bin")
    soft_bin = prr.get("soft_bin")
    x_coord = prr.get("x_coord")
    y_coord = prr.get("y_coord")
    num_tests = prr.get("num_tests")

    ctx.update(
        {
            "part_index": state.part_counter,
            "part_id": prr.get("part_id", ""),
            "part_result": part_result(prr.get("part_flg")),
            "part_flags": flags_hex(prr.get("part_flg")),
            "hard_bin": "" if hard_bin in (None, 0xFFFF) else hard_bin,
            "soft_bin": "" if soft_bin in (None, 0xFFFF) else soft_bin,
            "x_coord": "" if x_coord in (None, -32768) else x_coord,
            "y_coord": "" if y_coord in (None, -32768) else y_coord,
            "num_tests": "" if num_tests in (None, 0xFFFF) else num_tests,
            "part_test_time_ms": prr.get("part_test_time_ms", ""),
        }
    )

    for row in context.rows:
        output = dict(ctx)
        output.update(row)
        if incomplete:
            output["part_result"] = ""
        writer.writerow({column: output.get(column, "") for column in CSV_COLUMNS})
        state.row_count += 1
    context.rows.clear()
    return state.row_count


def flush_all_contexts(
    state: ConversionState, writer: csv.DictWriter, incomplete: bool = False
) -> None:
    for context in state.contexts.values():
        write_context_rows(state, context, None, writer, incomplete=incomplete)
    state.contexts.clear()


def parse_stdf(input_path: Path, output_path: Path, overwrite: bool, encoding: str) -> Dict[str, Any]:
    started = time.monotonic()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists() and not overwrite:
        return {
            "status": "skipped",
            "input": str(input_path),
            "output": str(output_path),
            "reason": "output exists; pass --overwrite to replace it",
        }

    state = ConversionState(input_path=input_path)
    temp_path: Optional[str] = None
    try:
        fd, temp_path = tempfile.mkstemp(
            prefix=f".{output_path.name}.", suffix=".tmp", dir=str(output_path.parent)
        )
        os.close(fd)

        with open_input(input_path) as stream, open(
            temp_path, "w", encoding=encoding, newline=""
        ) as csv_stream:
            writer = csv.DictWriter(
                csv_stream, fieldnames=CSV_COLUMNS, lineterminator="\n", extrasaction="ignore"
            )
            writer.writeheader()

            first_header = stream.read(4)
            endian = detect_endianness(first_header)
            header = first_header
            record_index = 0

            while header:
                if len(header) != 4:
                    raise StdfError(f"truncated record header after record {record_index}")
                rec_len, rec_typ, rec_sub = struct.unpack(endian + "HBB", header)
                body = stream.read(rec_len)
                if len(body) != rec_len:
                    raise StdfError(
                        f"record {record_index + 1} body is truncated: "
                        f"expected {rec_len} bytes, got {len(body)}"
                    )
                record_index += 1
                name = RECORD_NAMES.get((rec_typ, rec_sub), f"UNKNOWN_{rec_typ}_{rec_sub}")
                state.count_record(name)

                if record_index == 1:
                    if name != "FAR":
                        raise StdfError("first record must be FAR")
                    if len(body) < 2:
                        raise StdfError("FAR record is missing CPU_TYPE or STDF_VER")
                    state.cpu_type = body[0]
                    state.stdf_version = body[1]
                    if state.stdf_version != 4:
                        raise StdfError(
                            f"unsupported STDF version {state.stdf_version}; "
                            "the converter supports STDF V4"
                        )
                    expected_endian = ">" if state.cpu_type == 1 else "<"
                    if state.cpu_type in (0, 1, 2) and endian != expected_endian:
                        state.warn(
                            f"FAR CPU_TYPE={state.cpu_type} normally implies "
                            f"{'big' if expected_endian == '>' else 'little'}-endian data; "
                            f"detected {'big' if endian == '>' else 'little'}-endian records"
                        )
                    if state.cpu_type not in (1, 2):
                        state.warn(
                            f"CPU_TYPE={state.cpu_type} is not a standard IEEE CPU_TYPE 1/2; "
                            "numeric decoding is best-effort"
                        )
                    header = stream.read(4)
                    continue

                reader = Reader(body, endian, state.cpu_type)
                try:
                    if name == "MIR":
                        state.lot = parse_mir(reader)
                        for warning in state.lot.pop("_warnings", []):
                            state.warn(f"MIR: {warning}")
                    elif name == "WIR":
                        state.wafer_counter += 1
                        state.wafer = parse_wir(reader)
                        state.wafer["wafer_index"] = state.wafer_counter
                    elif name == "PIR":
                        head_num, site_num = reader.u1(), reader.u1()
                        key = (head_num, site_num)
                        old = state.contexts.get(key)
                        if old and old.rows:
                            state.warn(
                                f"new PIR at head/site {key} closed an unfinished part sequence"
                            )
                            write_context_rows(state, old, None, writer, incomplete=True)
                        state.contexts[key] = PartContext(head_num, site_num)
                    elif name == "PRR":
                        prr = parse_prr(reader)
                        for warning in prr.pop("_warnings", []):
                            state.warn(f"PRR: {warning}")
                        key = (prr.get("head_num"), prr.get("site_num"))
                        context = state.contexts.pop(key, PartContext(*key))
                        write_context_rows(state, context, prr, writer)
                    elif name == "PTR":
                        record = resolve_test_defaults(parse_ptr(reader), "PTR", state)
                        for warning in record.pop("_warnings", []):
                            state.warn(f"PTR record {record_index}: {warning}")
                        rows = make_test_rows(record, "PTR", record_index)
                        append_test_rows(state, record, rows)
                    elif name == "MPR":
                        record = resolve_test_defaults(parse_mpr(reader), "MPR", state)
                        for warning in record.pop("_warnings", []):
                            state.warn(f"MPR record {record_index}: {warning}")
                        rows = make_test_rows(record, "MPR", record_index)
                        append_test_rows(state, record, rows)
                    elif name == "FTR":
                        record = resolve_test_defaults(parse_ftr(reader), "FTR", state)
                        for warning in record.pop("_warnings", []):
                            state.warn(f"FTR record {record_index}: {warning}")
                        rows = make_test_rows(record, "FTR", record_index)
                        append_test_rows(state, record, rows)
                    elif reader.remaining:
                        # Unsupported record types are counted, but not emitted.
                        pass
                except UnsupportedFloatEncoding:
                    raise
                except TruncatedRecord as exc:
                    raise StdfError(f"{name} record {record_index}: {exc}") from exc

                header = stream.read(4)

            flush_all_contexts(state, writer, incomplete=True)
            csv_stream.flush()
            os.fsync(csv_stream.fileno())
            if state.row_count == 0:
                state.warn(
                    "no PTR, MPR, or FTR test rows were found; the CSV contains only its header"
                )
            if state.record_counts.get("STR", 0):
                state.warn(
                    f"{state.record_counts['STR']} STR record(s) were counted but not decoded"
                )

        os.replace(temp_path, output_path)
        temp_path = None
        return {
            "status": "converted",
            "input": str(input_path),
            "output": str(output_path),
            "rows": state.row_count,
            "parts_written": state.part_counter,
            "records": sum(state.record_counts.values()),
            "record_counts": state.record_counts,
            "warnings_count": state.warning_count,
            "warnings": state.warnings,
            "elapsed_seconds": round(time.monotonic() - started, 3),
        }
    except Exception as exc:
        return {
            "status": "failed",
            "input": str(input_path),
            "output": str(output_path),
            "error": str(exc),
            "warnings_count": state.warning_count,
            "warnings": state.warnings,
            "elapsed_seconds": round(time.monotonic() - started, 3),
        }
    finally:
        if temp_path:
            try:
                os.unlink(temp_path)
            except FileNotFoundError:
                pass


def append_test_rows(
    state: ConversionState, record: Dict[str, Any], rows: Sequence[Dict[str, Any]]
) -> None:
    key = (record.get("head_num"), record.get("site_num"))
    context = state.contexts.get(key)
    if context is None:
        if key not in state.orphan_sites:
            state.warn(f"test records at head/site {key} had no preceding PIR")
            state.orphan_sites.add(key)
        context = PartContext(*key)
        state.contexts[key] = context
    context.rows.extend(rows)


def strip_stdf_extension(path: Path) -> str:
    name = path.name
    lower = name.lower()
    for extension in STDF_EXTENSIONS:
        if lower.endswith(extension):
            return name[: -len(extension)]
    return path.stem


def collect_inputs(raw_paths: Iterable[str], recursive: bool) -> Tuple[List[Path], List[Dict[str, Any]]]:
    inputs: List[Path] = []
    errors: List[Dict[str, Any]] = []
    seen: set = set()

    for raw in raw_paths:
        path = Path(raw).expanduser()
        if not path.exists():
            errors.append({"status": "failed", "input": str(path), "error": "path does not exist"})
            continue
        if path.is_file():
            candidates = [path]
        elif path.is_dir():
            iterator = path.rglob("*") if recursive else path.glob("*")
            candidates = [
                candidate
                for candidate in iterator
                if candidate.is_file()
                and candidate.name.lower().endswith(STDF_EXTENSIONS)
            ]
            if not candidates:
                errors.append(
                    {
                        "status": "failed",
                        "input": str(path),
                        "error": "no supported STDF files found in directory",
                    }
                )
                continue
        else:
            errors.append(
                {"status": "failed", "input": str(path), "error": "not a regular file or directory"}
            )
            continue

        for candidate in sorted(candidates, key=lambda item: str(item).lower()):
            resolved = candidate.resolve()
            if resolved not in seen:
                seen.add(resolved)
                inputs.append(resolved)
    return inputs, errors


def plan_outputs(inputs: Sequence[Path], output_dir: Path) -> Dict[Path, Path]:
    planned: Dict[Path, Path] = {}
    used_names: set = set()
    for input_path in inputs:
        stem = strip_stdf_extension(input_path) or "output"
        candidate = f"{stem}.csv"
        suffix = 2
        while candidate.lower() in used_names:
            candidate = f"{stem}__{suffix}.csv"
            suffix += 1
        used_names.add(candidate.lower())
        planned[input_path] = output_dir / candidate
    return planned


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Convert one or more STDF V4 files into separate CSV files. "
            "Each CSV row is one PTR/FTR/MPR test result with lot, wafer, site, and part context."
        )
    )
    parser.add_argument("inputs", nargs="+", help="STDF files and/or directories")
    parser.add_argument(
        "-o",
        "--output-dir",
        default="stdf_csv",
        help="directory for generated CSV files (default: ./stdf_csv)",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="scan input directories recursively",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=0,
        help="parallel conversion workers; 0 selects up to 4 automatically",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="replace existing CSV files",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8-sig",
        help="CSV encoding (default: utf-8-sig for Excel-compatible Unicode)",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {VERSION}")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.workers < 0:
        parser.error("--workers must be 0 or greater")

    output_dir = Path(args.output_dir).expanduser().resolve()
    inputs, input_errors = collect_inputs(args.inputs, args.recursive)
    planned = plan_outputs(inputs, output_dir)

    results: List[Dict[str, Any]] = list(input_errors)
    pending: List[Path] = []
    for input_path in inputs:
        output_path = planned[input_path]
        if output_path.exists() and not args.overwrite:
            results.append(
                {
                    "status": "skipped",
                    "input": str(input_path),
                    "output": str(output_path),
                    "reason": "output exists; pass --overwrite to replace it",
                }
            )
        else:
            pending.append(input_path)

    if pending:
        output_dir.mkdir(parents=True, exist_ok=True)
        worker_count = args.workers or min(4, len(pending))
        with ThreadPoolExecutor(max_workers=max(1, worker_count)) as executor:
            futures = {
                executor.submit(
                    parse_stdf,
                    input_path,
                    planned[input_path],
                    args.overwrite,
                    args.encoding,
                ): input_path
                for input_path in pending
            }
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as exc:
                    input_path = futures[future]
                    results.append(
                        {"status": "failed", "input": str(input_path), "error": str(exc)}
                    )

    results.sort(key=lambda item: item.get("input", ""))
    converted = sum(item["status"] == "converted" for item in results)
    failed = sum(item["status"] == "failed" for item in results)
    skipped = sum(item["status"] == "skipped" for item in results)
    summary = {
        "ok": failed == 0,
        "input_count": len(inputs),
        "converted": converted,
        "skipped": skipped,
        "failed": failed,
        "output_dir": str(output_dir),
        "files": results,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
