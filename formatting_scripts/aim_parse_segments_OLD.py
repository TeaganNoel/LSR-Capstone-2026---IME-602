#!/usr/bin/env python3
"""
aim_parse_segments.py

Purpose
-------
Parse an AiM MyChron CSV export and verify we can robustly locate:
  1) The "Beacon Markers" row and its segment end-times (found in column B).
  2) The time-series header rows:
       - A row in column A whose cell is exactly "sec" (case-insensitive).
       - The previous row (often the "logical meaning" row such as "Time", "Speed", etc.).
  3) The time-series data block:
       - Starts 3 rows after the "sec" row (the next 2 rows are ignored).
       - Column A contains decimal time in seconds.
       - Next N columns contain channel values.

Header construction (generalized)
--------------------------------
Starting at column A on the 'sec' row, scan to the right. For each column where the
'sec' row cell is non-blank, treat it as a valid column in the time-series table.
Stop at the first blank cell on the 'sec' row.

For each valid column, construct a combined header string:
  header[col] = <prev_row_cell> + " " + <sec_row_cell>

Time rounding policy
--------------------
Some segment end-times (Beacon Markers) and time-series values may include hundredths.
To ensure consistent alignment, we round:
  - Beacon Markers parsed times to nearest 0.1 s
  - Time-series column A values to nearest 0.1 s

All printed reporting is shown to 0.1 s.

Segment detection (preferred)
-----------------------------
Segments are detected by finding rows in the time-series data where time == 0 (within tolerance).
There should be exactly one such "0-time" row per segment.

The program:
  - Finds all 0-time rows and uses them to define segment start/end row ranges.
  - Checks that the number of 0-time rows equals the number of segment end-times parsed
    from the "Beacon Markers" row (if present).
  - Reports each segment by inclusive start/end row indices (1-based, Excel-style).

Standalone + importable
-----------------------
This file can be:
  - Run directly to print a report (current behavior preserved).
  - Imported by a master script. Public entry points:
        analyze_aim_csv(path: Path) -> AimParseResult
        print_report(result: AimParseResult) -> None

Notes
-----
- CSV delimiter varies across AiM exports; we sniff the dialect.
- Beacon markers field may contain comma-separated floats inside a single CSV field
  (usually quoted). We extract floats via regex for robustness.
- Decimal comma is handled for numeric parsing (e.g., "12,34" -> 12.34) when needed.
"""

from __future__ import annotations

import argparse
import csv
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple


# ----------------------------- Utility helpers ----------------------------- #

def col_idx_to_excel_letter(idx0: int) -> str:
    """Convert 0-based column index to Excel letters: 0->A, 1->B, ..., 25->Z, 26->AA, ..."""
    n = idx0 + 1
    letters = []
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters.append(chr(ord("A") + rem))
    return "".join(reversed(letters))


def normalize_cell(s: Optional[str]) -> str:
    """Normalize a CSV cell for robust comparisons."""
    if s is None:
        return ""
    return str(s).strip()


def parse_float_maybe(s: str) -> Optional[float]:
    """
    Parse a float from a string that might use '.' or ',' as decimal separator.
    Returns None if not parseable.
    """
    t = normalize_cell(s)
    if t == "":
        return None
    try:
        return float(t)
    except ValueError:
        # Try decimal-comma conversion if it looks like a numeric token.
        # Avoid converting strings with multiple commas that are clearly lists.
        if "," in t and t.count(",") == 1 and "." not in t:
            try:
                return float(t.replace(",", "."))
            except ValueError:
                return None
        return None


def round_to_0p1(x: float) -> float:
    """
    Round to nearest 0.1 seconds (one decimal place), avoiding banker's rounding.
    Works for non-negative times; also handles negative values sanely if present.
    """
    ax = abs(x)
    y = math.floor(ax * 10.0 + 0.5) / 10.0
    return math.copysign(y, x)


_FLOAT_RE = re.compile(r"[-+]?\d+(?:[.,]\d+)?(?:[eE][-+]?\d+)?")


def extract_floats_from_text(s: str) -> List[float]:
    """
    Extract float-like tokens from a text blob and return as floats.
    Handles decimal comma tokens too.
    """
    out: List[float] = []
    for m in _FLOAT_RE.finditer(s):
        tok = m.group(0)
        # Normalize decimal comma -> dot if needed
        if "," in tok and "." not in tok:
            tok = tok.replace(",", ".")
        try:
            out.append(float(tok))
        except ValueError:
            pass
    return out


def fmt_0p1(x: float) -> str:
    """Format a float to 0.1 seconds for printing."""
    return f"{x:.1f}"


# ----------------------------- Core parsing ----------------------------- #

def read_csv_table(path: Path, sniff_bytes: int = 65536) -> List[List[str]]:
    """
    Read CSV into a rectangular-ish list of rows (ragged rows allowed).
    Sniffs CSV dialect for delimiter/quotechar/newline.
    """
    raw = path.read_bytes()
    sample = raw[:sniff_bytes].decode("utf-8", errors="replace")

    try:
        dialect = csv.Sniffer().sniff(sample)
    except csv.Error:
        # Fallback: assume comma-delimited
        dialect = csv.excel

    text = raw.decode("utf-8", errors="replace").splitlines()

    rows: List[List[str]] = []
    reader = csv.reader(text, dialect)
    for r in reader:
        rows.append([c for c in r])

    return rows


def find_row_by_colA_value(rows: List[List[str]], target: str) -> Optional[int]:
    """
    Find the first row index (0-based) where column A matches target (case-insensitive).
    """
    tgt = target.strip().lower()
    for i, r in enumerate(rows):
        if len(r) == 0:
            continue
        a = normalize_cell(r[0]).lower()
        if a == tgt:
            return i
    return None


def get_cell(rows: List[List[str]], r0: int, c0: int) -> str:
    """Safe cell getter; returns '' if out of bounds."""
    if r0 < 0 or r0 >= len(rows):
        return ""
    row = rows[r0]
    if c0 < 0 or c0 >= len(row):
        return ""
    return normalize_cell(row[c0])


@dataclass(frozen=True)
class SegmentRange:
    segment_index: int
    start_row_1: int
    end_row_1: int
    start_time_s: float
    end_time_s: float


@dataclass(frozen=True)
class AimParseResult:
    """Container for parsed AiM CSV structure and derived segment ranges."""
    path: Path
    beacon_row_1: Optional[int]
    beacon_end_times_s: List[float]                  # rounded to 0.1s
    sec_row_1: int
    data_start_row_1: int
    data_end_row_1: Optional[int]
    time_span_s: Optional[Tuple[float, float]]       # rounded; may reset
    ncols: int
    combined_headers: List[Tuple[str, str]]          # (Excel col letter, combined header string)
    zero_time_rows_1: List[int]                      # rows where time ~= 0
    segments: List[SegmentRange]                     # start/end times rounded to 0.1s
    count_check_ok: Optional[bool]                   # None if no beacon times, else True/False


def parse_beacon_end_times(rows: List[List[str]]) -> Tuple[Optional[int], List[float]]:
    """
    Locate 'Beacon Markers' in col A and parse segment end-times from col B.
    Times are rounded to nearest 0.1s.

    Returns (row_index_0based, sorted_end_times_rounded).
    """
    r0 = find_row_by_colA_value(rows, "Beacon Markers")
    if r0 is None:
        return None, []

    raw_times = get_cell(rows, r0, 1)  # column B
    times = extract_floats_from_text(raw_times)

    # Keep positive, round to 0.1s, sort, unique with tolerance
    times = [round_to_0p1(t) for t in times if t > 0]
    times.sort()

    # de-dupe lightly (after rounding)
    uniq: List[float] = []
    eps = 1e-9
    for t in times:
        if not uniq or abs(t - uniq[-1]) > eps:
            uniq.append(t)

    return r0, uniq


def detect_ncols_from_sec_row(rows: List[List[str]], sec_row0: int) -> int:
    """
    Determine the number of time-series columns by scanning the 'sec' row left-to-right:
      - Start at col A (0)
      - Include columns while the 'sec' row cell is non-blank
      - Stop at the first blank cell
    Returns ncols (>= 0). In normal files, ncols >= 1.
    """
    sec = rows[sec_row0]
    ncols = 0
    for c in range(len(sec)):
        if normalize_cell(sec[c]) == "":
            break
        ncols += 1
    return ncols


def parse_combined_headers(rows: List[List[str]], sec_row0: int, ncols: int) -> List[Tuple[str, str]]:
    """
    For the first ncols columns (A..), construct a single combined header string:
        "<prev_row_cell> <sec_row_cell>"
    where 'prev' is the row before the 'sec' row.

    Returns a list of (excel_col_letter, combined_header_string).
    """
    prev = rows[sec_row0 - 1] if sec_row0 - 1 >= 0 else []
    sec = rows[sec_row0]

    combined: List[Tuple[str, str]] = []
    for c in range(ncols):
        col_letter = col_idx_to_excel_letter(c)
        prev_s = normalize_cell(prev[c]) if c < len(prev) else ""
        sec_s = normalize_cell(sec[c]) if c < len(sec) else ""
        label = " ".join([p for p in (prev_s, sec_s) if p]).strip()
        combined.append((col_letter, label))
    return combined


def extract_time_series_index(rows: List[List[str]], data_start0: int) -> List[Tuple[int, float]]:
    """
    From data_start0 onward, collect (row_index_1based, time_s_rounded) pairs until time parsing fails.
    Stops on the first non-numeric time AFTER we have started collecting.

    Time values are rounded to nearest 0.1s.
    """
    ts: List[Tuple[int, float]] = []
    started = False
    for r0 in range(data_start0, len(rows)):
        t_cell = get_cell(rows, r0, 0)  # column A
        t = parse_float_maybe(t_cell)
        if t is None:
            if started:
                break
            else:
                continue
        started = True
        ts.append((r0 + 1, round_to_0p1(float(t))))
    return ts


def find_segment_start_indices(time_rows: List[Tuple[int, float]], zero_tol: float = 1e-9) -> List[int]:
    """
    Find indices in time_rows where time is (approximately) 0.0.
    These indices define segment starts.

    Returns indices into the time_rows list (0-based).
    """
    starts: List[int] = []
    for i, (_row_1, t) in enumerate(time_rows):
        if abs(t) <= zero_tol:
            starts.append(i)
    return starts


def derive_segment_ranges_from_time_resets(
    time_rows: List[Tuple[int, float]],
    zero_tol: float = 1e-9,
) -> List[SegmentRange]:
    """
    Derive segment row ranges using time resets (t == 0) within the time-series data.

    - Each segment begins at a row where time ~= 0.
    - A segment ends at the row just before the next time-reset row,
      or at the final time-series row for the last segment.

    Returns list of SegmentRange objects with 1-based inclusive row indices.
    """
    if not time_rows:
        return []

    start_indices = find_segment_start_indices(time_rows, zero_tol=zero_tol)
    if not start_indices:
        return [SegmentRange(
            segment_index=1,
            start_row_1=time_rows[0][0],
            end_row_1=time_rows[-1][0],
            start_time_s=time_rows[0][1],
            end_time_s=time_rows[-1][1],
        )]

    ranges: List[SegmentRange] = []
    for seg_i, start_idx in enumerate(start_indices, start=1):
        end_idx = (start_indices[seg_i] - 1) if seg_i < len(start_indices) else (len(time_rows) - 1)
        start_row_1, start_time = time_rows[start_idx]
        end_row_1, end_time = time_rows[end_idx]
        ranges.append(SegmentRange(seg_i, start_row_1, end_row_1, start_time, end_time))

    return ranges


def analyze_aim_csv(path: Path) -> AimParseResult:
    """
    High-level parse routine suitable for both CLI use and import by a master script.
    Returns an AimParseResult containing the parsed structure and derived segments.
    """
    rows = read_csv_table(path)

    beacon_row0, end_times = parse_beacon_end_times(rows)
    sec_row0 = find_row_by_colA_value(rows, "sec")
    if sec_row0 is None:
        raise ValueError("Could not find a row where column A == 'sec'.")

    data_start0 = sec_row0 + 3  # 'sec' row + 2 ignored rows + first data row

    ncols = detect_ncols_from_sec_row(rows, sec_row0)
    combined_headers = parse_combined_headers(rows, sec_row0, ncols=ncols)

    time_rows = extract_time_series_index(rows, data_start0)

    segments = derive_segment_ranges_from_time_resets(time_rows, zero_tol=1e-9)
    zero_start_indices = find_segment_start_indices(time_rows, zero_tol=1e-9)
    zero_time_rows_1 = [time_rows[i][0] for i in zero_start_indices] if time_rows else []

    if len(end_times) == 0:
        count_check_ok = None
    else:
        count_check_ok = (len(zero_time_rows_1) == len(end_times))

    data_end_row_1 = time_rows[-1][0] if time_rows else None
    time_span_s = (time_rows[0][1], time_rows[-1][1]) if time_rows else None

    return AimParseResult(
        path=path,
        beacon_row_1=(beacon_row0 + 1) if beacon_row0 is not None else None,
        beacon_end_times_s=end_times,
        sec_row_1=sec_row0 + 1,
        data_start_row_1=data_start0 + 1,
        data_end_row_1=data_end_row_1,
        time_span_s=time_span_s,
        ncols=ncols,
        combined_headers=combined_headers,
        zero_time_rows_1=zero_time_rows_1,
        segments=segments,
        count_check_ok=count_check_ok,
    )


def print_report(r: AimParseResult) -> None:
    """Print a human-readable report matching the prior CLI output style (plus ncols)."""
    print("\n=== File ===")
    print(str(r.path))

    print("\n=== Beacon Markers ===")
    if r.beacon_row_1 is None:
        print("Beacon Markers row: NOT FOUND")
        print("Segment end-times: (none)")
    else:
        print(f"Beacon Markers row: {r.beacon_row_1}")
        end_times_str = "[" + ", ".join(fmt_0p1(t) for t in r.beacon_end_times_s) + "]"
        print(f"Segment end-times (s, rounded to 0.1): {end_times_str}")
        print(f"Count of end-times: {len(r.beacon_end_times_s)}")

    print("\n=== Headers (combined from prev row + 'sec' row) ===")
    print(f"'sec' header row: {r.sec_row_1}")
    print(f"Detected number of columns (stop at first blank on 'sec' row): {r.ncols}")
    for col_letter, label in r.combined_headers:
        print(f"  {col_letter}: {label}")

    print("\n=== Data block ===")
    print(f"Data block start row (1-based): {r.data_start_row_1}")
    if r.data_end_row_1 is not None:
        print(f"Data block end row (1-based): {r.data_end_row_1}")

        # Total time span across all identified segments (sum of per-segment durations).
        # This is robust even when time resets to 0 at the start of each segment.
        if r.segments:
            total_span_s = sum((s.end_time_s - s.start_time_s) for s in r.segments)
            print(f"Total time span (sum across segments): {fmt_0p1(total_span_s)} s")
        elif r.time_span_s is not None:
            total_span_s = r.time_span_s[1] - r.time_span_s[0]
            print(f"Total time span: {fmt_0p1(total_span_s)} s")
        else:
            print("Total time span: (unknown)")
    else:
        print("Data block end row: (no numeric time rows found)")

    print("\n=== Segment detection via time resets (t ~= 0) ===")
    if r.data_end_row_1 is None:
        print("(no time-series rows)")
    else:
        print(f"0-time rows (segment starts): {r.zero_time_rows_1}")
        print(f"Count of 0-time rows: {len(r.zero_time_rows_1)}")
        if r.count_check_ok is None:
            if r.beacon_row_1 is not None:
                print("Count check: beacon end-times present? (no) -> skipped")
        else:
            print(
                f"Count check (0-time rows vs beacon end-times): "
                f"{len(r.zero_time_rows_1)} vs {len(r.beacon_end_times_s)} -> "
                f"{'OK' if r.count_check_ok else 'MISMATCH'}"
            )

    print("\n=== Segments (row ranges are inclusive, 1-based) ===")
    if not r.segments:
        print("(no segments derived)")
    else:
        print(f"Number of segments derived: {len(r.segments)}")
        for s in r.segments:
            print(
                f"Segment {s.segment_index}: rows {s.start_row_1} .. {s.end_row_1} "
                f"(t={fmt_0p1(s.start_time_s)} .. {fmt_0p1(s.end_time_s)} s)"
            )

    print()


# ----------------------------- CLI entry point ----------------------------- #

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Parse AiM MyChron CSV: locate combined headers, data block, and segment row ranges."
    )
    parser.add_argument("aim_csv", type=Path, help="Path to AiM CSV export file.")
    args = parser.parse_args()

    try:
        result = analyze_aim_csv(args.aim_csv)
    except Exception as e:
        raise SystemExit(f"ERROR: {e}")

    print_report(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())