#!/usr/bin/env python3
"""
dataq_parse.py

Purpose
-------
Parse a DataQ DI-4108E CSV export and report structural stats needed for later
downsampling + alignment with AiM/MyChron data.

Assumptions / conventions
-------------------------
- The CSV contains two adjacent header rows for the time-series table:
    * The second header row is identified by a cell in column A equal to "sec"
      (case-insensitive).
    * The row immediately above it provides the "logical meaning" portion of each header.
- The number of columns in the time-series table is determined by scanning the "sec" row
  from column A to the right until the first blank cell.

Data block detection
--------------------
Unlike the AiM parser, this parser does not assume fixed "ignored rows" after the header.
Instead it robustly finds the first numeric time value in column A after the "sec" row,
then continues until time parsing fails.

Outputs (standalone)
--------------------
- 1-based row indices (Excel style)
- Header row index, number of columns, and combined header strings
- First and last data row indices
- First/last time values and total time spanned (last - first)

Importable API
--------------
- analyze_dataq_csv(path: Path) -> DataQParseResult
- print_report(result: DataQParseResult) -> None
"""

from __future__ import annotations

import argparse
import csv
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


# ----------------------------- CSV reading + locating ----------------------------- #

def read_csv_table(path: Path, sniff_bytes: int = 65536) -> List[List[str]]:
    """
    Read CSV into a list of rows (ragged rows allowed).
    Sniffs CSV dialect for delimiter/quotechar/newline.
    """
    raw = path.read_bytes()
    sample = raw[:sniff_bytes].decode("utf-8", errors="replace")

    try:
        dialect = csv.Sniffer().sniff(sample)
    except csv.Error:
        dialect = csv.excel  # fallback

    text = raw.decode("utf-8", errors="replace").splitlines()

    rows: List[List[str]] = []
    reader = csv.reader(text, dialect)
    for r in reader:
        rows.append([c for c in r])
    return rows


def find_row_by_colA_value(rows: List[List[str]], target: str) -> Optional[int]:
    """Find the first row index (0-based) where column A matches target (case-insensitive)."""
    tgt = target.strip().lower()
    for i, r in enumerate(rows):
        if not r:
            continue
        if normalize_cell(r[0]).lower() == tgt:
            return i
    return None


def detect_ncols_from_sec_row(rows: List[List[str]], sec_row0: int) -> int:
    """
    Determine the number of time-series columns by scanning the 'sec' row left-to-right:
      - Start at col A (0)
      - Include columns while the 'sec' row cell is non-blank
      - Stop at the first blank cell
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
    For the first ncols columns, construct combined headers:
        "<prev_row_cell> <sec_row_cell>"
    Returns list of (Excel col letter, combined header string).
    """
    prev = rows[sec_row0 - 1] if sec_row0 - 1 >= 0 else []
    sec = rows[sec_row0]

    out: List[Tuple[str, str]] = []
    for c in range(ncols):
        col_letter = col_idx_to_excel_letter(c)
        prev_s = normalize_cell(prev[c]) if c < len(prev) else ""
        sec_s = normalize_cell(sec[c]) if c < len(sec) else ""
        label = " ".join([p for p in (prev_s, sec_s) if p]).strip()
        out.append((col_letter, label))
    return out


def extract_time_series_index(rows: List[List[str]], start_row0: int) -> List[Tuple[int, float]]:
    """
    From start_row0 onward:
      - find the first row with numeric time in column A
      - then collect contiguous numeric-time rows until time parsing fails

    Returns list of (row_index_1based, time_s).
    """
    ts: List[Tuple[int, float]] = []

    # Find first numeric time row
    r0 = start_row0
    while r0 < len(rows):
        t = parse_float_maybe(rows[r0][0] if rows[r0] else "")
        if t is not None:
            break
        r0 += 1

    if r0 >= len(rows):
        return ts

    # Collect contiguous numeric time rows
    for k in range(r0, len(rows)):
        t = parse_float_maybe(rows[k][0] if rows[k] else "")
        if t is None:
            break
        ts.append((k + 1, float(t)))

    return ts


# ----------------------------- Result container + reporting ----------------------------- #

@dataclass(frozen=True)
class DataQParseResult:
    path: Path
    sec_row_1: int
    ncols: int
    combined_headers: List[Tuple[str, str]]          # (Excel col letter, combined header string)
    first_data_row_1: Optional[int]
    last_data_row_1: Optional[int]
    first_time_s: Optional[float]
    last_time_s: Optional[float]
    total_span_s: Optional[float]
    n_data_rows: int


def analyze_dataq_csv(path: Path) -> DataQParseResult:
    """High-level parse routine suitable for CLI use or import by a master script."""
    rows = read_csv_table(path)

    sec_row0 = find_row_by_colA_value(rows, "sec")
    if sec_row0 is None:
        raise ValueError("Could not find a row where column A == 'sec'.")

    ncols = detect_ncols_from_sec_row(rows, sec_row0)
    headers = parse_combined_headers(rows, sec_row0, ncols=ncols)

    time_rows = extract_time_series_index(rows, start_row0=sec_row0 + 1)

    if time_rows:
        first_row_1, first_t = time_rows[0]
        last_row_1, last_t = time_rows[-1]
        total_span = last_t - first_t
    else:
        first_row_1 = last_row_1 = None
        first_t = last_t = total_span = None

    return DataQParseResult(
        path=path,
        sec_row_1=sec_row0 + 1,
        ncols=ncols,
        combined_headers=headers,
        first_data_row_1=first_row_1,
        last_data_row_1=last_row_1,
        first_time_s=first_t,
        last_time_s=last_t,
        total_span_s=total_span,
        n_data_rows=len(time_rows),
    )


def print_report(r: DataQParseResult) -> None:
    """Print DataQ structural stats."""
    print("\n=== File ===")
    print(str(r.path))

    print("\n=== Headers (combined from prev row + 'sec' row) ===")
    print(f"'sec' header row: {r.sec_row_1}")
    print(f"Detected number of columns (stop at first blank on 'sec' row): {r.ncols}")
    for col_letter, label in r.combined_headers:
        print(f"  {col_letter}: {label}")

    print("\n=== Data block ===")
    if r.first_data_row_1 is None:
        print("First data row: (none found)")
        print("Last data row: (none found)")
        print("Total time spanned: (unknown)")
    else:
        print(f"First data row (1-based): {r.first_data_row_1}")
        print(f"Last data row (1-based):  {r.last_data_row_1}")
        print(f"Data rows: {r.n_data_rows}")
        print(f"First time (s): {r.first_time_s}")
        print(f"Last time (s):  {r.last_time_s}")
        print(f"Total time spanned (s): {r.total_span_s}")

    print()


# ----------------------------- CLI entry point ----------------------------- #

def main() -> int:
    parser = argparse.ArgumentParser(description="Parse DataQ CSV: headers + data block stats.")
    parser.add_argument("dataq_csv", type=Path, help="Path to DataQ CSV export file.")
    args = parser.parse_args()

    try:
        result = analyze_dataq_csv(args.dataq_csv)
    except Exception as e:
        raise SystemExit(f"ERROR: {e}")

    print_report(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())