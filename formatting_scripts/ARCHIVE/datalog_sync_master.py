#!/usr/bin/env python3
"""
datalog_sync_master.py

Master/orchestrator script for the "Data Time Synch" pipeline.

Current capabilities
--------------------
- inspect: parse/inspect both AiM and DataQ CSVs and print stats.
- merge:   detect segments via AiM + DataQ sync spikes, resample DataQ onto AiM time,
           and write a multi-sheet XLSX (one sheet per segment).

Planned extensions
------------------
- Replace the simple moving-average filter with a more explicit anti-aliasing filter if needed.
- Add optional diagnostics plots (sync waveform, spike detection, alignment checks).
"""

from __future__ import annotations

import argparse
from pathlib import Path

from formatting_scripts import aim_parse_segments as aim
from formatting_scripts import dataq_parse as dq
from formatting_scripts import sync_merge as sm


def cmd_inspect(aim_csv: Path, dataq_csv: Path) -> int:
    """Inspect both logs and print reports."""
    aim_result = aim.analyze_aim_csv(aim_csv)
    dq_result = dq.analyze_dataq_csv(dataq_csv)

    print("\n==================== AiM / MyChron ====================")
    aim.print_report(aim_result)

    print("\n==================== DataQ ============================")
    dq.print_report(dq_result)

    return 0


def cmd_merge(aim_csv: Path, dataq_csv: Path, out_xlsx: Path) -> int:
    """Merge to multi-sheet XLSX."""
    sm.merge_to_xlsx(aim_csv, dataq_csv, out_xlsx)
    print(f"Wrote: {out_xlsx}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Data Time Synch master script (orchestrator).")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_ins = sub.add_parser("inspect", help="Parse/inspect both AiM and DataQ CSVs and print stats.")
    p_ins.add_argument("aim_csv", type=Path, help="Path to AiM MyChron CSV export file.")
    p_ins.add_argument("dataq_csv", type=Path, help="Path to DataQ CSV export file.")

    p_m = sub.add_parser("merge", help="Merge AiM + DataQ into XLSX (one sheet per segment).")
    p_m.add_argument("aim_csv", type=Path, help="Path to AiM MyChron CSV export file.")
    p_m.add_argument("dataq_csv", type=Path, help="Path to DataQ CSV export file.")
    p_m.add_argument("out_xlsx", type=Path, help="Output XLSX filename.")

    args = parser.parse_args()

    if args.cmd == "inspect":
        return cmd_inspect(args.aim_csv, args.dataq_csv)

    if args.cmd == "merge":
        return cmd_merge(args.aim_csv, args.dataq_csv, args.out_xlsx)

    raise SystemExit("Unknown command")


if __name__ == "__main__":
    raise SystemExit(main())