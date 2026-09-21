import sys
import argparse
from pathlib import Path

from src.core.config import settings
from src.services.catalog_ingest import import_catalog_cold_assets

def main():
    parser = argparse.ArgumentParser(
        description="Bulk registration of video files as COLD VOD assets."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate import without modifying database."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of candidates to process."
    )
    parser.add_argument(
        "--root",
        type=str,
        default=None,
        help="Root directory to scan (defaults to INGEST_ROOT)."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=250,
        help="Batch commit size (default: 250)."
    )

    args = parser.parse_args()

    def print_progress(msg: str):
        print(f"[INFO] {msg}", flush=True)

    print("=" * 65)
    print("           VOD BULK CATALOG IMPORT (COLD ASSETS)")
    print("=" * 65)
    if args.dry_run:
        print("[MODE] *** DRY RUN ACTIVE — No changes will be made to database ***\n")
    else:
        print("[MODE] LIVE IMPORT — Assets will be registered as COLD in DB\n")

    try:
        res = import_catalog_cold_assets(
            root_dir=args.root,
            dry_run=args.dry_run,
            limit=args.limit,
            batch_size=args.batch_size,
            progress_callback=print_progress
        )
    except Exception as e:
        print(f"\n[ERROR] Catalog import failed: {e}", file=sys.stderr)
        sys.exit(1)

    print("\n" + "=" * 65)
    print("                   CATALOG IMPORT SUMMARY")
    print("=" * 65)
    print(f"Root path:          {res.root_path}")
    print(f"Mode:               {'DRY-RUN' if res.dry_run else 'LIVE'}")
    print(f"Scanned files:      {res.total_scanned}")
    print(f"{'Would create' if res.dry_run else 'Created'}:       {res.created}")
    print(f"Already existing:   {res.already_exists}")
    print(f"Conflicts:          {res.conflicts}")
    print(f"Invalid / Skipped:  {res.invalid}")
    print(f"Errors:             {res.errors}")
    print(f"Duration:           {res.duration_seconds:.4f}s ({res.assets_per_second:.2f} assets/sec)")
    if res.report_file:
        print(f"Report file:        {res.report_file}")
    print("=" * 65)

    if res.conflict_details:
        print(f"\n[WARNING] Conflicts detected ({len(res.conflict_details)}):")
        for c in res.conflict_details[:5]:
            print(f"  - {c.get('enlace_id')}: {c.get('reason')} ({c.get('first_path', c.get('existing_source_uri'))} vs {c.get('conflicting_path', c.get('new_source_uri'))})")
        if len(res.conflict_details) > 5:
            print(f"  ... and {len(res.conflict_details) - 5} more (see report file)")

    if res.invalid_details:
        print(f"\n[INFO] Invalid/Skipped items ({len(res.invalid_details)}):")
        for inv in res.invalid_details[:5]:
            print(f"  - {inv.get('path')}: {inv.get('reason')} - {inv.get('detail')}")
        if len(res.invalid_details) > 5:
            print(f"  ... and {len(res.invalid_details) - 5} more (see report file)")

    sys.exit(0 if res.errors == 0 else 1)

if __name__ == "__main__":
    main()
