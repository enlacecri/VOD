import sys
import argparse
import uuid
import json

from src.core.database import SessionLocal
from src.services.job_dispatch import batch_enqueue_asset

def main():
    parser = argparse.ArgumentParser(description="Enqueue an asset for background batch transcoding (vod_batch).")
    parser.add_argument("vod_uuid", nargs="?", default=None, help="VOD UUID of the asset")
    parser.add_argument("--enlace-id", dest="enlace_id", default=None, help="Enlace ID of the asset")
    args = parser.parse_args()

    identifier = args.vod_uuid or args.enlace_id
    if not identifier:
        print("[ERROR] Must provide either a vod_uuid or --enlace-id <id>", file=sys.stderr)
        parser.print_help()
        sys.exit(1)

    db = SessionLocal()
    try:
        res = batch_enqueue_asset(asset_identifier=identifier, db=db)
    except Exception as e:
        print(f"[ERROR] Exception during batch enqueue: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        db.close()

    status_code = res.get("status")
    print("=" * 60)
    print("               BATCH ENQUEUE RESULT")
    print("=" * 60)
    print(f"Status:       {status_code}")
    if "vod_uuid" in res:
        print(f"VOD UUID:     {res.get('vod_uuid')}")
    if "enlace_id" in res:
        print(f"Enlace ID:    {res.get('enlace_id')}")
    if "asset_status" in res:
        print(f"Asset Status: {res.get('asset_status')}")
    if "job_id" in res:
        print(f"Job ID:       {res.get('job_id')}")
    if "rq_job_id" in res:
        print(f"RQ Job ID:    {res.get('rq_job_id')}")
    if "queue_name" in res:
        print(f"Queue:        {res.get('queue_name')}")
    if "detail" in res:
        print(f"Detail:       {res.get('detail')}")
    print("=" * 60)

    if status_code in ("NOT_FOUND", "REQUIRES_EXPLICIT_RETRY") or status_code.startswith("UNSUPPORTED"):
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()
