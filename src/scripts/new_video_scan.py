import sys
import argparse
import logging
from src.services.new_video_scanner import scan_new_videos

logging.basicConfig(level=logging.INFO)

def main():
    parser = argparse.ArgumentParser(description="Scan and ingest new incoming videos.")
    parser.add_argument("--root", default=None, help="Root path of incoming new videos folder")
    parser.add_argument("--dry-run", action="store_true", help="Perform inspection without persisting to DB or Redis")
    parser.add_argument("--stable-seconds", type=int, default=None, help="Override stability observation window in seconds")
    args = parser.parse_args()

    result = scan_new_videos(
        root_path=args.root,
        dry_run=args.dry_run,
        stable_seconds=args.stable_seconds,
    )
    print(result.print_summary())

if __name__ == "__main__":
    main()
