import argparse
import sys
import os
from typing import Optional

from src.core.config import settings
from src.core.database import SessionLocal
from src.core.queues import get_redis_connection
from src.services.ranking.static_provider import StaticRankingProvider
from src.services.prewarm import PrewarmPlanner, PrewarmPlanResult, PrewarmRunResult


def print_plan_output(plan: PrewarmPlanResult) -> None:
    print("\nPREWARM PLAN\n")
    print(f"Provider:             {plan.provider_source}")
    print(f"Ranking size:         {plan.ranking_size:>6}")
    print(f"Target TOP N:         {plan.target_top_n:>6}")
    print(f"Matched assets:       {plan.matched_assets:>6}")
    print(f"Already READY:        {plan.already_ready:>6}")
    print(f"Active:               {plan.active:>6}")
    print(f"COLD candidates:      {plan.cold_candidates:>6}")
    print(f"FAILED:               {plan.failed_skipped:>6}")
    print(f"Missing catalog:      {plan.missing_catalog:>6}")
    print(f"Batch queue depth:    {plan.batch_queue_depth:>6}")
    print(f"Queue capacity:       {plan.queue_capacity:>6}")
    print(f"Would enqueue now:    {plan.would_enqueue_now:>6}\n")


def print_run_output(run_res: PrewarmRunResult) -> None:
    plan = run_res.plan
    print("\nPREWARM RUN SUMMARY\n")
    print(f"Snapshot:             {str(run_res.snapshot_id) if run_res.snapshot_id else 'N/A'}")
    print(f"Provider:             {plan.provider_source}")
    print(f"Target TOP:           {plan.target_top_n:>6}")
    print(f"Examined:             {plan.ranking_size:>6}")
    print(f"Matched:              {plan.matched_assets:>6}")
    print(f"Ready:                {plan.already_ready:>6}")
    print(f"Active:               {plan.active:>6}")
    print(f"Failed:               {plan.failed_skipped:>6}")
    print(f"Missing:              {plan.missing_catalog:>6}")
    print(f"Cold candidates:      {plan.cold_candidates:>6}")
    print(f"Batch depth before:   {plan.batch_queue_depth:>6}")
    print(f"Batch capacity:       {plan.queue_capacity:>6}")
    print(f"Enqueued:             {run_res.enqueued_count:>6}")
    print(f"Batch depth after:    {run_res.batch_queue_depth_after:>6}")
    print(f"Duration:             {run_res.duration_seconds:.2f}s")
    print(f"Status:               {run_res.status}")
    if run_res.error:
        print(f"Error:                {run_res.error}")
    print("")


def run_cli():
    parser = argparse.ArgumentParser(description="VOD Phase 4 Prewarming CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # plan subcommand
    plan_parser = subparsers.add_parser("plan", help="Execute 100%% dry-run prewarm plan")
    plan_parser.add_argument("--ranking-file", required=True, help="Path to JSON ranking file")
    plan_parser.add_argument("--top", type=int, default=None, help="Target TOP N items to analyze")
    plan_parser.add_argument("--limit", type=int, default=None, help="Maximum items to enqueue in this run")

    # run subcommand
    run_parser = subparsers.add_parser("run", help="Execute batch prewarming")
    run_parser.add_argument("--ranking-file", required=True, help="Path to JSON ranking file")
    run_parser.add_argument("--top", type=int, default=None, help="Target TOP N items to analyze")
    run_parser.add_argument("--limit", type=int, default=None, help="Maximum items to enqueue in this run")
    run_parser.add_argument("--dry-run", action="store_true", help="Simulate run without writing to DB or Redis")

    args = parser.parse_args()

    if not os.path.exists(args.ranking_file):
        print(f"Error: Ranking file '{args.ranking_file}' does not exist.", file=sys.stderr)
        sys.exit(1)

    try:
        provider = StaticRankingProvider(args.ranking_file)
    except Exception as e:
        print(f"Error loading ranking provider: {e}", file=sys.stderr)
        sys.exit(1)

    db = SessionLocal()
    try:
        redis_conn = get_redis_connection()
        planner = PrewarmPlanner(db=db, redis_conn=redis_conn)

        if args.command == "plan":
            plan_res = planner.plan_prewarm(
                provider=provider,
                target_top_n=args.top,
                enqueue_limit=args.limit,
            )
            print_plan_output(plan_res)
        elif args.command == "run":
            run_res = planner.run_prewarm(
                provider=provider,
                target_top_n=args.top,
                enqueue_limit=args.limit,
                dry_run=args.dry_run,
            )
            if args.dry_run:
                print_plan_output(run_res.plan)
            else:
                print_run_output(run_res)
    finally:
        db.close()


if __name__ == "__main__":
    run_cli()
