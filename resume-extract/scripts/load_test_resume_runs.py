import argparse
import asyncio
import csv
import json
import os
import time
from collections import Counter
from typing import Any
from uuid import uuid4

import httpx


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_DB_SCHEMA = os.getenv("SUPABASE_DB_SCHEMA", "public")


class LoadTestError(Exception):
    pass


def _headers(*, prefer: str | None = None) -> dict[str, str]:
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise LoadTestError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set")
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Accept-Profile": SUPABASE_DB_SCHEMA,
        "Content-Profile": SUPABASE_DB_SCHEMA,
    }
    if prefer:
        headers["Prefer"] = prefer
    return headers


async def _fetch(
    client: httpx.AsyncClient,
    *,
    path: str,
    method: str,
    body: Any | None = None,
    prefer: str | None = None,
) -> Any:
    response = await client.request(
        method=method,
        url=f"{SUPABASE_URL}/rest/v1/{path}",
        headers=_headers(prefer=prefer),
        json=body,
    )
    if response.status_code >= 400:
        raise LoadTestError(f"{method} {path} failed: {response.status_code} {response.text}")
    if "application/json" in response.headers.get("content-type", ""):
        return response.json()
    return None


def _build_run_rows(args: argparse.Namespace) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index in range(args.count):
        rows.append(
            {
                "request_id": str(uuid4()),
                "user_id": args.user_id,
                "resume_path": args.resume_path,
                "resume_filename": args.resume_filename,
                "job_description": args.job_description_template.format(n=index + 1),
            }
        )
    return rows


async def _insert_runs(client: httpx.AsyncClient, rows: list[dict[str, Any]]) -> list[str]:
    inserted = await _fetch(
        client,
        path="resume_runs?select=id",
        method="POST",
        body=rows,
        prefer="return=representation",
    )
    if not isinstance(inserted, list):
        raise LoadTestError("Insert did not return a row list")
    run_ids: list[str] = []
    for row in inserted:
        run_id = row.get("id")
        if not isinstance(run_id, str):
            raise LoadTestError("Inserted row missing id")
        run_ids.append(run_id)
    return run_ids


async def _fetch_run_statuses(client: httpx.AsyncClient, run_ids: list[str]) -> list[dict[str, Any]]:
    ids = ",".join(run_ids)
    rows = await _fetch(
        client,
        path=f"resume_runs?id=in.({ids})&select=id,status,error_code,created_at,updated_at",
        method="GET",
    )
    if not isinstance(rows, list):
        raise LoadTestError("Status query did not return a row list")
    return rows


def _summarize(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter()
    for row in rows:
        status = row.get("status")
        if isinstance(status, str):
            counts[status] += 1
    return dict(counts)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Enqueue a batch of resume_runs and poll extractor drain progress.")
    parser.add_argument("--user-id", required=True, help="Existing auth.users UUID to own the test runs.")
    parser.add_argument("--resume-path", required=True, help="Existing storage path to a PDF or DOCX resume.")
    parser.add_argument("--count", type=int, default=20, help="Number of queued runs to insert.")
    parser.add_argument(
        "--job-description-template",
        default="Load test resume tailoring run #{n}",
        help="Python format string used for job_description. Supports {n}.",
    )
    parser.add_argument("--resume-filename", default="load-test-resume.pdf", help="resume_filename to store on each run.")
    parser.add_argument("--poll-interval", type=float, default=2.0, help="Seconds between status polls.")
    parser.add_argument("--timeout", type=float, default=900.0, help="Max seconds to wait for all runs to finish.")
    parser.add_argument("--summary-json", help="Optional path to write the final summary JSON.")
    parser.add_argument("--summary-csv", help="Optional path to write final per-run rows as CSV.")
    parser.add_argument("--dry-run", action="store_true", help="Print the payload summary without inserting any runs.")
    args = parser.parse_args()

    rows = _build_run_rows(args)
    if args.dry_run:
        preview = rows[: min(3, len(rows))]
        print(
            json.dumps(
                {
                    "count": len(rows),
                    "user_id": args.user_id,
                    "resume_path": args.resume_path,
                    "resume_filename": args.resume_filename,
                    "job_description_template": args.job_description_template,
                    "preview_rows": preview,
                },
                indent=2,
            )
        )
        return
    started_at = time.monotonic()

    async with httpx.AsyncClient(timeout=30.0) as client:
        run_ids = await _insert_runs(client, rows)
        print(f"Inserted {len(run_ids)} queued runs")
        print(json.dumps({"run_ids": run_ids}, indent=2))

        while True:
            status_rows = await _fetch_run_statuses(client, run_ids)
            summary = _summarize(status_rows)
            elapsed = time.monotonic() - started_at
            print(json.dumps({"elapsed_seconds": round(elapsed, 1), "status_counts": summary}, sort_keys=True))

            terminal = summary.get("extracted", 0) + summary.get("failed", 0)
            if terminal >= len(run_ids):
                final = {
                    "run_ids": run_ids,
                    "elapsed_seconds": round(elapsed, 3),
                    "status_counts": summary,
                    "rows": status_rows,
                }
                if args.summary_json:
                    with open(args.summary_json, "w", encoding="utf-8") as handle:
                        json.dump(final, handle, indent=2)
                if args.summary_csv:
                    with open(args.summary_csv, "w", encoding="utf-8", newline="") as handle:
                        writer = csv.DictWriter(
                            handle,
                            fieldnames=["id", "status", "error_code", "created_at", "updated_at"],
                        )
                        writer.writeheader()
                        for row in status_rows:
                            writer.writerow(
                                {
                                    "id": row.get("id"),
                                    "status": row.get("status"),
                                    "error_code": row.get("error_code"),
                                    "created_at": row.get("created_at"),
                                    "updated_at": row.get("updated_at"),
                                }
                            )
                return

            if elapsed >= args.timeout:
                raise LoadTestError(f"Timed out after {args.timeout}s waiting for runs to finish")

            await asyncio.sleep(args.poll_interval)


if __name__ == "__main__":
    asyncio.run(main())
