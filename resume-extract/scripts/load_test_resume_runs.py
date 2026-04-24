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
from dotenv import load_dotenv


load_dotenv(override=True)


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_DB_SCHEMA = os.getenv("SUPABASE_DB_SCHEMA", "public")


class LoadTestError(Exception):
    pass


def _is_supabase_opaque_key(value: str) -> bool:
    return value.startswith("sb_secret_") or value.startswith("sb_publishable_")


def _headers(*, prefer: str | None = None) -> dict[str, str]:
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise LoadTestError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set")
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Content-Type": "application/json",
        "Accept-Profile": SUPABASE_DB_SCHEMA,
        "Content-Profile": SUPABASE_DB_SCHEMA,
    }
    if not _is_supabase_opaque_key(SUPABASE_SERVICE_ROLE_KEY):
        headers["Authorization"] = f"Bearer {SUPABASE_SERVICE_ROLE_KEY}"
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
        if response.status_code in {401, 403}:
            raise LoadTestError(
                f"{method} {path} failed: {response.status_code} auth rejected by Supabase. "
                "Verify SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY, and make sure the service-role key "
                "has not been revoked or truncated."
            )
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
        path=(
            "resume_runs"
            f"?id=in.({ids})"
            "&select=id,status,error_code,created_at,updated_at,"
            "extraction_claimed_by,extraction_claimed_at,extraction_heartbeat_at,extraction_attempt_count,"
            "generation_claimed_by,generation_claimed_at,generation_heartbeat_at,generation_attempt_count,"
            "pdf_claimed_by,pdf_claimed_at,pdf_heartbeat_at,pdf_attempt_count"
        ),
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


def _claimed_by_counts(rows: list[dict[str, Any]], field_name: str) -> dict[str, int]:
    counts = Counter()
    for row in rows:
        claimed_by = row.get(field_name)
        if isinstance(claimed_by, str) and claimed_by:
            counts[claimed_by] += 1
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
    peak_extracting = 0
    peak_extracting_at = 0.0
    peak_generating = 0
    peak_generating_at = 0.0
    peak_compiling_pdf = 0
    peak_compiling_pdf_at = 0.0
    extraction_claimed_by_seen: set[str] = set()
    generation_claimed_by_seen: set[str] = set()
    pdf_claimed_by_seen: set[str] = set()

    async with httpx.AsyncClient(timeout=30.0) as client:
        run_ids = await _insert_runs(client, rows)
        print(f"Inserted {len(run_ids)} queued runs")
        print(json.dumps({"run_ids": run_ids}, indent=2))

        while True:
            status_rows = await _fetch_run_statuses(client, run_ids)
            summary = _summarize(status_rows)
            extraction_claimed_by = _claimed_by_counts(status_rows, "extraction_claimed_by")
            generation_claimed_by = _claimed_by_counts(status_rows, "generation_claimed_by")
            pdf_claimed_by = _claimed_by_counts(status_rows, "pdf_claimed_by")
            extraction_claimed_by_seen.update(extraction_claimed_by)
            generation_claimed_by_seen.update(generation_claimed_by)
            pdf_claimed_by_seen.update(pdf_claimed_by)
            elapsed = time.monotonic() - started_at
            extracting_now = summary.get("extracting", 0)
            generating_now = summary.get("generating", 0)
            compiling_pdf_now = summary.get("compiling_pdf", 0)
            if extracting_now > peak_extracting:
                peak_extracting = extracting_now
                peak_extracting_at = elapsed
            if generating_now > peak_generating:
                peak_generating = generating_now
                peak_generating_at = elapsed
            if compiling_pdf_now > peak_compiling_pdf:
                peak_compiling_pdf = compiling_pdf_now
                peak_compiling_pdf_at = elapsed
            print(
                json.dumps(
                    {
                        "elapsed_seconds": round(elapsed, 1),
                        "status_counts": summary,
                        "extraction_claimed_by_counts": extraction_claimed_by,
                        "generation_claimed_by_counts": generation_claimed_by,
                        "pdf_claimed_by_counts": pdf_claimed_by,
                        "peak_extracting_so_far": peak_extracting,
                        "peak_generating_so_far": peak_generating,
                        "peak_compiling_pdf_so_far": peak_compiling_pdf,
                    },
                    sort_keys=True,
                )
            )

            terminal = summary.get("completed", 0) + summary.get("failed", 0)
            if terminal >= len(run_ids):
                final = {
                    "run_ids": run_ids,
                    "elapsed_seconds": round(elapsed, 3),
                    "status_counts": summary,
                    "extraction_claimed_by_counts": extraction_claimed_by,
                    "generation_claimed_by_counts": generation_claimed_by,
                    "pdf_claimed_by_counts": pdf_claimed_by,
                    "peak_extracting": peak_extracting,
                    "peak_extracting_at_seconds": round(peak_extracting_at, 3),
                    "peak_generating": peak_generating,
                    "peak_generating_at_seconds": round(peak_generating_at, 3),
                    "peak_compiling_pdf": peak_compiling_pdf,
                    "peak_compiling_pdf_at_seconds": round(peak_compiling_pdf_at, 3),
                    "distinct_extraction_claimed_by": sorted(extraction_claimed_by_seen),
                    "distinct_generation_claimed_by": sorted(generation_claimed_by_seen),
                    "distinct_pdf_claimed_by": sorted(pdf_claimed_by_seen),
                    "rows": status_rows,
                }
                if args.summary_json:
                    with open(args.summary_json, "w", encoding="utf-8") as handle:
                        json.dump(final, handle, indent=2)
                if args.summary_csv:
                    with open(args.summary_csv, "w", encoding="utf-8", newline="") as handle:
                        writer = csv.DictWriter(
                            handle,
                            fieldnames=[
                                "id",
                                "status",
                                "error_code",
                                "created_at",
                                "updated_at",
                                "extraction_claimed_by",
                                "extraction_claimed_at",
                                "extraction_heartbeat_at",
                                "extraction_attempt_count",
                                "generation_claimed_by",
                                "generation_claimed_at",
                                "generation_heartbeat_at",
                                "generation_attempt_count",
                                "pdf_claimed_by",
                                "pdf_claimed_at",
                                "pdf_heartbeat_at",
                                "pdf_attempt_count",
                            ],
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
                                    "extraction_claimed_by": row.get("extraction_claimed_by"),
                                    "extraction_claimed_at": row.get("extraction_claimed_at"),
                                    "extraction_heartbeat_at": row.get("extraction_heartbeat_at"),
                                    "extraction_attempt_count": row.get("extraction_attempt_count"),
                                    "generation_claimed_by": row.get("generation_claimed_by"),
                                    "generation_claimed_at": row.get("generation_claimed_at"),
                                    "generation_heartbeat_at": row.get("generation_heartbeat_at"),
                                    "generation_attempt_count": row.get("generation_attempt_count"),
                                    "pdf_claimed_by": row.get("pdf_claimed_by"),
                                    "pdf_claimed_at": row.get("pdf_claimed_at"),
                                    "pdf_heartbeat_at": row.get("pdf_heartbeat_at"),
                                    "pdf_attempt_count": row.get("pdf_attempt_count"),
                                }
                            )
                return

            if elapsed >= args.timeout:
                raise LoadTestError(f"Timed out after {args.timeout}s waiting for runs to finish")

            await asyncio.sleep(args.poll_interval)


if __name__ == "__main__":
    asyncio.run(main())
