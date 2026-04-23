alter table public.resume_runs
  add column if not exists extraction_attempt_count integer not null default 0,
  add column if not exists extraction_next_retry_at timestamp with time zone;

create index if not exists resume_runs_status_next_retry_idx
  on public.resume_runs (status, extraction_next_retry_at, created_at, id);

create or replace function public.claim_resume_run(
  p_run_id uuid,
  p_claimed_by text
)
returns setof public.resume_runs
language plpgsql
security definer
set search_path = public
as $$
begin
  return query
  update public.resume_runs rr
  set
    status = 'extracting',
    error_code = null,
    error_message = null,
    extraction_claimed_by = p_claimed_by,
    extraction_claimed_at = now(),
    extraction_heartbeat_at = now(),
    extraction_attempt_count = coalesce(rr.extraction_attempt_count, 0) + 1,
    extraction_next_retry_at = null
  where rr.id = p_run_id
    and rr.status = 'queued'
    and coalesce(rr.extraction_next_retry_at, rr.created_at) <= now()
  returning rr.*;
end;
$$;

revoke all on function public.claim_resume_run(uuid, text) from public;
grant execute on function public.claim_resume_run(uuid, text) to service_role;

create or replace function public.claim_next_resume_run(
  p_claimed_by text,
  p_lease_seconds integer default 120
)
returns setof public.resume_runs
language plpgsql
security definer
set search_path = public
as $$
begin
  return query
  with next_run as (
    select rr.id
    from public.resume_runs rr
    where (
      rr.status = 'queued'
      and coalesce(rr.extraction_next_retry_at, rr.created_at) <= now()
    ) or (
      rr.status = 'extracting'
      and coalesce(rr.extraction_heartbeat_at, rr.extraction_claimed_at, rr.updated_at, rr.created_at)
        <= now() - make_interval(secs => greatest(p_lease_seconds, 1))
    )
    order by coalesce(rr.extraction_next_retry_at, rr.created_at) asc, rr.created_at asc, rr.id asc
    for update skip locked
    limit 1
  )
  update public.resume_runs rr
  set
    status = 'extracting',
    error_code = null,
    error_message = null,
    extraction_claimed_by = p_claimed_by,
    extraction_claimed_at = now(),
    extraction_heartbeat_at = now(),
    extraction_attempt_count = coalesce(rr.extraction_attempt_count, 0) + 1,
    extraction_next_retry_at = null
  from next_run
  where rr.id = next_run.id
  returning rr.*;
end;
$$;

revoke all on function public.claim_next_resume_run(text, integer) from public;
grant execute on function public.claim_next_resume_run(text, integer) to service_role;
