alter table public.resume_runs
  add column if not exists extraction_claimed_at timestamp with time zone,
  add column if not exists extraction_heartbeat_at timestamp with time zone;

create index if not exists resume_runs_status_heartbeat_idx
  on public.resume_runs (status, extraction_heartbeat_at);

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
    ) or (
      rr.status = 'extracting'
      and coalesce(rr.extraction_heartbeat_at, rr.extraction_claimed_at, rr.updated_at, rr.created_at)
        <= now() - make_interval(secs => greatest(p_lease_seconds, 1))
    )
    order by rr.created_at asc, rr.id asc
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
    extraction_heartbeat_at = now()
  from next_run
  where rr.id = next_run.id
  returning rr.*;
end;
$$;

revoke all on function public.claim_next_resume_run(text, integer) from public;
grant execute on function public.claim_next_resume_run(text, integer) to service_role;
