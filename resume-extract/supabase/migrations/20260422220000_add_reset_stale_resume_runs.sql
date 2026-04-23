create or replace function public.reset_stale_resume_runs(
  p_stale_seconds integer default 120,
  p_limit integer default 100
)
returns setof public.resume_runs
language plpgsql
security definer
set search_path = public
as $$
begin
  return query
  with stale_runs as (
    select rr.id
    from public.resume_runs rr
    where rr.status = 'extracting'
      and coalesce(rr.extraction_heartbeat_at, rr.extraction_claimed_at, rr.updated_at, rr.created_at)
        <= now() - make_interval(secs => greatest(p_stale_seconds, 1))
    order by rr.created_at asc, rr.id asc
    for update skip locked
    limit greatest(p_limit, 1)
  )
  update public.resume_runs rr
  set
    status = 'queued',
    extraction_claimed_by = null,
    extraction_claimed_at = null,
    extraction_heartbeat_at = null,
    extraction_next_retry_at = now()
  from stale_runs
  where rr.id = stale_runs.id
  returning rr.*;
end;
$$;

revoke all on function public.reset_stale_resume_runs(integer, integer) from public;
grant execute on function public.reset_stale_resume_runs(integer, integer) to service_role;
