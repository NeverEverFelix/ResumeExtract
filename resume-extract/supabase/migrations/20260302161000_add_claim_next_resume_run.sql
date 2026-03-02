-- Atomic, race-safe claim primitive for extraction workers.
-- Claims at most one queued run in deterministic order.
create or replace function public.claim_next_resume_run()
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
    where rr.status = 'queued'
    order by rr.created_at asc, rr.id asc
    for update skip locked
    limit 1
  )
  update public.resume_runs rr
  set
    status = 'extracting',
    error_code = null,
    error_message = null
  from next_run
  where rr.id = next_run.id
  returning rr.*;
end;
$$;

revoke all on function public.claim_next_resume_run() from public;
grant execute on function public.claim_next_resume_run() to service_role;
