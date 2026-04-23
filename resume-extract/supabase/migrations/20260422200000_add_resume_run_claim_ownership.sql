alter table public.resume_runs
  add column if not exists extraction_claimed_by text;

create index if not exists resume_runs_status_claimed_by_idx
  on public.resume_runs (status, extraction_claimed_by);

create or replace function public.claim_next_resume_run(
  p_claimed_by text
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
    where rr.status = 'queued'
    order by rr.created_at asc, rr.id asc
    for update skip locked
    limit 1
  )
  update public.resume_runs rr
  set
    status = 'extracting',
    error_code = null,
    error_message = null,
    extraction_claimed_by = p_claimed_by
  from next_run
  where rr.id = next_run.id
  returning rr.*;
end;
$$;

revoke all on function public.claim_next_resume_run(text) from public;
grant execute on function public.claim_next_resume_run(text) to service_role;
