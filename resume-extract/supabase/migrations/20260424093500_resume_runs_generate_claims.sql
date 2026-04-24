alter table public.resume_runs
  add column if not exists generation_attempt_count integer not null default 0;

alter table public.resume_runs
  add column if not exists generation_claimed_at timestamp with time zone;

alter table public.resume_runs
  add column if not exists generation_claimed_by text;

alter table public.resume_runs
  add column if not exists generation_heartbeat_at timestamp with time zone;

alter table public.resume_runs
  add column if not exists generation_next_retry_at timestamp with time zone;

create index if not exists resume_runs_generation_claimed_by_idx
  on public.resume_runs (status, generation_claimed_by);

create index if not exists resume_runs_generation_heartbeat_idx
  on public.resume_runs (status, generation_heartbeat_at);

create index if not exists resume_runs_generation_next_retry_idx
  on public.resume_runs (status, generation_next_retry_at, created_at, id);

create or replace function public.claim_next_generate_run(
  p_claimed_by text,
  p_lease_seconds integer default 300
)
returns setof public.resume_runs
language plpgsql
security definer
set search_path to 'public'
as $function$
begin
  return query
  with next_run as (
    select rr.id
    from public.resume_runs rr
    where (
      rr.status = 'queued_generate'
      and coalesce(rr.generation_next_retry_at, rr.updated_at, rr.created_at) <= now()
    ) or (
      rr.status = 'generating'
      and coalesce(rr.generation_heartbeat_at, rr.generation_claimed_at, rr.updated_at, rr.created_at)
        <= now() - make_interval(secs => greatest(p_lease_seconds, 1))
    )
    order by coalesce(rr.generation_next_retry_at, rr.updated_at, rr.created_at) asc, rr.created_at asc, rr.id asc
    for update skip locked
    limit 1
  )
  update public.resume_runs rr
  set
    status = 'generating',
    error_code = null,
    error_message = null,
    generation_claimed_by = p_claimed_by,
    generation_claimed_at = now(),
    generation_heartbeat_at = now(),
    generation_attempt_count = coalesce(rr.generation_attempt_count, 0) + 1,
    generation_next_retry_at = null
  from next_run
  where rr.id = next_run.id
  returning rr.*;
end;
$function$;

create or replace function public.reset_stale_generate_runs(
  p_stale_seconds integer default 300,
  p_limit integer default 100
)
returns setof public.resume_runs
language plpgsql
security definer
set search_path to 'public'
as $function$
begin
  return query
  with stale_runs as (
    select rr.id
    from public.resume_runs rr
    where rr.status = 'generating'
      and coalesce(rr.generation_heartbeat_at, rr.generation_claimed_at, rr.updated_at, rr.created_at)
        <= now() - make_interval(secs => greatest(p_stale_seconds, 1))
    order by rr.created_at asc, rr.id asc
    for update skip locked
    limit greatest(p_limit, 1)
  )
  update public.resume_runs rr
  set
    status = 'queued_generate',
    generation_claimed_by = null,
    generation_claimed_at = null,
    generation_heartbeat_at = null,
    generation_next_retry_at = now()
  from stale_runs
  where rr.id = stale_runs.id
  returning rr.*;
end;
$function$;

revoke all on function public.claim_next_generate_run(text, integer) from public;
grant execute on function public.claim_next_generate_run(text, integer) to service_role;

revoke all on function public.reset_stale_generate_runs(integer, integer) from public;
grant execute on function public.reset_stale_generate_runs(integer, integer) to service_role;
