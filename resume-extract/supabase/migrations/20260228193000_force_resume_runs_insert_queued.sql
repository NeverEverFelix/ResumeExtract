-- Enforce pipeline contract at DB boundary:
-- every new resume_runs row starts as queued.

create or replace function public.enforce_resume_runs_insert_queued()
returns trigger
language plpgsql
as $$
begin
  -- Extraction lifecycle must start from queued on insert.
  new.status := 'queued';
  new.error_code := null;
  new.error_message := null;
  return new;
end;
$$;

drop trigger if exists trg_enforce_resume_runs_insert_queued on public.resume_runs;

create trigger trg_enforce_resume_runs_insert_queued
before insert on public.resume_runs
for each row
execute function public.enforce_resume_runs_insert_queued();
