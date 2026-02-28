-- Align resume_runs status with extraction contract.
-- Canonical states: queued -> extracting -> extracted | failed

alter table public.resume_runs
  drop constraint if exists resume_runs_status_check;

alter table public.resume_runs
  drop constraint if exists resume_runs_status_valid;

-- Legacy data migration: map previous statuses into canonical lifecycle.
update public.resume_runs
set status = 'queued'
where status = 'processing';

update public.resume_runs
set status = 'extracted'
where status = 'success';

-- Any remaining legacy/unknown state is treated as terminal failure.
update public.resume_runs
set status = 'failed'
where status not in ('queued', 'extracting', 'extracted', 'failed');

alter table public.resume_runs
  add constraint resume_runs_status_check
  check (status = any (array['queued'::text, 'extracting'::text, 'extracted'::text, 'failed'::text]));
