-- Expand resume_runs.status to support downstream queued pipeline stages.

do $$
begin
  if to_regclass('public.resume_runs') is null then
    return;
  end if;

  alter table public.resume_runs
    drop constraint if exists resume_runs_status_valid;

  alter table public.resume_runs
    drop constraint if exists resume_runs_status_check;

  alter table public.resume_runs
    add constraint resume_runs_status_valid
    check (
      status in (
        'queued',
        'extracting',
        'extracted',
        'queued_generate',
        'generating',
        'queued_pdf',
        'compiling_pdf',
        'completed',
        'failed'
      )
    );
end $$;
