create table if not exists public.extract_worker_heartbeats (
  worker_id text primary key,
  hostname text not null,
  pid integer not null,
  role text not null default 'extractor',
  started_at timestamp with time zone not null default now(),
  last_seen_at timestamp with time zone not null default now()
);

create index if not exists extract_worker_heartbeats_last_seen_idx
  on public.extract_worker_heartbeats (last_seen_at desc);

grant all on table public.extract_worker_heartbeats to service_role;
