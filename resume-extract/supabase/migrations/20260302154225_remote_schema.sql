set check_function_bodies = off;

CREATE OR REPLACE FUNCTION public.match_embeddings(query_embedding text, match_count integer DEFAULT 5, role_slug_filter text DEFAULT NULL::text)
 RETURNS TABLE(id bigint, document_id uuid, chunk_index integer, chunk_text text, similarity double precision, metadata jsonb)
 LANGUAGE sql
 STABLE
AS $function$
  select
    e.id,
    e.document_id,
    e.chunk_index,
    e.chunk_text,
    1 - (e.embedding <=> (query_embedding::vector)) as similarity,
    e.metadata
  from public.embeddings e
  join public.documents d on d.id = e.document_id
  where role_slug_filter is null or d.role_slug = role_slug_filter
  order by e.embedding <=> (query_embedding::vector)
  limit greatest(match_count, 1);
$function$
;

CREATE OR REPLACE FUNCTION public.match_embeddings(query_embedding text, role_slug_filter text, match_count integer DEFAULT 5, source_type_filter text DEFAULT NULL::text, document_type_filter text DEFAULT NULL::text)
 RETURNS TABLE(id bigint, document_id uuid, chunk_index integer, chunk_text text, similarity double precision, metadata jsonb)
 LANGUAGE sql
 STABLE
AS $function$
  select
    e.id,
    e.document_id,
    e.chunk_index,
    e.chunk_text,
    1 - (e.embedding <=> (query_embedding::vector)) as similarity,
    e.metadata
  from public.embeddings e
  join public.documents d on d.id = e.document_id
  where d.role_slug = role_slug_filter
    and (
      source_type_filter is null
      or d.metadata ->> 'source_type' = source_type_filter
    )
    and (
      document_type_filter is null
      or d.metadata ->> 'document_type' = document_type_filter
    )
  order by e.embedding <=> (query_embedding::vector)
  limit greatest(match_count, 1);
$function$
;

CREATE OR REPLACE FUNCTION public.match_embeddings(query_embedding text, role_slug_filter text, user_id_filter text, match_count integer DEFAULT 5, source_type_filter text DEFAULT NULL::text, document_type_filter text DEFAULT NULL::text)
 RETURNS TABLE(id bigint, document_id uuid, chunk_index integer, chunk_text text, similarity double precision, metadata jsonb)
 LANGUAGE sql
 STABLE
AS $function$
  select
    e.id,
    e.document_id,
    e.chunk_index,
    e.chunk_text,
    1 - (e.embedding <=> (query_embedding::vector)) as similarity,
    e.metadata
  from public.embeddings e
  join public.documents d on d.id = e.document_id
  where d.role_slug = role_slug_filter
    and d.metadata ->> 'user_id' = user_id_filter
    and (
      source_type_filter is null
      or d.metadata ->> 'source_type' = source_type_filter
    )
    and (
      document_type_filter is null
      or d.metadata ->> 'document_type' = document_type_filter
    )
  order by e.embedding <=> (query_embedding::vector)
  limit greatest(match_count, 1);
$function$
;


