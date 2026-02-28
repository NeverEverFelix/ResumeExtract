# Resume Extraction Service (FastAPI Minimal Bootstrap)

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

For OCR on scanned PDFs, install system tools:
- `tesseract` (OCR engine)
- `poppler` (`pdf2image` backend)

macOS example:

```bash
brew install tesseract poppler
```

## Run

```bash
uvicorn src.main:app --host 0.0.0.0 --port 3000 --reload
```

## Test

```bash
python -m unittest discover -s tests -p "test_*.py" -v
```

Required environment variables:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `SUPABASE_STORAGE_BUCKET`
- `EXTRACT_AUTH_TOKEN` (required shared secret for `POST /extract`)
- `SUPABASE_DB_SCHEMA` (optional, default `public`)
- `MAX_RESUME_FILE_SIZE_MB` (optional, default `15`)
- `EXTRACT_DOWNLOAD_TIMEOUT_SECONDS` (optional, default `60`)

`.env` is auto-loaded via `python-dotenv`.

## Endpoints

### `GET /health`
Returns:

```json
{ "ok": true }
```

### `POST /extract`
Request body:

```json
{ "run_id": "<uuid>" }
```

Required header:

```http
X-Extract-Token: <EXTRACT_AUTH_TOKEN>
```

Behavior (status contract):
1. Claims run atomically (`queued -> extracting`) using a conditional update.
2. Downloads resume from Supabase Storage with safety guards (timeout, content-type validation, max file size) and extracts clean text (`.pdf` / `.docx`), with OCR fallback for scanned PDFs.
3. Persists one `resume_documents` row for the run (`run_id`-keyed upsert for idempotent retries).
4. Finalizes run to `extracted`.
5. On failure after claim, marks run `failed` with `error_code` and `error_message`.

Retry semantics:
- Service claims only `queued` runs.
- DB trigger enforces all new `resume_runs` inserts to start as `queued`.
- If a run is explicitly reset to `queued` and retried, `resume_documents` write is idempotent by `run_id` (upsert), avoiding unique-key conflicts from partial prior attempts.
- Re-calling `/extract` without reset still returns `run_not_queued`.

Operational logging:
- Structured JSON logs are emitted for extract phases and include `run_id`, `phase`, `parser`, `duration_ms`, and `error_code` when applicable.

Standardized `error_code` values:
- `missing_config`
- `unauthorized`
- `db_error`
- `run_not_queued`
- `run_not_extracting`
- `invalid_run_row`
- `download_failed`
- `file_too_large`
- `unsupported_content_type`
- `empty_resume_file`
- `unsupported_file_type`
- `empty_extracted_text`
- `parse_error`
- `ocr_unavailable`
- `ocr_dependency_missing`

Normalization for RAG quality:
- Bullet styles are normalized to `- ...`
- Page markers / footer-like lines (`Page X of Y`, `1/3`, etc.) are removed
- Weird whitespace (`NBSP`, zero-width chars, tabs) is normalized
- Consecutive duplicate lines are removed
- For PDF extraction, repeated short boilerplate lines across pages are collapsed

Success response:

```json
{ "ok": true, "run_id": "<uuid>", "status": "extracted" }
```
