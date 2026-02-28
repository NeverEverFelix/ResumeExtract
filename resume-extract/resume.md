# Resume Extraction – Run Status Transitions

## Purpose
This document defines the **authoritative run status lifecycle** for the resume extraction service.  
No service code should be written until this contract is understood and followed exactly.

---

## Canonical Run States (Extraction Phase)

No schema changes are required.

- **queued**
  - Run has been created.
  - Resume file exists in storage.
  - Waiting for extraction to begin.

- **extracting**
  - Extraction service has successfully claimed the run.
  - No other worker/service may process this run.

- **extracted**
  - Extraction completed successfully.
  - A corresponding row exists in `resume_documents`.
  - Resume text is now the durable source of truth.

- **failed**
  - Terminal failure during extraction.
  - `error_code` and `error_message` must be populated.
  - Run may be retried only via explicit reset logic.

---

## Required Extraction Service Behavior

The extraction service **must** follow this sequence:

1. **Eligibility Check**
   - Only begin work if `resume_runs.status = 'queued'`.

2. **Claim the Run (Atomic)**
   - Immediately update:
     ```
     status = 'extracting'
     ```
   - This update must be atomic (e.g., `WHERE status = 'queued'`).
   - If no rows are affected, the run has already been claimed or processed.

3. **Perform Extraction**
   - Download resume from storage.
   - Extract clean text from PDF/DOCX.
   - Prepare metadata (parser, pages, warnings, etc.).

4. **Persist Extracted Text**
   - Insert exactly one row into `resume_documents`.
   - `run_id` must be unique (1:1 with `resume_runs`).

5. **Finalize**
   - Update run status to:
     ```
     status = 'extracted'
     ```
   - This step must only occur **after** `resume_documents` insertion succeeds.

6. **Failure Handling**
   - On any exception:
     ```
     status = 'failed'
     error_code = <machine-readable code>
     error_message = <human-readable message>
     ```

---

## Invariant (Non-Negotiable)

> **All downstream processing (LLM, RAG, scoring) must read resume text exclusively from `resume_documents.text`.**

No service beyond extraction is allowed to read directly from Supabase Storage.

---

## Notes
- Idempotency is achieved via status checks.
- Duplicate extraction work must be avoided.
- This contract is foundational for RAG correctness and cost control.