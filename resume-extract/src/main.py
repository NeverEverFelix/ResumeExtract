import json
import logging
import os
import re
import shutil
import time
from collections import Counter
from hmac import compare_digest
from io import BytesIO
from typing import Any
from urllib.parse import quote
from uuid import UUID

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Header
from fastapi.responses import JSONResponse
from pypdf import PdfReader
from pydantic import BaseModel
from docx import Document

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_DB_SCHEMA = os.getenv("SUPABASE_DB_SCHEMA", "public")
SUPABASE_STORAGE_BUCKET = os.getenv("SUPABASE_STORAGE_BUCKET")
EXTRACT_AUTH_TOKEN = os.getenv("EXTRACT_AUTH_TOKEN")
EXTRACT_DOWNLOAD_TIMEOUT_SECONDS = float(os.getenv("EXTRACT_DOWNLOAD_TIMEOUT_SECONDS", "60"))
MAX_RESUME_FILE_SIZE_MB = int(os.getenv("MAX_RESUME_FILE_SIZE_MB", "15"))

MAX_RESUME_FILE_SIZE_BYTES = MAX_RESUME_FILE_SIZE_MB * 1024 * 1024
ALLOWED_BINARY_CONTENT_TYPES = {"application/octet-stream", "binary/octet-stream"}
ALLOWED_PDF_CONTENT_TYPES = {
    "application/pdf",
    "application/x-pdf",
    *ALLOWED_BINARY_CONTENT_TYPES,
}
ALLOWED_DOCX_CONTENT_TYPES = {
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/zip",
    *ALLOWED_BINARY_CONTENT_TYPES,
}

app = FastAPI()
logger = logging.getLogger("resume_extract_service")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# Canonical error codes for extraction pipeline.
ERR_MISSING_CONFIG = "missing_config"
ERR_UNAUTHORIZED = "unauthorized"
ERR_DB_ERROR = "db_error"
ERR_RUN_NOT_QUEUED = "run_not_queued"
ERR_RUN_NOT_EXTRACTING = "run_not_extracting"
ERR_INVALID_RUN_ROW = "invalid_run_row"
ERR_DOWNLOAD_FAILED = "download_failed"
ERR_FILE_TOO_LARGE = "file_too_large"
ERR_UNSUPPORTED_CONTENT_TYPE = "unsupported_content_type"
ERR_EMPTY_RESUME_FILE = "empty_resume_file"
ERR_UNSUPPORTED_FILE_TYPE = "unsupported_file_type"
ERR_EMPTY_EXTRACTED_TEXT = "empty_extracted_text"
ERR_PARSE_ERROR = "parse_error"
ERR_OCR_UNAVAILABLE = "ocr_unavailable"
ERR_OCR_DEPENDENCY_MISSING = "ocr_dependency_missing"

BULLET_PREFIX_RE = re.compile(r"^[\-\*\u2022\u2023\u25E6\u2043\u2219\u00B7]+[\s\t]*")
PAGE_MARKER_RE = re.compile(
    r"^(page\s+\d+(\s+of\s+\d+)?|\d+\s*/\s*\d+|\d+\s+of\s+\d+)$",
    flags=re.IGNORECASE,
)
ZERO_WIDTH_CHARS_RE = re.compile(r"[\u200b\u200c\u200d\ufeff]")


class HttpError(Exception):
    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


class ExtractRequest(BaseModel):
    run_id: UUID


def _log_event(
    phase: str,
    run_id: UUID | None = None,
    parser: str | None = None,
    duration_ms: int | None = None,
    error_code: str | None = None,
    **extra: Any,
) -> None:
    payload: dict[str, Any] = {"event": "resume_extract", "phase": phase}
    if run_id is not None:
        payload["run_id"] = str(run_id)
    if parser:
        payload["parser"] = parser
    if duration_ms is not None:
        payload["duration_ms"] = duration_ms
    if error_code:
        payload["error_code"] = error_code
    payload.update(extra)
    logger.info(json.dumps(payload, separators=(",", ":"), default=str))


def _assert_extract_auth(token: str | None) -> None:
    if not EXTRACT_AUTH_TOKEN:
        raise HttpError(
            status=500,
            code=ERR_MISSING_CONFIG,
            message="EXTRACT_AUTH_TOKEN must be configured",
        )
    if not token or not compare_digest(token, EXTRACT_AUTH_TOKEN):
        raise HttpError(
            status=401,
            code=ERR_UNAUTHORIZED,
            message="Invalid extract auth token",
        )


def _assert_config() -> None:
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY or not SUPABASE_STORAGE_BUCKET:
        raise HttpError(
            status=500,
            code=ERR_MISSING_CONFIG,
            message="SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, and SUPABASE_STORAGE_BUCKET must be configured",
        )


def _validate_content_type(resume_path: str, content_type_header: str | None) -> None:
    if not content_type_header:
        return

    content_type = content_type_header.split(";", 1)[0].strip().lower()
    lower_path = resume_path.lower()

    if lower_path.endswith(".pdf"):
        allowed = ALLOWED_PDF_CONTENT_TYPES
    elif lower_path.endswith(".docx"):
        allowed = ALLOWED_DOCX_CONTENT_TYPES
    else:
        return

    if content_type not in allowed:
        raise HttpError(
            status=415,
            code=ERR_UNSUPPORTED_CONTENT_TYPE,
            message=f"Unexpected content-type '{content_type}' for file '{resume_path}'",
        )


def _validate_file_size(content_length_header: str | None, file_bytes_len: int) -> None:
    if content_length_header:
        try:
            content_length = int(content_length_header)
            if content_length > MAX_RESUME_FILE_SIZE_BYTES:
                raise HttpError(
                    status=413,
                    code=ERR_FILE_TOO_LARGE,
                    message=f"Resume file exceeds max size of {MAX_RESUME_FILE_SIZE_MB}MB",
                )
        except ValueError:
            pass

    if file_bytes_len > MAX_RESUME_FILE_SIZE_BYTES:
        raise HttpError(
            status=413,
            code=ERR_FILE_TOO_LARGE,
            message=f"Resume file exceeds max size of {MAX_RESUME_FILE_SIZE_MB}MB",
        )


async def _supabase_fetch(path: str, method: str, body: dict[str, Any] | None = None, prefer: str | None = None) -> Any:
    _assert_config()
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Accept-Profile": SUPABASE_DB_SCHEMA,
        "Content-Profile": SUPABASE_DB_SCHEMA,
    }
    if prefer:
        headers["Prefer"] = prefer

    url = f"{SUPABASE_URL}/rest/v1/{path}"
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.request(method=method, url=url, headers=headers, json=body)

    if response.status_code >= 400:
        raise HttpError(
            status=502,
            code=ERR_DB_ERROR,
            message=f"Supabase error: {response.text}",
        )

    if "application/json" in response.headers.get("content-type", ""):
        return response.json()
    return None


async def _claim_run(run_id: UUID) -> dict[str, Any]:
    path = f"resume_runs?id=eq.{run_id}&status=eq.queued&select=id,status,user_id,resume_path"
    rows = await _supabase_fetch(
        path=path,
        method="PATCH",
        body={
            "status": "extracting",
            "error_code": None,
            "error_message": None,
        },
        prefer="return=representation",
    )
    if not isinstance(rows, list) or len(rows) == 0:
        raise HttpError(
            status=409,
            code=ERR_RUN_NOT_QUEUED,
            message="Run is not in queued state or does not exist",
        )
    return rows[0]


async def _insert_resume_document(
    run_id: UUID,
    run_user_id: str,
    run_resume_path: str,
    extracted_text: str,
    metadata: dict[str, Any],
) -> None:
    # Idempotent write for retry flows: if a row already exists for run_id,
    # overwrite it for the same run instead of failing on unique constraint.
    await _supabase_fetch(
        path="resume_documents?on_conflict=run_id",
        method="POST",
        body={
            "run_id": str(run_id),
            "user_id": run_user_id,
            "resume_path": run_resume_path,
            "text": extracted_text,
            "text_source": "extract_service",
            "metadata": metadata,
        },
        prefer="resolution=merge-duplicates,return=minimal",
    )


async def _set_run_extracted(run_id: UUID) -> None:
    path = f"resume_runs?id=eq.{run_id}&status=eq.extracting"
    rows = await _supabase_fetch(
        path=path,
        method="PATCH",
        body={
            "status": "extracted",
            "error_code": None,
            "error_message": None,
        },
        prefer="return=representation",
    )
    if not isinstance(rows, list) or len(rows) == 0:
        raise HttpError(
            status=409,
            code=ERR_RUN_NOT_EXTRACTING,
            message="Run is not in extracting state during finalize",
        )


async def _set_run_failed(run_id: UUID, code: str, message: str) -> None:
    path = f"resume_runs?id=eq.{run_id}&status=eq.extracting"
    rows = await _supabase_fetch(
        path=path,
        method="PATCH",
        body={
            "status": "failed",
            "error_code": code,
            "error_message": message[:400],
        },
        prefer="return=representation",
    )
    if not isinstance(rows, list) or len(rows) == 0:
        raise HttpError(
            status=409,
            code=ERR_RUN_NOT_EXTRACTING,
            message="Run is not in extracting state during failure finalize",
        )


def _normalize_line(line: str) -> str:
    line = line.replace("\x00", " ").replace("\xa0", " ")
    line = ZERO_WIDTH_CHARS_RE.sub("", line)
    line = line.replace("\t", " ")
    line = re.sub(r"\s+", " ", line).strip()
    if not line:
        return ""
    if PAGE_MARKER_RE.match(line):
        return ""

    # Normalize bullet styles into a single durable format for chunking.
    bullet_removed = BULLET_PREFIX_RE.sub("", line).strip()
    if bullet_removed and bullet_removed != line:
        return f"- {bullet_removed}"
    return line


def _normalize_text(raw_text: str, remove_repeated_boilerplate: bool = False) -> str:
    lines = [_normalize_line(line) for line in raw_text.splitlines()]
    lines = [line for line in lines if line]

    if remove_repeated_boilerplate and lines:
        lowered = [line.casefold() for line in lines]
        freq = Counter(lowered)
        kept_boilerplate: set[str] = set()
        filtered: list[str] = []
        for line in lines:
            key = line.casefold()
            is_repeated_boilerplate = (
                freq[key] >= 3
                and len(line) <= 120
                and len(line.split()) <= 12
            )
            if is_repeated_boilerplate:
                if key in kept_boilerplate:
                    continue
                kept_boilerplate.add(key)
            filtered.append(line)
        lines = filtered

    # Remove consecutive duplicate lines to reduce OCR and parser noise.
    deduped: list[str] = []
    for line in lines:
        if deduped and deduped[-1].casefold() == line.casefold():
            continue
        deduped.append(line)

    return "\n".join(deduped).strip()


async def _download_resume_bytes(resume_path: str) -> bytes:
    _assert_config()
    encoded_path = quote(resume_path, safe="/")
    url = f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_STORAGE_BUCKET}/{encoded_path}"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    }
    try:
        async with httpx.AsyncClient(timeout=EXTRACT_DOWNLOAD_TIMEOUT_SECONDS) as client:
            response = await client.get(url, headers=headers)
    except httpx.TimeoutException as exc:
        raise HttpError(
            status=504,
            code=ERR_DOWNLOAD_FAILED,
            message="Timed out while downloading resume file from storage",
        ) from exc
    except httpx.HTTPError as exc:
        raise HttpError(
            status=502,
            code=ERR_DOWNLOAD_FAILED,
            message=f"Network error while downloading resume file: {exc}",
        ) from exc

    if response.status_code >= 400:
        raise HttpError(
            status=502,
            code=ERR_DOWNLOAD_FAILED,
            message=f"Failed to download resume file from storage: {response.text}",
        )

    _validate_content_type(resume_path, response.headers.get("content-type"))
    _validate_file_size(response.headers.get("content-length"), len(response.content))

    if not response.content:
        raise HttpError(
            status=422,
            code=ERR_EMPTY_RESUME_FILE,
            message="Resume file is empty",
        )
    return response.content


def _extract_pdf(file_bytes: bytes) -> tuple[str, dict[str, Any]]:
    try:
        reader = PdfReader(BytesIO(file_bytes))
    except Exception as exc:
        raise HttpError(
            status=422,
            code=ERR_PARSE_ERROR,
            message=f"Failed to parse PDF: {exc}",
        ) from exc
    pages_text: list[str] = []
    for page in reader.pages:
        try:
            pages_text.append(page.extract_text() or "")
        except Exception:
            pages_text.append("")
    merged = "\n".join(pages_text)
    cleaned = _normalize_text(merged, remove_repeated_boilerplate=True)
    if cleaned:
        return cleaned, {"parser": "pypdf", "pages": len(reader.pages), "ocr_used": False}

    return _extract_pdf_with_ocr(file_bytes, len(reader.pages))


def _extract_pdf_with_ocr(file_bytes: bytes, page_count: int) -> tuple[str, dict[str, Any]]:
    if shutil.which("tesseract") is None:
        raise HttpError(
            status=422,
            code=ERR_OCR_UNAVAILABLE,
            message="No extractable text found in PDF and OCR is unavailable (missing tesseract binary)",
        )
    try:
        # Lazy imports so normal text-PDF flow does not require OCR packages at import time.
        from pdf2image import convert_from_bytes
        import pytesseract
    except ImportError as exc:
        raise HttpError(
            status=500,
            code=ERR_OCR_DEPENDENCY_MISSING,
            message="OCR dependencies are not installed (pdf2image and pytesseract)",
        ) from exc

    try:
        images = convert_from_bytes(file_bytes, fmt="png")
    except Exception as exc:
        raise HttpError(
            status=422,
            code=ERR_PARSE_ERROR,
            message=f"Failed to render PDF pages for OCR: {exc}",
        ) from exc

    ocr_pages: list[str] = []
    for image in images:
        try:
            ocr_pages.append(pytesseract.image_to_string(image))
        except Exception:
            ocr_pages.append("")

    cleaned = _normalize_text("\n".join(ocr_pages), remove_repeated_boilerplate=True)
    if not cleaned:
        raise HttpError(
            status=422,
            code=ERR_EMPTY_EXTRACTED_TEXT,
            message="No extractable text found in PDF after OCR",
        )
    return cleaned, {"parser": "pypdf+tesseract", "pages": page_count, "ocr_used": True}


def _extract_docx(file_bytes: bytes) -> tuple[str, dict[str, Any]]:
    try:
        document = Document(BytesIO(file_bytes))
    except Exception as exc:
        raise HttpError(
            status=422,
            code=ERR_PARSE_ERROR,
            message=f"Failed to parse DOCX: {exc}",
        ) from exc
    chunks: list[str] = []

    for paragraph in document.paragraphs:
        if paragraph.text and paragraph.text.strip():
            chunks.append(paragraph.text)

    for table in document.tables:
        for row in table.rows:
            cells = [cell.text.strip() for cell in row.cells if cell.text and cell.text.strip()]
            if cells:
                chunks.append(" | ".join(cells))

    cleaned = _normalize_text("\n".join(chunks))
    if not cleaned:
        raise HttpError(
            status=422,
            code=ERR_EMPTY_EXTRACTED_TEXT,
            message="No extractable text found in DOCX",
        )
    return cleaned, {"parser": "python-docx", "paragraph_count": len(document.paragraphs)}


async def _perform_extraction(resume_path: str) -> tuple[str, dict[str, Any]]:
    file_bytes = await _download_resume_bytes(resume_path)
    lower_path = resume_path.lower()

    if lower_path.endswith(".pdf"):
        return _extract_pdf(file_bytes)
    if lower_path.endswith(".docx"):
        return _extract_docx(file_bytes)

    raise HttpError(
        status=422,
        code=ERR_UNSUPPORTED_FILE_TYPE,
        message="Only PDF and DOCX files are supported",
    )


@app.exception_handler(HttpError)
async def http_error_handler(_, exc: HttpError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status,
        content={
            "ok": False,
            "error_code": exc.code,
            "error_message": exc.message,
        },
    )


@app.get("/health")
async def health() -> dict[str, bool]:
    return {"ok": True}


@app.post("/extract")
async def extract(
    payload: ExtractRequest,
    x_extract_token: str | None = Header(default=None, alias="X-Extract-Token"),
) -> dict[str, Any]:
    _assert_extract_auth(x_extract_token)
    run_id = payload.run_id
    parser_name: str | None = None
    request_start = time.perf_counter()
    _log_event(phase="extract_start", run_id=run_id)

    claim_start = time.perf_counter()
    run_row = await _claim_run(run_id)
    _log_event(
        phase="claim_complete",
        run_id=run_id,
        duration_ms=int((time.perf_counter() - claim_start) * 1000),
    )

    run_user_id = run_row.get("user_id")
    run_resume_path = run_row.get("resume_path")
    if not isinstance(run_user_id, str) or not isinstance(run_resume_path, str):
        raise HttpError(
            status=500,
            code=ERR_INVALID_RUN_ROW,
            message="Claimed run is missing user_id or resume_path",
        )

    try:
        extraction_start = time.perf_counter()
        extracted_text, metadata = await _perform_extraction(run_resume_path)
        parser_name = str(metadata.get("parser", "unknown"))
        _log_event(
            phase="extraction_complete",
            run_id=run_id,
            parser=parser_name,
            duration_ms=int((time.perf_counter() - extraction_start) * 1000),
            chars=len(extracted_text),
        )

        persist_start = time.perf_counter()
        await _insert_resume_document(run_id, run_user_id, run_resume_path, extracted_text, metadata)
        _log_event(
            phase="persist_complete",
            run_id=run_id,
            parser=parser_name,
            duration_ms=int((time.perf_counter() - persist_start) * 1000),
        )

        finalize_start = time.perf_counter()
        await _set_run_extracted(run_id)
        _log_event(
            phase="finalize_complete",
            run_id=run_id,
            parser=parser_name,
            duration_ms=int((time.perf_counter() - finalize_start) * 1000),
        )
        _log_event(
            phase="extract_success",
            run_id=run_id,
            parser=parser_name,
            duration_ms=int((time.perf_counter() - request_start) * 1000),
        )
        return {"ok": True, "run_id": str(run_id), "status": "extracted"}
    except Exception as error:  # pragma: no cover
        error_code = error.code if isinstance(error, HttpError) else ERR_PARSE_ERROR
        error_message = str(error) if str(error) else "Unknown extraction failure"
        _log_event(
            phase="extract_error",
            run_id=run_id,
            parser=parser_name,
            duration_ms=int((time.perf_counter() - request_start) * 1000),
            error_code=error_code,
            error_message=error_message,
        )
        try:
            failed_start = time.perf_counter()
            await _set_run_failed(run_id, error_code, error_message)
            _log_event(
                phase="failed_status_set",
                run_id=run_id,
                parser=parser_name,
                duration_ms=int((time.perf_counter() - failed_start) * 1000),
                error_code=error_code,
            )
        except Exception:
            _log_event(
                phase="failed_status_set_error",
                run_id=run_id,
                parser=parser_name,
                error_code=error_code,
            )
            pass
        if isinstance(error, HttpError):
            raise error
        raise HttpError(status=500, code=error_code, message=error_message)
