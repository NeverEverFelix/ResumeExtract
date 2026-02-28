import asyncio
import unittest
from io import BytesIO
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from docx import Document
from fastapi.testclient import TestClient

import src.main as main


class MainTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(main.app)
        self.original_extract_auth_token = main.EXTRACT_AUTH_TOKEN
        main.EXTRACT_AUTH_TOKEN = "test-extract-token"

    def tearDown(self) -> None:
        main.EXTRACT_AUTH_TOKEN = self.original_extract_auth_token

    def test_health_returns_ok(self) -> None:
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"ok": True})

    def test_extract_success_contract_flow(self) -> None:
        run_id = uuid4()
        insert_mock = AsyncMock()
        set_extracted_mock = AsyncMock()

        with (
            patch.object(main, "_claim_run", AsyncMock(return_value={"id": str(run_id), "user_id": "user-123", "resume_path": "user-123/resume.pdf"})),
            patch.object(main, "_perform_extraction", AsyncMock(return_value=("Extracted text", {"parser": "fake"}))),
            patch.object(main, "_insert_resume_document", insert_mock),
            patch.object(main, "_set_run_extracted", set_extracted_mock),
        ):
            response = self.client.post(
                "/extract",
                json={"run_id": str(run_id)},
                headers={"X-Extract-Token": "test-extract-token"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"ok": True, "run_id": str(run_id), "status": "extracted"})
        insert_mock.assert_awaited_once_with(
            run_id, "user-123", "user-123/resume.pdf", "Extracted text", {"parser": "fake"}
        )
        set_extracted_mock.assert_awaited_once_with(run_id)

    def test_insert_resume_document_uses_upsert_for_retry_idempotency(self) -> None:
        with patch.object(main, "_supabase_fetch", AsyncMock()) as fetch_mock:
            asyncio.run(
                main._insert_resume_document(
                    run_id=uuid4(),
                    run_user_id="user-123",
                    run_resume_path="user-123/resume.pdf",
                    extracted_text="text",
                    metadata={"parser": "fake"},
                )
            )

        fetch_mock.assert_awaited_once()
        kwargs = fetch_mock.await_args.kwargs
        self.assertEqual(kwargs["path"], "resume_documents?on_conflict=run_id")
        self.assertEqual(kwargs["method"], "POST")
        self.assertEqual(kwargs["prefer"], "resolution=merge-duplicates,return=minimal")

    def test_extract_returns_409_when_not_queued(self) -> None:
        with patch.object(
            main,
            "_claim_run",
            AsyncMock(side_effect=main.HttpError(409, "run_not_queued", "Run is not in queued state or does not exist")),
        ):
            response = self.client.post(
                "/extract",
                json={"run_id": str(uuid4())},
                headers={"X-Extract-Token": "test-extract-token"},
            )

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json().get("error_code"), "run_not_queued")

    def test_extract_marks_failed_on_exception(self) -> None:
        run_id = uuid4()
        set_failed_mock = AsyncMock()

        with (
            patch.object(main, "_claim_run", AsyncMock(return_value={"id": str(run_id), "user_id": "user-123", "resume_path": "user-123/resume.pdf"})),
            patch.object(main, "_perform_extraction", AsyncMock(side_effect=ValueError("boom"))),
            patch.object(main, "_set_run_failed", set_failed_mock),
            patch.object(main, "_set_run_extracted", AsyncMock()),
        ):
            response = self.client.post(
                "/extract",
                json={"run_id": str(run_id)},
                headers={"X-Extract-Token": "test-extract-token"},
            )

        self.assertEqual(response.status_code, 500)
        payload = response.json()
        self.assertEqual(payload.get("error_code"), main.ERR_PARSE_ERROR)
        set_failed_mock.assert_awaited_once()
        args = set_failed_mock.await_args.args
        self.assertEqual(str(args[0]), str(run_id))
        self.assertEqual(args[1], main.ERR_PARSE_ERROR)
        self.assertEqual(args[2], "boom")

    def test_normalize_text_removes_empty_lines_and_extra_spaces(self) -> None:
        raw = "  Jane   Doe  \n\n Senior   Engineer \n\n  Python  "
        self.assertEqual(main._normalize_text(raw), "Jane Doe\nSenior Engineer\nPython")

    def test_normalize_text_normalizes_bullets(self) -> None:
        raw = "• Python\n- SQL\n  * Leadership"
        self.assertEqual(main._normalize_text(raw), "- Python\n- SQL\n- Leadership")

    def test_normalize_text_removes_page_markers_and_consecutive_duplicates(self) -> None:
        raw = "Page 1 of 2\nJane Doe\nJane Doe\n2/2\nSenior Engineer"
        self.assertEqual(main._normalize_text(raw), "Jane Doe\nSenior Engineer")

    def test_normalize_text_removes_weird_whitespace(self) -> None:
        raw = "Jane\u00a0Doe\u200b\nSenior\tEngineer"
        self.assertEqual(main._normalize_text(raw), "Jane Doe\nSenior Engineer")

    def test_normalize_text_reduces_repeated_boilerplate_for_pdf_mode(self) -> None:
        raw = "Jane Doe\nACME Inc.\nExperience\nACME Inc.\nSkills\nACME Inc.\nProjects"
        self.assertEqual(
            main._normalize_text(raw, remove_repeated_boilerplate=True),
            "Jane Doe\nACME Inc.\nExperience\nSkills\nProjects",
        )

    def test_extract_docx_parses_text(self) -> None:
        document = Document()
        document.add_paragraph("Jane Doe")
        document.add_paragraph("Senior Engineer")
        buf = BytesIO()
        document.save(buf)

        text, metadata = main._extract_docx(buf.getvalue())
        self.assertIn("Jane Doe", text)
        self.assertIn("Senior Engineer", text)
        self.assertEqual(metadata.get("parser"), "python-docx")

    def test_perform_extraction_rejects_unsupported_extension(self) -> None:
        with patch.object(main, "_download_resume_bytes", AsyncMock(return_value=b"dummy")):
            with self.assertRaises(main.HttpError) as exc:
                asyncio.run(main._perform_extraction("user-123/resume.txt"))
        self.assertEqual(exc.exception.code, main.ERR_UNSUPPORTED_FILE_TYPE)

    def test_extract_pdf_uses_ocr_fallback_when_text_is_empty(self) -> None:
        class FakePage:
            def extract_text(self):
                return ""

        class FakeReader:
            def __init__(self, _):
                self.pages = [FakePage(), FakePage()]

        with (
            patch.object(main, "PdfReader", FakeReader),
            patch.object(main, "_extract_pdf_with_ocr", return_value=("OCR text", {"parser": "pypdf+tesseract"})) as ocr_mock,
        ):
            text, metadata = main._extract_pdf(b"%PDF-1.4 dummy")

        self.assertEqual(text, "OCR text")
        self.assertEqual(metadata["parser"], "pypdf+tesseract")
        ocr_mock.assert_called_once()

    def test_extract_pdf_returns_ocr_unavailable_when_tesseract_missing(self) -> None:
        with patch.object(main.shutil, "which", return_value=None):
            with self.assertRaises(main.HttpError) as exc:
                main._extract_pdf_with_ocr(b"%PDF-1.4 dummy", 1)
        self.assertEqual(exc.exception.code, main.ERR_OCR_UNAVAILABLE)

    def test_extract_rejects_missing_auth_header(self) -> None:
        response = self.client.post("/extract", json={"run_id": str(uuid4())})
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json().get("error_code"), main.ERR_UNAUTHORIZED)

    def test_extract_rejects_wrong_auth_header(self) -> None:
        response = self.client.post(
            "/extract",
            json={"run_id": str(uuid4())},
            headers={"X-Extract-Token": "wrong-token"},
        )
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json().get("error_code"), main.ERR_UNAUTHORIZED)

    def test_validate_content_type_rejects_mismatch(self) -> None:
        with self.assertRaises(main.HttpError) as exc:
            main._validate_content_type("user-123/resume.pdf", "text/plain")
        self.assertEqual(exc.exception.code, main.ERR_UNSUPPORTED_CONTENT_TYPE)

    def test_validate_file_size_rejects_large_payload(self) -> None:
        with patch.object(main, "MAX_RESUME_FILE_SIZE_BYTES", 10), patch.object(main, "MAX_RESUME_FILE_SIZE_MB", 0):
            with self.assertRaises(main.HttpError) as exc:
                main._validate_file_size("11", 5)
        self.assertEqual(exc.exception.code, main.ERR_FILE_TOO_LARGE)

    def test_download_resume_maps_timeout_error(self) -> None:
        class FakeTimeoutClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def get(self, *args, **kwargs):
                raise main.httpx.TimeoutException("timeout")

        with (
            patch.object(main.httpx, "AsyncClient", return_value=FakeTimeoutClient()),
            patch.object(main, "SUPABASE_URL", "https://example.supabase.co"),
            patch.object(main, "SUPABASE_SERVICE_ROLE_KEY", "key"),
            patch.object(main, "SUPABASE_STORAGE_BUCKET", "Resumes"),
        ):
            with self.assertRaises(main.HttpError) as exc:
                asyncio.run(main._download_resume_bytes("user-123/resume.pdf"))
        self.assertEqual(exc.exception.code, main.ERR_DOWNLOAD_FAILED)


if __name__ == "__main__":
    unittest.main()
