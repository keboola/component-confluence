import os
import tempfile
import unittest
from unittest import mock

from client.confluence_client import (ConfluenceClient, ConfluenceClientException, matches_media_type,
                                      sanitize_file_name)


def _attachment(att_id="att1", title="diagram.png", media_type="image/png", size=1234, download="/download/a.png"):
    return {
        "id": att_id,
        "title": title,
        "extensions": {"mediaType": media_type, "fileSize": size},
        "version": {"when": "2024-01-02T03:04:05.000Z"},
        "history": {"createdDate": "2024-01-01T00:00:00.000Z", "createdBy": {"displayName": "Jane"}},
        "_links": {"download": download},
    }


def _client():
    with mock.patch("client.confluence_client.Confluence"):
        client = ConfluenceClient("https://example.atlassian.net", "user", "token")
    # the real constructor derives this from the url; keep it explicit for the host check
    assert client.allowed_host == "example.atlassian.net"
    return client


class TestSanitizeFileName(unittest.TestCase):

    def test_prefixes_with_attachment_id(self):
        self.assertEqual(sanitize_file_name("att123", "diagram.png"), "att123_diagram.png")

    def test_replaces_unsafe_characters(self):
        self.assertEqual(sanitize_file_name("att1", "my report (final).pdf"), "att1_my_report_final.pdf")

    def test_strips_path_traversal(self):
        result = sanitize_file_name("att1", "../../etc/passwd")
        self.assertNotIn("/", result)
        self.assertNotIn("..", result)

    def test_falls_back_when_title_is_empty(self):
        self.assertEqual(sanitize_file_name("att1", ""), "att1_attachment")

    def test_keeps_extension_when_name_is_long(self):
        result = sanitize_file_name("att1", "a" * 300 + ".png")
        self.assertTrue(result.endswith(".png"))
        self.assertLess(len(result), 200)


class TestAttachmentRow(unittest.TestCase):

    def test_builds_row_with_absolute_download_url(self):
        row = ConfluenceClient._build_attachment_row(
            _attachment(), "Confluence - Space-123", "Space", "https://example.atlassian.net/wiki")
        self.assertEqual(row["id"], "att1")
        self.assertEqual(row["page_id"], "Confluence - Space-123")
        self.assertEqual(row["space"], "Space")
        self.assertEqual(row["media_type"], "image/png")
        self.assertEqual(row["file_size"], 1234)
        self.assertEqual(row["creator"], "Jane")
        self.assertEqual(row["download_url"], "https://example.atlassian.net/wiki/download/a.png")
        self.assertEqual(row["file_name"], "")

    def test_tolerates_missing_fields(self):
        row = ConfluenceClient._build_attachment_row({"id": "att9"}, "page", "Space", "https://example.net/wiki")
        self.assertEqual(row["id"], "att9")
        self.assertEqual(row["download_url"], "")
        self.assertEqual(row["creator"], "")

    def test_media_type_filter(self):
        self.assertTrue(matches_media_type("image/png", ["image/"]))
        self.assertTrue(matches_media_type("application/pdf", ["image/", "application/pdf"]))
        self.assertFalse(matches_media_type("text/plain", ["image/"]))
        self.assertFalse(matches_media_type("", ["image/"]))

    def test_no_filter_matches_everything(self):
        self.assertTrue(matches_media_type("text/plain", []))
        self.assertTrue(matches_media_type("text/plain", None))


class TestGetPageAttachments(unittest.TestCase):

    def test_paginates_and_lists_every_attachment(self):
        client = _client()
        page_one = {"results": [_attachment("a1"), _attachment("a2", media_type="text/plain")],
                    "_links": {"base": "https://example.atlassian.net/wiki"}}
        page_two = {"results": [_attachment("a3")], "_links": {"base": "https://example.atlassian.net/wiki"}}
        client.confluence.get_attachments_from_content.side_effect = [page_one, page_two]

        rows = list(client.get_page_attachments("123", "page-key", "Space", limit=2))

        # the table holds the full inventory; the media type filter only limits the downloaded files
        self.assertEqual([row["id"] for row in rows], ["a1", "a2", "a3"])
        self.assertEqual(client.attachments_total, 3)

    def test_stops_on_short_page(self):
        client = _client()
        client.confluence.get_attachments_from_content.return_value = {
            "results": [_attachment("a1")], "_links": {"base": "https://example.atlassian.net/wiki"}}

        rows = list(client.get_page_attachments("123", "page-key", "Space", limit=50))

        self.assertEqual(len(rows), 1)
        self.assertEqual(client.confluence.get_attachments_from_content.call_count, 1)


class TestDownloadAttachment(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.target = os.path.join(self.tmp, "file.bin")

    def _row(self, size=10, url="https://example.atlassian.net/wiki/download/a.png"):
        return {"id": "att1", "title": "a.png", "file_size": size, "download_url": url}

    def _mock_response(self, client, chunks):
        response = mock.MagicMock()
        response.iter_content.return_value = chunks
        response.__enter__.return_value = response
        client.confluence._session.get.return_value = response
        return response

    def test_downloads_file(self):
        client = _client()
        self._mock_response(client, [b"hello ", b"world"])

        self.assertTrue(client.download_attachment(self._row(), self.target, max_bytes=1024))

        with open(self.target, "rb") as handle:
            self.assertEqual(handle.read(), b"hello world")
        self.assertEqual(client.attachments_downloaded, 1)

    def test_skips_when_declared_size_is_over_the_limit(self):
        client = _client()
        self.assertFalse(client.download_attachment(self._row(size=5000), self.target, max_bytes=1024))
        self.assertFalse(os.path.exists(self.target))
        self.assertEqual(client.attachments_skipped, 1)
        client.confluence._session.get.assert_not_called()

    def test_skips_when_the_stream_is_over_the_limit(self):
        client = _client()
        self._mock_response(client, [b"x" * 600, b"x" * 600])

        self.assertFalse(client.download_attachment(self._row(size=None), self.target, max_bytes=1000))

        self.assertFalse(os.path.exists(self.target))
        self.assertEqual(client.attachments_skipped, 1)
        self.assertEqual(client.attachments_downloaded, 0)

    def test_no_limit_when_max_bytes_is_zero(self):
        client = _client()
        self._mock_response(client, [b"x" * 5000])
        self.assertTrue(client.download_attachment(self._row(size=5000), self.target, max_bytes=0))
        self.assertEqual(client.attachments_downloaded, 1)

    def test_skips_when_there_is_no_download_link(self):
        client = _client()
        self.assertFalse(client.download_attachment(self._row(url=""), self.target, max_bytes=1024))
        self.assertEqual(client.attachments_skipped, 1)

    def test_skips_a_non_http_url(self):
        client = _client()
        self.assertFalse(client.download_attachment(self._row(url="file:///etc/passwd"), self.target,
                                                    max_bytes=1024))
        self.assertEqual(client.attachments_skipped, 1)
        client.confluence._session.get.assert_not_called()

    def test_never_sends_credentials_to_another_host(self):
        """An absolute download link to a foreign host must not receive the Confluence credentials."""
        client = _client()
        self.assertFalse(client.download_attachment(
            self._row(url="https://evil.example.com/steal"), self.target, max_bytes=1024))
        client.confluence._session.get.assert_not_called()
        self.assertEqual(client.attachments_skipped, 1)

    def test_raises_a_user_facing_error_when_the_disk_is_full(self):
        client = _client()
        self._mock_response(client, [b"x" * 10])
        with mock.patch("builtins.open", side_effect=OSError("No space left on device")):
            with self.assertRaises(ConfluenceClientException):
                client.download_attachment(self._row(), self.target, max_bytes=1024)



class TestResilience(unittest.TestCase):
    """One unreadable page or one failed file must not stop a long extraction."""

    def test_unexpected_response_body_is_a_warning(self):
        """AtlassianRestAPI returns None for an empty body and a str for a non JSON body."""
        for body in (None, "<html>login</html>"):
            client = _client()
            client.confluence.get_attachments_from_content.return_value = body
            self.assertEqual(list(client.get_page_attachments("123", "page-key", "Space")), [])
            self.assertEqual(client.pages_with_unreadable_attachments, 1)

    def test_unreadable_attachment_listing_is_a_warning(self):
        from atlassian.errors import ApiError
        client = _client()
        client.confluence.get_attachments_from_content.side_effect = ApiError("no permission")

        rows = list(client.get_page_attachments("123", "page-key", "Space"))

        self.assertEqual(rows, [])
        self.assertEqual(client.pages_with_unreadable_attachments, 1)

    def test_failed_download_is_a_warning(self):
        from requests.exceptions import ConnectionError
        client = _client()
        client.confluence._session.get.side_effect = ConnectionError("boom")

        with tempfile.TemporaryDirectory() as tmp:
            target = os.path.join(tmp, "file.bin")
            result = client.download_attachment(
                {"id": "att1", "title": "a.png", "file_size": 10,
                 "download_url": "https://example.atlassian.net/wiki/download/a.png"}, target, max_bytes=1024)

            self.assertFalse(result)
            self.assertFalse(os.path.exists(target))
        self.assertEqual(client.attachments_failed, 1)
        self.assertEqual(client.attachments_downloaded, 0)

if __name__ == "__main__":
    unittest.main()
