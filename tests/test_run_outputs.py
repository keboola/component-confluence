import csv
import json
import os
import shutil
import tempfile
import unittest
from unittest import mock

from component import Component

PAGE_ROW = {
    "id": "Confluence - Space-123", "created_date": "2024-01-01T00:00:00.000Z",
    "last_updated_date": "2024-01-02T00:00:00.000Z", "title": "Page", "creator": "Jane",
    "last_modifier": "Jane", "url": "https://example.atlassian.net/wiki/spaces/S/pages/123",
    "space": "Space", "text": "body",
}

ATTACHMENT_ROW = {
    "id": "att1", "page_id": "Confluence - Space-123", "space": "Space", "title": "diagram.png",
    "media_type": "image/png", "file_size": 12, "created_date": "2024-01-01T00:00:00.000Z",
    "last_updated_date": "2024-01-02T00:00:00.000Z", "creator": "Jane",
    "download_url": "https://example.atlassian.net/wiki/download/diagram.png", "file_name": "",
}


class TestRunOutputs(unittest.TestCase):
    """End to end checks of what the component writes into the data directory."""

    def setUp(self):
        self.datadir = tempfile.mkdtemp()
        for path in ("out/tables", "out/files", "in"):
            os.makedirs(os.path.join(self.datadir, path), exist_ok=True)
        self.addCleanup(shutil.rmtree, self.datadir, True)

    def _write_config(self, parameters):
        with open(os.path.join(self.datadir, "config.json"), "w") as handle:
            json.dump({"parameters": parameters}, handle)

    def _run(self, parameters, client):
        self._write_config(parameters)
        with mock.patch.dict(os.environ, {"KBC_DATADIR": self.datadir}), \
                mock.patch("component.ConfluenceClient", return_value=client):
            Component().run()

    @staticmethod
    def _client(attachments=None, downloads=True):
        client = mock.MagicMock()
        client.iter_pages.return_value = iter([("123", "Space", dict(PAGE_ROW))])
        client.get_page_attachments.return_value = iter(attachments or [])
        def _download(row, path, max_bytes=0):
            if not downloads:
                return False
            with open(path, "wb") as handle:
                handle.write(b"binarycontent")
            return True

        client.download_attachment.side_effect = _download
        client.fetched_total = 1
        client.attachments_total = len(attachments or [])
        client.attachments_downloaded = 0
        client.attachments_skipped = 0
        client.attachments_failed = 0
        client.pages_with_unreadable_attachments = 0
        return client

    def _tables(self):
        return sorted(f for f in os.listdir(os.path.join(self.datadir, "out/tables")) if not f.endswith(".manifest"))

    def _files(self):
        return sorted(f for f in os.listdir(os.path.join(self.datadir, "out/files")) if not f.endswith(".manifest"))

    def _read(self, table):
        with open(os.path.join(self.datadir, "out/tables", table)) as handle:
            return list(csv.DictReader(handle))

    BASE = {"url": "https://example.atlassian.net", "username": "u", "#api_token": "t"}

    def test_existing_configuration_is_unchanged(self):
        """No `attachments` group: one table, no files, and the attachment API is never called."""
        client = self._client()
        self._run(dict(self.BASE), client)

        self.assertEqual(self._tables(), ["confluence_pages"])
        self.assertEqual(self._files(), [])
        client.get_page_attachments.assert_not_called()
        rows = self._read("confluence_pages")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], "Confluence - Space-123")

    def test_attachments_disabled_explicitly(self):
        client = self._client()
        self._run(dict(self.BASE, attachments={"enabled": False}), client)
        self.assertEqual(self._tables(), ["confluence_pages"])
        client.get_page_attachments.assert_not_called()

    def test_metadata_only(self):
        """Enabled without download_files: a second table, still no files in File Storage."""
        client = self._client(attachments=[dict(ATTACHMENT_ROW)])
        self._run(dict(self.BASE, attachments={"enabled": True}), client)

        self.assertEqual(self._tables(), ["confluence_attachments", "confluence_pages"])
        self.assertEqual(self._files(), [])
        client.download_attachment.assert_not_called()

        rows = self._read("confluence_attachments")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], "att1")
        self.assertEqual(rows[0]["file_name"], "")
        # the attachment row joins the page row
        self.assertEqual(rows[0]["page_id"], self._read("confluence_pages")[0]["id"])

    def test_downloads_files_and_links_them(self):
        client = self._client(attachments=[dict(ATTACHMENT_ROW)])
        self._run(dict(self.BASE, attachments={"enabled": True, "download_files": True}), client)

        self.assertEqual(self._files(), ["att1_diagram.png"])
        rows = self._read("confluence_attachments")
        self.assertEqual(rows[0]["file_name"], "att1_diagram.png")
        manifest = os.path.join(self.datadir, "out/files", "att1_diagram.png.manifest")
        self.assertTrue(os.path.exists(manifest))
        with open(manifest) as handle:
            self.assertIn("confluence-attachment", json.load(handle)["tags"])

    def test_skipped_download_leaves_no_file_and_an_empty_file_name(self):
        client = self._client(attachments=[dict(ATTACHMENT_ROW)], downloads=False)
        self._run(dict(self.BASE, attachments={"enabled": True, "download_files": True}), client)

        self.assertEqual(self._files(), [])
        self.assertEqual(self._read("confluence_attachments")[0]["file_name"], "")

    def test_media_filter_limits_the_files_but_not_the_table(self):
        """The table keeps the full inventory so the user can see what was not downloaded."""
        pdf = dict(ATTACHMENT_ROW, id="att2", title="report.pdf", media_type="application/pdf")
        client = self._client(attachments=[dict(ATTACHMENT_ROW), pdf])
        self._run(dict(self.BASE, attachments={"enabled": True, "download_files": True,
                                               "media_types": "image/"}), client)

        rows = self._read("confluence_attachments")
        self.assertEqual([row["id"] for row in rows], ["att1", "att2"])
        self.assertEqual(self._files(), ["att1_diagram.png"])
        self.assertEqual(rows[0]["file_name"], "att1_diagram.png")
        self.assertEqual(rows[1]["file_name"], "")

    def test_files_are_permanent_and_tagged_per_configuration(self):
        client = self._client(attachments=[dict(ATTACHMENT_ROW)])
        env = {"KBC_DATADIR": self.datadir, "KBC_COMPONENTID": "kds-team.ex-confluence",
               "KBC_CONFIGID": "12345", "KBC_RUNID": "run-9"}
        self._write_config(dict(self.BASE, attachments={"enabled": True, "download_files": True}))
        with mock.patch.dict(os.environ, env), mock.patch("component.ConfluenceClient", return_value=client):
            Component().run()

        with open(os.path.join(self.datadir, "out/files", "att1_diagram.png.manifest")) as handle:
            manifest = json.load(handle)
        self.assertTrue(manifest["is_permanent"])
        self.assertIn("confluence-attachment", manifest["tags"])
        self.assertIn("configId-12345", manifest["tags"])
        self.assertIn("runId-run-9", manifest["tags"])
        self.assertIn("componentId-kds-team.ex-confluence", manifest["tags"])

    def test_fractional_size_limit_is_not_treated_as_unlimited(self):
        client = self._client(attachments=[dict(ATTACHMENT_ROW)])
        self._run(dict(self.BASE, attachments={"enabled": True, "download_files": True,
                                               "max_file_size_mb": 0.5}), client)
        _, kwargs = client.download_attachment.call_args
        self.assertEqual(kwargs["max_bytes"], 524288)

    def test_zero_size_limit_means_no_limit(self):
        client = self._client(attachments=[dict(ATTACHMENT_ROW)])
        self._run(dict(self.BASE, attachments={"enabled": True, "download_files": True,
                                               "max_file_size_mb": 0}), client)
        _, kwargs = client.download_attachment.call_args
        self.assertEqual(kwargs["max_bytes"], 0)

    def test_pages_table_columns_are_unchanged(self):
        client = self._client()
        self._run(dict(self.BASE), client)
        with open(os.path.join(self.datadir, "out/tables", "confluence_pages")) as handle:
            header = handle.readline().strip()
        self.assertEqual(header, "id,created_date,last_updated_date,title,creator,last_modifier,url,space,text")


if __name__ == "__main__":
    unittest.main()
