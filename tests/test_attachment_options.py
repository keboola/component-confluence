import unittest
from unittest import mock

from keboola.component.exceptions import UserException

from component import (Component, DEFAULT_MAX_FILE_SIZE_MB, KEY_ATTACHMENTS_DOWNLOAD_FILES, KEY_ATTACHMENTS_ENABLED,
                       KEY_ATTACHMENTS_MAX_FILE_SIZE_MB, KEY_ATTACHMENTS_MEDIA_TYPES)


def _options(parameters):
    """Read the attachments group without constructing a real Component (no datadir needed)."""
    component = Component.__new__(Component)
    with mock.patch.object(Component, "configuration", new_callable=mock.PropertyMock) as configuration:
        configuration.return_value = mock.Mock(parameters=parameters)
        return Component._init_attachment_options(component)


class TestAttachmentOptions(unittest.TestCase):

    def test_defaults_are_off_for_an_existing_configuration(self):
        """An existing configuration has no `attachments` group, so nothing changes for it."""
        options = _options({"url": "https://example.atlassian.net"})
        self.assertFalse(options[KEY_ATTACHMENTS_ENABLED])
        self.assertFalse(options[KEY_ATTACHMENTS_DOWNLOAD_FILES])
        self.assertEqual(options[KEY_ATTACHMENTS_MAX_FILE_SIZE_MB], DEFAULT_MAX_FILE_SIZE_MB)
        self.assertEqual(options[KEY_ATTACHMENTS_MEDIA_TYPES], [])

    def test_empty_group_is_off(self):
        self.assertFalse(_options({"attachments": {}})[KEY_ATTACHMENTS_ENABLED])

    def test_null_group_is_off(self):
        self.assertFalse(_options({"attachments": None})[KEY_ATTACHMENTS_ENABLED])

    def test_reads_an_enabled_group(self):
        options = _options({"attachments": {"enabled": True, "download_files": True, "max_file_size_mb": 10}})
        self.assertTrue(options[KEY_ATTACHMENTS_ENABLED])
        self.assertTrue(options[KEY_ATTACHMENTS_DOWNLOAD_FILES])
        self.assertEqual(options[KEY_ATTACHMENTS_MAX_FILE_SIZE_MB], 10)

    def test_media_types_from_a_string(self):
        options = _options({"attachments": {"enabled": True, "media_types": "image/, application/pdf "}})
        self.assertEqual(options[KEY_ATTACHMENTS_MEDIA_TYPES], ["image/", "application/pdf"])

    def test_media_types_from_a_list(self):
        options = _options({"attachments": {"enabled": True, "media_types": ["image/", " application/pdf"]}})
        self.assertEqual(options[KEY_ATTACHMENTS_MEDIA_TYPES], ["image/", "application/pdf"])

    def test_max_file_size_accepts_a_numeric_string(self):
        options = _options({"attachments": {"enabled": True, "max_file_size_mb": "25"}})
        self.assertEqual(options[KEY_ATTACHMENTS_MAX_FILE_SIZE_MB], 25)

    def test_max_file_size_rejects_a_non_number(self):
        with self.assertRaises(UserException):
            _options({"attachments": {"enabled": True, "max_file_size_mb": "big"}})

    def test_max_file_size_rejects_a_negative_number(self):
        with self.assertRaises(UserException):
            _options({"attachments": {"enabled": True, "max_file_size_mb": -1}})


if __name__ == "__main__":
    unittest.main()
