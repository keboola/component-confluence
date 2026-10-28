import unittest
from unittest import mock

from atlassian.errors import ApiError
from requests.exceptions import HTTPError

from client.confluence_client import ConfluenceClient, ConfluenceClientException
from component import Component


def _keys(parameters):
    component = Component.__new__(Component)
    with mock.patch.object(Component, "configuration", new_callable=mock.PropertyMock) as configuration:
        configuration.return_value = mock.Mock(parameters=parameters)
        return Component._init_space_keys(component)


def _client():
    with mock.patch("client.confluence_client.Confluence"):
        return ConfluenceClient("https://example.atlassian.net", "user", "token")


class TestSpaceKeyParsing(unittest.TestCase):

    def test_no_filter_for_an_existing_configuration(self):
        self.assertEqual(_keys({"url": "https://example.atlassian.net"}), [])

    def test_empty_string_is_no_filter(self):
        self.assertEqual(_keys({"spaces": ""}), [])

    def test_null_is_no_filter(self):
        self.assertEqual(_keys({"spaces": None}), [])

    def test_reads_a_comma_separated_string(self):
        self.assertEqual(_keys({"spaces": "CSU, ENG"}), ["CSU", "ENG"])

    def test_ignores_empty_items_and_spacing(self):
        self.assertEqual(_keys({"spaces": " CSU , , ENG ,"}), ["CSU", "ENG"])

    def test_reads_a_list(self):
        self.assertEqual(_keys({"spaces": ["CSU", " ENG"]}), ["CSU", "ENG"])

    def test_removes_duplicates_and_keeps_the_order(self):
        self.assertEqual(_keys({"spaces": "ENG, CSU, ENG"}), ["ENG", "CSU"])


class TestSpaceResolution(unittest.TestCase):

    def test_without_a_filter_it_lists_every_space(self):
        client = _client()
        client.confluence.get_all_spaces.return_value = {"results": [{"key": "A", "name": "A"}]}

        self.assertEqual(client._get_spaces([]), [{"key": "A", "name": "A"}])
        client.confluence.get_all_spaces.assert_called_once_with()
        client.confluence.get_space.assert_not_called()

    def test_with_a_filter_it_reads_each_space_by_key(self):
        client = _client()
        client.confluence.get_space.side_effect = [{"key": "CSU", "name": "Customer Success"},
                                                   {"key": "ENG", "name": "Engineering"}]

        spaces = client._get_spaces(["CSU", "ENG"])

        self.assertEqual([space["key"] for space in spaces], ["CSU", "ENG"])
        # the unpaginated listing is skipped entirely, so a space is found on a large site
        client.confluence.get_all_spaces.assert_not_called()

    def test_an_unknown_key_stops_the_run_with_a_clear_message(self):
        for error in (HTTPError("404 Not Found"), ApiError("no permission")):
            client = _client()
            client.confluence.get_space.side_effect = error
            with self.assertRaises(ConfluenceClientException) as caught:
                client._get_spaces(["NOPE"])
            self.assertIn("NOPE", str(caught.exception))
            self.assertIn("space key", str(caught.exception))

    def test_an_empty_space_response_stops_the_run(self):
        client = _client()
        client.confluence.get_space.return_value = None
        with self.assertRaises(ConfluenceClientException):
            client._get_spaces(["NOPE"])


class TestIterPagesUsesTheFilter(unittest.TestCase):

    def test_only_the_listed_spaces_are_read(self):
        client = _client()
        client.confluence.get_space.return_value = {"key": "CSU", "name": "Customer Success"}
        client.confluence.get_space_content.return_value = {"page": {"results": [], "size": 0}}

        list(client.iter_pages(space_keys=["CSU"]))

        client.confluence.get_all_spaces.assert_not_called()
        client.confluence.get_space_content.assert_called_once()
        self.assertEqual(client.confluence.get_space_content.call_args[0][0], "CSU")


if __name__ == "__main__":
    unittest.main()
