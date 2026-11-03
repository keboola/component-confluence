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

    def test_a_space_name_is_accepted(self):
        """The `space` column holds the name, so a user who copies from the output gives a name."""
        client = _client()
        client.confluence.get_space.side_effect = HTTPError("404 Not Found")
        client.confluence.get_all_spaces.return_value = {"results": [
            {"key": "ENG", "name": "Engineering Wiki"}, {"key": "CSU", "name": "Customer Success"}]}

        spaces = client._get_spaces(["Engineering Wiki"])

        self.assertEqual([space["key"] for space in spaces], ["ENG"])

    def test_a_name_or_key_is_matched_ignoring_letter_case(self):
        for entry in ("engineering wiki", "eng", "ENG", "  Engineering Wiki  "):
            client = _client()
            client.confluence.get_space.side_effect = HTTPError("404 Not Found")
            client.confluence.get_all_spaces.return_value = {"results": [
                {"key": "ENG", "name": "Engineering Wiki"}]}
            self.assertEqual(client._get_spaces([entry])[0]["key"], "ENG", entry)

    def test_an_exact_key_does_not_list_every_space(self):
        client = _client()
        client.confluence.get_space.return_value = {"key": "ENG", "name": "Engineering Wiki"}
        client._get_spaces(["ENG"])
        client.confluence.get_all_spaces.assert_not_called()

    def test_a_key_match_wins_over_a_name_match(self):
        client = _client()
        client.confluence.get_space.side_effect = HTTPError("404")
        client.confluence.get_all_spaces.return_value = {"results": [
            {"key": "OTHER", "name": "ENG"}, {"key": "ENG", "name": "Engineering Wiki"}]}
        self.assertEqual(client._get_spaces(["ENG"])[0]["key"], "ENG")

    def test_an_unknown_space_lists_what_is_available(self):
        for error in (HTTPError("404 Not Found"), ApiError("no permission")):
            client = _client()
            client.confluence.get_space.side_effect = error
            client.confluence.get_all_spaces.return_value = {"results": [
                {"key": "ENG", "name": "Engineering Wiki"}]}
            with self.assertRaises(ConfluenceClientException) as caught:
                client._get_spaces(["NOPE"])
            message = str(caught.exception)
            self.assertIn("NOPE", message)
            self.assertIn("Engineering Wiki (ENG)", message)

    def test_an_empty_space_response_falls_back_to_matching(self):
        client = _client()
        client.confluence.get_space.return_value = None
        client.confluence.get_all_spaces.return_value = {"results": [
            {"key": "ENG", "name": "Engineering Wiki"}]}
        self.assertEqual(client._get_spaces(["Engineering Wiki"])[0]["key"], "ENG")


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
