from atlassian import Confluence
from bs4 import BeautifulSoup

from datetime import datetime
import logging


rename_map = {
    "id": "id",
    "CreatedDate": "created_date",
    "LastUpdatedDate": "last_updated_date",
    "Title": "title",
    "Creator": "creator",
    "LastModifier": "last_modifier",
    "url": "url",
    "Space": "space",
    "text": "text"
}


class ConfluenceClient:
    def __init__(self, confluence_url, confluence_username, confluence_password):
        self.confluence = Confluence(
            url=confluence_url, username=confluence_username, password=confluence_password
        )
        self.fetched_total = 0

    def get_confluence_pages(self, timestamp_from: str = None, beautify: bool = True, limit: int = 100) -> dict:
        spaces = self.confluence.get_all_spaces()

        if timestamp_from:
            timestamp_from = datetime.strptime(timestamp_from, "%Y-%m-%dT%H:%M:%S.%fZ")

        for space in spaces.get("results"):
            logging.info(f"Downloading Confluence space: {space['name']}")
            start = 0

            while True:
                content = self.confluence.get_space_content(space["key"], start=start, limit=limit)
                page = content.get("page")
                results = page.get("results")

                if not results:
                    logging.info(f"No results for {space['name']}")
                    break

                metadata = self._get_metadata(results)
                last_updated = datetime.strptime(metadata.get("LastUpdatedDate"), "%Y-%m-%dT%H:%M:%S.%fZ")

                if not timestamp_from or (timestamp_from and last_updated > timestamp_from):
                    yield from self._build_result(results, metadata, beautify)
                    self.fetched_total += 1

                if page.get("size") == limit:
                    start += limit
                else:
                    break

    @staticmethod
    def _build_result(results: list, metadata: dict, beautify: bool = True):
        for result in results:
            logging.debug(f'Fetching document from Space: {metadata["Space"]} with document id {result["id"]}')

            text = result["body"]["storage"]["value"]

            if beautify:
                soup = BeautifulSoup(text, "html.parser")
                text = soup.get_text()
                text = result["title"] + "\n\n" + text

            metadata["text"] = text
            data = {rename_map[key]: metadata[key] for key in metadata.keys()}

            yield data

    def _get_metadata(self, results):
        page_id = results[0].get("id")
        if page_id:
            data = self.confluence.get_page_by_id(page_id)
            space = data["space"].get("name", "")

            page_metadata = {
                "id": "Confluence - " + space + "-" + data.get("id", ""),
                "CreatedDate": data["history"].get("createdDate", ""),
                "LastUpdatedDate": data["version"].get("when", ""),
                "Title": data.get("title", ""),
                "Creator": data["history"]["createdBy"].get("displayName", ""),
                "LastModifier": data["version"]["by"].get("displayName", ""),
                "url": f"{data['_links']['base']}/spaces/{data['space'].get('key', '')}/pages/{data.get('id', '')}",
                "Space": space,
            }

            return page_metadata
        return {}
