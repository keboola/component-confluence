from atlassian import Confluence
from bs4 import BeautifulSoup

from datetime import datetime
import logging
from requests.exceptions import HTTPError, InvalidSchema


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


class ConfluenceClientException(Exception):
    pass


class ConfluenceClient:
    def __init__(self, confluence_url, confluence_username, confluence_password):
        self.confluence = Confluence(
            url=confluence_url, username=confluence_username, password=confluence_password
        )
        self.fetched_total = 0

    def get_confluence_pages(self, timestamp_from: str = None, beautify: bool = True, limit: int = 100) -> dict:
        try:
            spaces = self.confluence.get_all_spaces()
        except HTTPError as e:
            raise ConfluenceClientException(f"HTTPError occured, please check your credentials. Details: {e}")
        except InvalidSchema as e:
            raise ConfluenceClientException(f"InvalidSchema error occured, please check Confluence url. Details: {e}")

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

                for result in results:
                    metadata = self._get_metadata(result)
                    last_updated = datetime.strptime(metadata.get("LastUpdatedDate"), "%Y-%m-%dT%H:%M:%S.%fZ")

                    if not timestamp_from or (timestamp_from and last_updated > timestamp_from):
                        yield from self._build_result(result, metadata, beautify)

                if page.get("size") == limit:
                    start += limit
                else:
                    break

    def _build_result(self, result: dict, metadata: dict, beautify: bool = True):
        logging.debug(f'Fetching document from Space: {metadata["Space"]} with document id {result["id"]}')

        text = result["body"]["storage"]["value"]

        if beautify:
            soup = BeautifulSoup(text, "html.parser")
            text = soup.get_text()
            text = result["title"] + "\n\n" + text

        metadata["text"] = text
        data = {rename_map[key]: metadata[key] for key in metadata.keys()}

        self.fetched_total += 1
        yield data

    def _get_metadata(self, result):
        page_id = result.get("id")
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
