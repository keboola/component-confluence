import logging
import os
import re
from datetime import datetime
from urllib.parse import urljoin, urlparse

from atlassian import Confluence
from atlassian.errors import ApiError
from bs4 import BeautifulSoup
from requests.exceptions import HTTPError, InvalidSchema, RequestException

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

ATTACHMENT_FIELDS = ["id", "page_id", "space", "title", "media_type", "file_size", "created_date",
                     "last_updated_date", "creator", "download_url", "file_name"]

# characters that are not safe in a Keboola File Storage file name
UNSAFE_FILENAME_CHARS = re.compile(r"[^A-Za-z0-9._-]+")
MAX_FILENAME_LENGTH = 120
MAX_EXTENSION_LENGTH = 16
DOWNLOAD_CHUNK_SIZE = 1024 * 1024


class ConfluenceClientException(Exception):
    pass


def matches_media_type(media_type: str, media_types: list) -> bool:
    """Return True when `media_type` starts with one of the configured prefixes, or when there is no filter."""
    if not media_types:
        return True
    media_type = (media_type or "").lower()
    return any(media_type.startswith(prefix.strip().lower()) for prefix in media_types if prefix.strip())


def sanitize_file_name(attachment_id: str, title: str) -> str:
    """Build a collision free, storage safe file name for an attachment.

    The attachment id is used as a prefix, so two pages with a file of the same name do not overwrite
    each other. Path separators and other unsafe characters are replaced.
    """
    root, ext = os.path.splitext((title or "attachment").strip())
    root = UNSAFE_FILENAME_CHARS.sub("_", root).strip("._-")[:MAX_FILENAME_LENGTH].strip("._-") or "attachment"
    ext = UNSAFE_FILENAME_CHARS.sub("", ext)[:MAX_EXTENSION_LENGTH]
    prefix = UNSAFE_FILENAME_CHARS.sub("_", attachment_id or "").strip("._-")
    return f"{prefix}_{root}{ext}" if prefix else f"{root}{ext}"


class ConfluenceClient:
    def __init__(self, confluence_url, confluence_username, confluence_password):
        self.confluence = Confluence(
            url=confluence_url, username=confluence_username, password=confluence_password
        )
        # The attachment download link comes from the API response. Only the configured host may
        # receive the credentials of the session, so the resolved host is checked against this one.
        self.allowed_host = (urlparse(confluence_url).hostname or "").lower()
        # Confluence Cloud removed the /download/attachments/... route for API token callers
        # (Atlassian CHANGE-2735, removed 2025-11-06). Cloud must use the REST attachment route.
        self.is_cloud = getattr(self.confluence, "cloud", False) is True or \
            self.allowed_host.endswith(".atlassian.net")
        self.fetched_total = 0
        self.attachments_total = 0
        self.attachments_downloaded = 0
        self.attachments_skipped = 0
        self.attachments_failed = 0
        self.pages_with_unreadable_attachments = 0

    def get_confluence_pages(self, timestamp_from: str = None, beautify: bool = True, limit: int = 100) -> dict:
        """Yield page rows. Kept for backwards compatibility with the original client contract."""
        for _, _, page_row in self.iter_pages(timestamp_from=timestamp_from, beautify=beautify, limit=limit):
            yield page_row

    def _get_spaces(self, space_keys: list = None) -> list:
        """Return the spaces to read.

        Without a filter this makes the same single `get_all_spaces` call as before. With a filter each
        space is read by key, so a space is found even when the account can see very many spaces.
        """
        if not space_keys:
            try:
                spaces = self.confluence.get_all_spaces()
            except HTTPError as e:
                raise ConfluenceClientException(f"HTTPError occured, please check your credentials. "
                                                f"Details: {e}")
            except InvalidSchema as e:
                raise ConfluenceClientException(f"InvalidSchema error occured, please check Confluence url. "
                                                f"Details: {e}")
            return spaces.get("results")

        resolved = []
        for key in space_keys:
            try:
                space = self.confluence.get_space(key)
            except InvalidSchema as e:
                raise ConfluenceClientException(f"InvalidSchema error occured, please check Confluence url. "
                                                f"Details: {e}")
            except (HTTPError, ApiError) as e:
                raise ConfluenceClientException(
                    f"Cannot read the Confluence space '{key}'. Check that the space key is correct and "
                    f"that the account can read the space. The space key is the short code in the page "
                    f"address, for example 'CSU' in /spaces/CSU/pages/123. Details: {e}")
            if not isinstance(space, dict) or not space.get("key"):
                raise ConfluenceClientException(
                    f"Confluence returned no space for the key '{key}'. Check that the space key is correct.")
            resolved.append(space)
        return resolved

    def iter_pages(self, timestamp_from: str = None, beautify: bool = True, limit: int = 100,
                   space_keys: list = None):
        """Yield (raw_page_id, space_name, page_row) for every page that passes the incremental filter.

        `space_keys` limits the run to those spaces. Empty or None reads every space the account can see.
        """
        space_list = self._get_spaces(space_keys)

        if timestamp_from:
            timestamp_from = datetime.strptime(timestamp_from, "%Y-%m-%dT%H:%M:%S.%fZ")

        for space in space_list:
            logging.info(f"Downloading Confluence space: {space['name']} ({space['key']})")
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
                        for page_row in self._build_result(result, metadata, beautify):
                            yield result.get("id"), metadata.get("Space", ""), page_row

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

    def get_page_attachments(self, page_id: str, page_key: str, space: str, limit: int = 50):
        """Yield one metadata row per attachment of the given page.

        `page_key` is the value used as `id` in the confluence_pages table, so the two tables can be joined.
        Every attachment is listed. The media type filter applies to the downloaded files only, so the
        table always holds the full inventory.
        """
        start = 0
        while True:
            try:
                response = self.confluence.get_attachments_from_content(
                    page_id, start=start, limit=limit, expand="version,history,metadata"
                )
            except (HTTPError, ApiError, RequestException) as e:
                # One unreadable page must not stop a long extraction. Confluence returns 404 for
                # content the account cannot see, so this is most often a permission problem.
                logging.warning(f"Cannot list the attachments of page {page_id}. The page itself is still "
                                f"extracted. Details: {e}")
                self.pages_with_unreadable_attachments += 1
                return

            if not isinstance(response, dict):
                # AtlassianRestAPI returns None for an empty body and a str for a non JSON body,
                # for example an HTML login page served with status 200.
                logging.warning(f"Confluence returned an unexpected response for the attachments of page "
                                f"{page_id}. The page itself is still extracted.")
                self.pages_with_unreadable_attachments += 1
                return

            results = response.get("results") or []
            base = (response.get("_links") or {}).get("base") or self.confluence.url

            for attachment in results:
                self.attachments_total += 1
                yield self._build_attachment_row(attachment, page_key, space, base, page_id)

            if len(results) == limit:
                start += limit
            else:
                break

    def _build_attachment_row(self, attachment: dict, page_key: str, space: str, base: str,
                              page_id: str) -> dict:
        extensions = attachment.get("extensions") or {}
        version = attachment.get("version") or {}
        history = attachment.get("history") or {}
        links = attachment.get("_links") or {}
        download_path = links.get("download", "")
        attachment_id = attachment.get("id", "")

        if self.is_cloud and attachment_id and page_id:
            # The supported Cloud route. `_links.download` still points at the removed
            # /download/attachments/... endpoint, which answers 404 for API token callers.
            download_url = f"{base.rstrip('/')}/rest/api/content/{page_id}/child/attachment/" \
                           f"{attachment_id}/download"
        elif download_path:
            download_url = urljoin(base + "/", download_path.lstrip("/"))
        else:
            download_url = ""

        return {
            "id": attachment_id,
            "page_id": page_key,
            "space": space,
            "title": attachment.get("title", ""),
            "media_type": extensions.get("mediaType", ""),
            "file_size": extensions.get("fileSize", ""),
            "created_date": history.get("createdDate", ""),
            "last_updated_date": version.get("when", ""),
            "creator": (history.get("createdBy") or {}).get("displayName", ""),
            "download_url": download_url,
            "file_name": "",
        }

    def download_attachment(self, row: dict, target_path: str, max_bytes: int = 0) -> bool:
        """Stream one attachment to `target_path`. Return True when the file was written.

        Returns False (and writes nothing) when the attachment has no download link, or when it is
        larger than `max_bytes`. `max_bytes` of 0 means no size limit.
        """
        url = row.get("download_url") or ""
        title = row.get("title") or row.get("id")

        if not url:
            logging.warning(f"Attachment '{title}' has no download link. The file is not downloaded.")
            self.attachments_skipped += 1
            return False

        parsed = urlparse(url)
        if not parsed.scheme.startswith("http"):
            logging.warning(f"Attachment '{title}' has a download link that is not an HTTP url. "
                            f"The file is not downloaded.")
            self.attachments_skipped += 1
            return False

        if self.allowed_host and (parsed.hostname or "").lower() != self.allowed_host:
            # Never send the Confluence credentials of the session to another host.
            logging.warning(f"Attachment '{title}' points to the host '{parsed.hostname}', which is not the "
                            f"configured Confluence host. The file is not downloaded.")
            self.attachments_skipped += 1
            return False

        declared_size = row.get("file_size")
        if max_bytes and isinstance(declared_size, int) and declared_size > max_bytes:
            logging.warning(f"Attachment '{title}' is {declared_size} B, which is over the configured limit "
                            f"of {max_bytes} B. The file is not downloaded. Its metadata is still in the "
                            f"confluence_attachments table.")
            self.attachments_skipped += 1
            return False

        written = 0
        try:
            with self.confluence._session.get(url, stream=True, timeout=120) as response:
                if response.status_code == 404:
                    # The attachment record exists but its binary does not. This happens with
                    # attachments that are orphaned in the Atlassian media storage.
                    logging.warning(f"Confluence has no file for attachment '{title}'. Its metadata is "
                                    f"still in the confluence_attachments table.")
                    self.attachments_failed += 1
                    return False
                response.raise_for_status()

                content_type = (response.headers.get("Content-Type") or "").lower()
                if content_type.startswith("text/html"):
                    # A login or error page must never be saved with the name of the attachment.
                    logging.warning(f"Confluence returned a web page instead of the file for attachment "
                                    f"'{title}'. The file is not downloaded.")
                    self.attachments_failed += 1
                    return False

                with open(target_path, "wb") as out_file:
                    for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                        if not chunk:
                            continue
                        written += len(chunk)
                        if max_bytes and written > max_bytes:
                            logging.warning(f"Attachment '{title}' is over the configured limit of {max_bytes} B. "
                                            f"The file is not downloaded.")
                            break
                        out_file.write(chunk)
        except (RequestException, ApiError) as e:
            # A single failed file must not throw away the rest of a long extraction.
            self._remove_if_exists(target_path)
            logging.warning(f"Cannot download attachment '{title}'. Its metadata is still in the "
                            f"confluence_attachments table. Details: {e}")
            self.attachments_failed += 1
            return False
        except OSError as e:
            # Running out of disk is fatal for the run, but it is the user's quota, not a component bug.
            self._remove_if_exists(target_path)
            raise ConfluenceClientException(
                f"Cannot write attachment '{title}' to disk: {e}. The attachments of a run are kept in the "
                f"working directory until the run ends. Lower 'Maximum file size (MB)', set a media type "
                f"filter, or extract fewer pages per run.")

        if max_bytes and written > max_bytes:
            self._remove_if_exists(target_path)
            self.attachments_skipped += 1
            return False

        if written == 0:
            logging.warning(f"Confluence returned an empty file for attachment '{title}'. "
                            f"The file is not downloaded.")
            self._remove_if_exists(target_path)
            self.attachments_failed += 1
            return False

        self.attachments_downloaded += 1
        return True

    @staticmethod
    def _remove_if_exists(path: str):
        if os.path.exists(path):
            os.remove(path)
