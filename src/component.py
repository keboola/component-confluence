import csv
import logging
import os
from datetime import datetime

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from client.confluence_client import (ATTACHMENT_FIELDS, ConfluenceClient, ConfluenceClientException,
                                      matches_media_type, sanitize_file_name)

# configuration variables
KEY_USERNAME = 'username'
KEY_URL = 'url'
KEY_API_TOKEN = '#api_token'
KEY_BEAUTIFY = 'beautify'
KEY_INCREMENTAL = 'incremental'
KEY_GROUP_DESTINATION_OPTIONS = 'destination_options'

KEY_GROUP_ATTACHMENTS = 'attachments'
KEY_ATTACHMENTS_ENABLED = 'enabled'
KEY_ATTACHMENTS_DOWNLOAD_FILES = 'download_files'
KEY_ATTACHMENTS_MAX_FILE_SIZE_MB = 'max_file_size_mb'
KEY_ATTACHMENTS_MEDIA_TYPES = 'media_types'

DEFAULT_MAX_FILE_SIZE_MB = 50
ATTACHMENT_FILE_TAG = 'confluence-attachment'

REQUIRED_PARAMETERS = [KEY_USERNAME, KEY_URL, KEY_API_TOKEN]


class Component(ComponentBase):

    def __init__(self):
        super().__init__()
        self.current_time = datetime.utcnow()
        self.last_run = "2000-01-01T00:00:00.000Z"

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        url, username, token, beautify, incremental = self._init_parameters()
        attachment_options = self._init_attachment_options()
        self.setup_last_run(incremental)

        table_out = self.create_out_table_definition("confluence_pages", primary_key=["id"], incremental=incremental)
        client = ConfluenceClient(url, username, token)

        attachments_out = None
        if attachment_options[KEY_ATTACHMENTS_ENABLED]:
            attachments_out = self.create_out_table_definition(
                "confluence_attachments", primary_key=["id"], incremental=incremental
            )

        self.write_confluence_data(client, beautify, table_out, attachments_out, attachment_options)

        self.write_manifest(table_out)
        if attachments_out:
            self.write_manifest(attachments_out)
        self.write_state_file({"last_run": self.current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'})
        logging.info(f"The component has fetched {client.fetched_total} new Confluence documents.")
        if attachments_out:
            logging.info(f"The component has fetched metadata of {client.attachments_total} attachments. "
                         f"Downloaded files: {client.attachments_downloaded}, "
                         f"skipped files: {client.attachments_skipped}.")
            if client.attachments_failed:
                logging.warning(f"{client.attachments_failed} attachment files could not be downloaded. "
                                f"Their metadata is in the confluence_attachments table, with an empty "
                                f"file_name. See the warnings above for the reason.")
            if client.pages_with_unreadable_attachments:
                logging.warning(f"The attachments of {client.pages_with_unreadable_attachments} pages could "
                                f"not be listed. This is usually a permission problem. See the warnings above.")

    def _init_parameters(self):
        params = self.configuration.parameters
        url = params.get(KEY_URL)
        username = params.get(KEY_USERNAME)
        token = params.get(KEY_API_TOKEN)
        destination_options = params.get(KEY_GROUP_DESTINATION_OPTIONS, {})
        beautify = destination_options.get(KEY_BEAUTIFY, False)
        incremental = destination_options.get(KEY_INCREMENTAL, False)
        return url, username, token, beautify, incremental

    def _init_attachment_options(self):
        """Read the optional `attachments` group. Attachments are off unless the user turns them on."""
        options = self.configuration.parameters.get(KEY_GROUP_ATTACHMENTS) or {}
        enabled = bool(options.get(KEY_ATTACHMENTS_ENABLED, False))
        download_files = bool(options.get(KEY_ATTACHMENTS_DOWNLOAD_FILES, False))

        max_file_size_mb = options.get(KEY_ATTACHMENTS_MAX_FILE_SIZE_MB, DEFAULT_MAX_FILE_SIZE_MB)
        if max_file_size_mb is None or max_file_size_mb == "":
            max_file_size_mb = DEFAULT_MAX_FILE_SIZE_MB
        try:
            # A fractional value is kept, so that 0.5 means 512 kB and not "no limit".
            max_file_size_mb = float(max_file_size_mb)
        except (TypeError, ValueError):
            raise UserException(f"The parameter '{KEY_ATTACHMENTS_MAX_FILE_SIZE_MB}' must be a number, "
                                f"but it is '{max_file_size_mb}'.")
        if max_file_size_mb < 0:
            raise UserException(f"The parameter '{KEY_ATTACHMENTS_MAX_FILE_SIZE_MB}' must not be negative.")

        raw_media_types = options.get(KEY_ATTACHMENTS_MEDIA_TYPES) or ""
        if isinstance(raw_media_types, list):
            media_types = [str(item).strip() for item in raw_media_types if str(item).strip()]
        else:
            media_types = [item.strip() for item in str(raw_media_types).split(",") if item.strip()]

        return {
            KEY_ATTACHMENTS_ENABLED: enabled,
            KEY_ATTACHMENTS_DOWNLOAD_FILES: download_files,
            KEY_ATTACHMENTS_MAX_FILE_SIZE_MB: max_file_size_mb,
            KEY_ATTACHMENTS_MEDIA_TYPES: media_types,
        }

    def setup_last_run(self, incremental):
        if incremental:
            statefile = self.get_state_file()
            if statefile.get("last_run"):
                self.last_run = statefile.get("last_run")
                logging.info(f"Using last_run from statefile: {self.last_run}")
            else:
                logging.info(f"No last_run found in statefile, using default timestamp: {self.last_run}")

    def write_confluence_data(self, client, beautify, table_out, attachments_out=None, attachment_options=None):
        attachment_options = attachment_options or {}
        fieldnames = ["id", "created_date", "last_updated_date", "title", "creator", "last_modifier", "url", "space",
                      "text"]

        with open(table_out.full_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            attachments_file = None
            attachments_writer = None
            try:
                if attachments_out:
                    attachments_file = open(attachments_out.full_path, 'w', newline='', encoding='utf-8')
                    attachments_writer = csv.DictWriter(attachments_file, fieldnames=ATTACHMENT_FIELDS)
                    attachments_writer.writeheader()

                for page_id, space, page in client.iter_pages(timestamp_from=self.last_run, beautify=beautify):
                    writer.writerow(page)
                    if attachments_writer:
                        self._write_page_attachments(client, attachments_writer, page_id, page.get("id", ""),
                                                     space, attachment_options)
            except ConfluenceClientException as e:
                raise UserException(f"Cannot fetch data from Confluence, error: {e}")
            finally:
                if attachments_file:
                    attachments_file.close()

    def _write_page_attachments(self, client, attachments_writer, page_id, page_key, space, attachment_options):
        download_files = attachment_options.get(KEY_ATTACHMENTS_DOWNLOAD_FILES, False)
        max_bytes = int(attachment_options.get(KEY_ATTACHMENTS_MAX_FILE_SIZE_MB,
                                               DEFAULT_MAX_FILE_SIZE_MB) * 1024 * 1024)
        media_types = attachment_options.get(KEY_ATTACHMENTS_MEDIA_TYPES) or None

        for row in client.get_page_attachments(page_id, page_key, space):
            # Every attachment is written to the table. The media type filter only limits the files.
            if download_files and matches_media_type(row.get("media_type", ""), media_types):
                file_name = sanitize_file_name(row.get("id", ""), row.get("title", ""))
                file_def = self.create_out_file_definition(
                    file_name, tags=self._attachment_file_tags(), is_permanent=True)
                if client.download_attachment(row, file_def.full_path, max_bytes=max_bytes):
                    row["file_name"] = file_name
                    self.write_manifest(file_def)
                else:
                    self._remove_file_if_exists(file_def.full_path)
            attachments_writer.writerow(row)

    def _attachment_file_tags(self):
        """Tag every file so that one configuration and one run can be told apart in File Storage."""
        environment = self.environment_variables
        tags = [ATTACHMENT_FILE_TAG]
        for prefix, value in (("componentId", environment.component_id), ("configId", environment.config_id),
                              ("runId", environment.run_id)):
            if value:
                tags.append(f"{prefix}-{value}")
        return tags

    @staticmethod
    def _remove_file_if_exists(path):
        if os.path.exists(path):
            os.remove(path)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
