import csv
import logging
from datetime import datetime

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from client.confluence_client import ConfluenceClient

# configuration variables
KEY_USERNAME = 'username'
KEY_URL = 'url'
KEY_API_TOKEN = '#api_token'
KEY_BEAUTIFY = 'beautify'
KEY_INCREMENTAL = 'incremental'

REQUIRED_PARAMETERS = [KEY_USERNAME, KEY_URL, KEY_API_TOKEN]


class Component(ComponentBase):

    def __init__(self):
        super().__init__()
        self.current_time = datetime.utcnow()
        self.last_run = "2000-01-01T00:00:00.000Z"

    def run(self):

        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        params = self.configuration.parameters

        url = params.get(KEY_URL)
        username = params.get(KEY_USERNAME)
        token = params.get(KEY_API_TOKEN)
        incremental = params.get(KEY_INCREMENTAL, False)

        if incremental:
            statefile = self.get_state_file()
            if statefile.get("last_run"):
                self.last_run = statefile.get("last_run")
                logging.info(f"Using last_run from statefile: {self.last_run}")
            else:
                logging.info(f"No last_run found in statefile, using default timestamp: {self.last_run}")

        table_out = self.create_out_table_definition("confluence_pages", primary_key=["id"],
                                                     incremental=incremental)

        client = ConfluenceClient(url, username, token)
        fieldnames = ["id", "created_date", "last_updated_date", "title", "creator", "last_modifier", "url", "space",
                      "text"]

        with open(table_out.full_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for page in client.get_confluence_pages(timestamp_from=self.last_run):
                writer.writerow(page)

        self.write_manifest(table_out)
        self.write_state_file({"last_run": self.current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'})


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
