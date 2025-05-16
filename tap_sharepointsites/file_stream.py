"""Stream type classes for tap-sharepointsites."""


from datetime import datetime, timezone
import re
import typing as t
from functools import cached_property

import requests
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from singer_sdk import metrics

from tap_sharepointsites.client import sharepointsitesStream
from tap_sharepointsites.file_handlers.csv_handler import CSVHandler
from tap_sharepointsites.file_handlers.excel_handler import ExcelHandler
from tap_sharepointsites.utils import snakecase


class FilesStream(sharepointsitesStream):
    """Define custom stream."""

    records_jsonpath = "$.value[*]"
    # replication_key = "lastModifiedDateTime" ## This is not a good replication key, MS Graph API does not always update it, run a full refresh every time
    primary_keys = ["_sdc_source_file", "_sdc_row_num"]

    # schema_filepath = SCHEMAS_DIR / "files.json"

    def __init__(self, *args, **kwargs):
        """Init CSVStram."""
        # cache file_config so we dont need to go iterating the config list again later

        self.file_config = kwargs.pop("file_config")
        super().__init__(*args, **kwargs)

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        url = "https://graph.microsoft.com/v1.0"

        return url


    @property
    def header(self):
        """Run header function."""
        return self.http_headers

    @property
    def path(self) -> str:
        """Return the API endpoint path, configurable via tap settings."""
        drive_id = self.get_drive_id()
        folder = self.file_config.get("folder")

        if not folder:
            base_url = f"/drives/{drive_id}/root/children"
        else:
            base_url = f"/drives/{drive_id}/root:/{folder}:/children"

        return base_url

    def list_all_files(self, headers=None):
        """List all files in the drive."""
        drive_id = self.get_drive_id()
        folder = self.file_config.get("folder")

        if not folder:
            base_url = (
                f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children"
            )
        else:
            base_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder}:/children"

        while base_url:
            response = requests.get(base_url, headers=headers, auth=self.authenticator)
            response.raise_for_status()
            data = response.json()
            for item in data["value"]:
                if "file" in item:
                    yield item

            base_url = data.get("@odata.nextLink")

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records."""
        resp_values = response.json()["value"]
        files_since = (
            self.get_starting_timestamp(self.context) or "1900-01-01T00:00:00Z"
        )

        for record in resp_values:
            if (
                "file" in record.keys()
                and re.match(self.file_config["file_pattern"], record["name"])
                and record["lastModifiedDateTime"] > files_since
            ):

                if self.file_config["file_type"] == "csv":
                    file = self.get_file_for_row(record)
                    dr = CSVHandler(
                        file, self.file_config.get("delimiter", ",")
                    ).get_dictreader()

                elif self.file_config["file_type"] == "excel":
                    file = self.get_file_for_row(record, text=False)
                    sheet_name = self.file_config.get("sheet_name", "Sheet1")
                    min_row = self.file_config.get("min_row", None)
                    max_row = self.file_config.get("max_row", None)
                    min_col = self.file_config.get("min_col", None)
                    max_col = self.file_config.get("max_col", None)
                    dr = ExcelHandler(
                        file, sheet_name, min_row, max_row, min_col, max_col
                    ).get_row_iterator()
                else:
                    filetype_name = self.file_config.get("file_type", "unknown")
                    raise Exception(f"File type { filetype_name } not supported (yet)")

                for i, row in enumerate(dr):

                    if self.file_config.get("clean_colnames", True):
                        row = {snakecase(k): v for k, v in row.items()}

                    row.update(
                        {
                            "_sdc_source_file": record["name"],
                            "_sdc_row_num": str(i),
                            "_sdc_loaded_at": str(datetime.now(timezone.utc)),
                            "lastModifiedDateTime": record["lastModifiedDateTime"],
                        }
                    )

                    yield row

    @cached_property
    def schema(self):
        """Create a schema for a *SV file."""
        all_files = self.list_all_files(headers=self.header)

        for file in all_files:
            if re.match(self.file_config["file_pattern"], file["name"]):

                if self.file_config["file_type"] == "csv":
                    file = self.get_file_for_row(file)
                    dr = CSVHandler(
                        file, self.file_config.get("delimiter", ",")
                    ).get_dictreader()

                elif self.file_config["file_type"] == "excel":
                    file = self.get_file_for_row(file, text=False)
                    sheet_name = self.file_config.get("sheet_name", "Sheet1")
                    min_row = self.file_config.get("min_row", None)
                    max_row = self.file_config.get("max_row", None)
                    min_col = self.file_config.get("min_col", None)
                    max_col = self.file_config.get("max_col", None)
                    dr = ExcelHandler(
                        file, sheet_name, min_row, max_row, min_col, max_col
                    )

                properties = {}

                fieldnames = [name for name in dr.fieldnames]

                if self.file_config.get("clean_colnames", True):
                    fieldnames = [snakecase(name) for name in fieldnames]

                extra_cols = [
                    "_sdc_source_file",
                    "_sdc_row_num",
                    "_sdc_loaded_at",
                    "lastModifiedDateTime",
                ]

                for field in fieldnames + extra_cols:
                    properties.update({field: {"type": ["null", "string"]}})

                return {"properties": properties}

        else:
            raise Exception("There is no spoon. Nor files, for that matter.")

    def get_drive_id(self):
        """Get drives in the sharepoint site."""
        drive = requests.get(f'{self.config["api_url"]}drive', headers=self.header, auth=self.authenticator)

        if not drive.ok:
            raise Exception(f"Error getting drive: {drive.status_code}: {drive.text}")
        return drive.json()["id"]

    def get_file_for_row(self, row_data, text=True):
        """Get the file for a row."""
        file = requests.get(
            row_data["@microsoft.graph.downloadUrl"], headers=self.header, auth=self.authenticator
        )
        file.raise_for_status()

        if text:
            return file.text
        else:
            return file.content

    def get_properties(self, fieldnames) -> dict:
        """Get a list of properties for a *SV file, to be used in creating a schema."""
        properties = {}

        if fieldnames is None:
            msg = (
                "Column names could not be read because they don't exist. Try "
                "manually specifying them using 'delimited_override_headers'."
            )
            raise RuntimeError(msg)
        for field in fieldnames:
            properties.update({field: {"type": ["null", "string"]}})

        return properties
