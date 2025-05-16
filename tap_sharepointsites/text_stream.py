"""Stream type classes for tap-sharepointsites."""

import datetime
import os
import re
import tempfile
import typing as t

import requests
import textract
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from singer_sdk import metrics
from singer_sdk import typing as th

from tap_sharepointsites.client import sharepointsitesStream


class TextStream(sharepointsitesStream):
    """Define custom stream."""

    records_jsonpath = "$.value[*]"
    replication_key = "lastModifiedDateTime"
    primary_keys = None  # ["_sdc_source_file"]

    # schema_filepath = SCHEMAS_DIR / "files.json"

    def __init__(self, *args, **kwargs):
        """Init CSVStram."""
        # cache text_config so we dont need to go iterating the config list again later

        self.text_config = kwargs.pop("text_config")
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
        folder = self.text_config.get("folder")

        if not folder:
            base_url = f"/drives/{drive_id}/root/children"
        else:
            base_url = f"/drives/{drive_id}/root:/{folder}:/children"

        return base_url

    def list_all_files(self, headers=None):
        """List all files in the drive."""
        drive_id = self.get_drive_id()
        folder = self.text_config.get("folder")

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

    def parse_response(self, response: requests.Response, context) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records."""
        resp_values = response.json()["value"]
        files_since = (
            self.get_starting_replication_key_value(context) or "1900-01-01T00:00:00Z"
        )

        for record in resp_values:
            if (
                "file" in record.keys()
                and re.match(self.text_config["file_pattern"], record["name"])
                and record["lastModifiedDateTime"] > files_since
            ):

                file = self.get_file_for_row(record, text=False)

                with tempfile.NamedTemporaryFile(suffix=record["name"], delete=False) as tmpfile:
                    tmpfile.write(file)
                    tmpfile.flush()
                    tmpfile.seek(0)
                    text = textract.process(tmpfile.name)

                row = {
                    "content": text.decode("utf-8"),
                    "metadata": {"source": record["name"]},
                    "_sdc_source_file": record["name"],
                    "_sdc_loaded_at": str(datetime.datetime.utcnow()),
                    "lastModifiedDateTime": record["lastModifiedDateTime"],
                }

                try:
                    os.remove(tmpfile.name)
                except Exception as e:
                    print(f"Error cleaning up temporary file: {e}")

                yield row

    schema = th.PropertiesList(
        th.Property(
            "content",
            th.StringType,
        ),
        th.Property(
            "metadata",
            th.ObjectType(
                th.Property(
                    "source",
                    th.StringType,
                ),
            ),
        ),
        th.Property(
            "_sdc_source_file",
            th.StringType,
            description="Filename",
        ),
        th.Property(
            "_sdc_loaded_at",
            th.StringType,
            description="Loaded at timestamp",
        ),
        th.Property(
            "lastModifiedDateTime",
            th.StringType,
            description="The last time the file was updated",
        ),
    ).to_dict()

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
