"""REST client handling, including sharepointsitesStream base class."""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional
from urllib.parse import parse_qsl

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseHATEOASPaginator
from singer_sdk.streams.rest import RESTStream

from tap_sharepointsites.auth import GraphAuthenticator

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class GraphHATEOASPaginator(BaseHATEOASPaginator):
    """Basic paginator."""

    def __init__(self):
        """Initialize the paginator."""
        super().__init__()

    def get_next_url(self, response):
        """Return the URL for next page."""
        return response.json().get("@odata.nextLink")


class sharepointsitesStream(RESTStream):
    """sharepointsites stream class."""
    
    def __init__(self, **kwargs):
        """Initialize stream class."""
        self._authenticator: Optional[GraphAuthenticator] = None
        super().__init__(**kwargs)

    @property
    def authenticator(self) -> GraphAuthenticator:
        """Return a new authenticator object."""
        if self._authenticator is None:
            self._authenticator = GraphAuthenticator.create_for_stream(self)
            
        return self._authenticator

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")
        return headers

    def get_new_paginator(self):
        """Return paginator class."""
        return GraphHATEOASPaginator()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return next page link or None."""
        if next_page_token:
            return dict(parse_qsl(next_page_token.query))
        return {}

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records."""
        # TODO: Parse response body and return a set of records.
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        row["_sdc_loaded_at"] = datetime.utcnow()
        return row
