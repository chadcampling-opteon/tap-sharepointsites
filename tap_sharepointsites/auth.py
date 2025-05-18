"""REST client handling, including sharepointsitesStream base class."""

import logging
from datetime import datetime, timezone

import typing as t

import requests
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from singer_sdk.authenticators import APIAuthenticatorBase, SingletonMeta
from singer_sdk.streams.rest import _HTTPStream

logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.WARNING)

class GraphAuthenticator(APIAuthenticatorBase, metaclass=SingletonMeta):
    """API Authenticator for OAuth 2.0 flows."""

    def __init__(
        self,
        stream: _HTTPStream,
    ) -> None:
        """Create a new authenticator.

        Args:
            stream: The stream instance to use with this authenticator.
            auth_endpoint: The OAuth 2.0 authorization endpoint.
            oauth_scopes: A comma-separated list of OAuth scopes.
            default_expiration: Default token expiry in seconds.
            oauth_headers: An optional dict of headers required to get a token.
        """
        super().__init__(stream=stream)

        # Initialize internal tracking attributes
        self.access_token: str | None = None
        self.last_refreshed: datetime.datetime | None = None
        self.expires_in: int | None = None

    def authenticate_request(
        self,
        request: requests.PreparedRequest,
    ) -> requests.PreparedRequest:
        """Authenticate an OAuth request.

        Args:
            request: A :class:`requests.PreparedRequest` object.

        Returns:
            The authenticated request object.
        """
        if not self.is_token_valid():
            self.update_access_token()

        self.auth_headers["Authorization"] = f"Bearer {self.access_token}"
        return super().authenticate_request(request)


    @property
    def client_id(self) -> str | None:
        """Get client ID string to be used in authentication.

        Returns:
            Optional client secret from stream config if it has been set.
        """
        return self.config.get("client_id") if self.config else None

    def is_token_valid(self) -> bool:
        """Check if token is valid.

        Returns:
            True if the token is valid (fresh).
        """
        if self.last_refreshed is None:
            return False
        if not self.expires_in:
            return True
        return self.expires_in > (datetime.now(timezone.utc) - self.last_refreshed).total_seconds()

    # Authentication and refresh
    def update_access_token(self) -> None:
        """Update `access_token` along with: `last_refreshed` and `expires_in`.

        Raises:
            RuntimeError: When OAuth login fails.
        """
        request_time = datetime.now(timezone.utc)
         
        ad_scope = "https://graph.microsoft.com/.default"
        
        try:
            if self.client_id:
                creds = ManagedIdentityCredential(client_id=self.config["client_id"])
                token = creds.get_token(ad_scope)
            else:
                creds = DefaultAzureCredential()
                token = creds.get_token(ad_scope)
                
        except Exception as ex:
            msg = f"Failed Azure Graph login. {ex}"
            raise RuntimeError(msg) from ex

        self.logger.info("Graph authorization attempt was successful.")

        self.access_token = token.token
        expiration = token.expires_on
        self.expires_in = int(expiration) if expiration else None
        if self.expires_in is None:
            self.logger.debug(
                "No expires_in received in get_token response and no "
                "default_expiration set. Token will be treated as if it never "
                "expires.",
            )
        self.last_refreshed = request_time
        
    @classmethod
    def create_for_stream(
        cls: t.Type["GraphAuthenticator"],
        stream: _HTTPStream
    ) -> "GraphAuthenticator":
        """Create an Authenticator object specific to the Stream class."""

        return cls(stream=stream)
