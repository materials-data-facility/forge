import json
import sys
import os

from globus_sdk import ConfidentialAppAuthClient, AccessTokenAuthorizer, RefreshTokenAuthorizer
from globus_sdk.base import BaseClient, merge_params

VERSION = "0.2.0"

class SearchClient(BaseClient):
    allowed_authorizer_types = [AccessTokenAuthorizer, RefreshTokenAuthorizer]

    def __init__(self, base_url, default_index=None, **kwargs):
        app_name = kwargs.pop('app_name', 'Search Client for MDF v'+VERSION)

        # Auth
        try:
            with open(os.path.join(os.path.dirname(__file__), "globus_login.json")) as cred_file:
                creds = json.load(cred_file)
        except IOError as e:
            sys.exit("Error opening credentials file: " + repr(e))
        app_name = kwargs.pop('app_name', creds.pop("app_name", 'Search Client for MDF v'+VERSION))
        auth_client = ConfidentialAppAuthClient(creds["client_id"], creds["client_secret"])
        token_response = auth_client.oauth2_client_credentials_tokens(requested_scopes=creds["scopes"])
        token = token_response.by_resource_server['search.api.globus.org']["access_token"]
        authorizer = AccessTokenAuthorizer(token)

        BaseClient.__init__(self, "search", app_name=app_name, authorizer=authorizer, **kwargs)
        # base URL lookup will fail, producing None, set it by hand
        self.base_url = base_url
        self._headers['Content-Type'] = 'application/json'
        self.default_index = default_index

    def _resolve_uri(self, base_uri, index):
        index = index or self.default_index
        if not index:
            raise ValueError(
                ('You must either pass an explicit index'
                 'or set a default one at the time that you create '
                 'a SearchClient'))
        return '{}/{}'.format(base_uri, index)

    def search(self, q, limit=None, offset=None, resource_type=None,
               index=None, advanced=None, **params):
        """
        Perform a simple ``GET`` based search.

        Does not support all of the behaviors and parameters of advanced
        searches.

        **Parameters**

          ``q`` (*string*)
            The user-query string. Required for simple searches (and most
            advanced searches).

          ``index`` (*string*)
            Optional unless ``default_index`` was not set.
            The index to query.

          ``limit`` (*int*)
            Optional. The number of results to return.

          ``offset`` (*int*)
            Optional. An offset into the total result set for paging.

          ``resource_type`` (*string*)
            Optional. A resource_type name as defined within the Search
            service.

          ``advanced`` (*bool*)
            Use simple query parsing vs. advanced query syntax when
            interpreting ``q``. Defaults to False.

          ``params``
            Any aditional query params to pass. For internal use only.
        """
        uri = self._resolve_uri('/v1/search', index)
        merge_params(params, q=q, limit=limit, offset=offset,
                     resource_type=resource_type, advanced=advanced)
        return self.get(uri, params=params)

    def structured_search(self, data, index=None, **params):
        """
        Perform a structured, ``POST``-based, search.

        **Parameters**

          ``data`` (*dict*)
            A valid GSearchRequest document to execute.

          ``index`` (*string*)
            Optional unless ``default_index`` was not set.
            The index to query.

          ``advanced`` (*bool*)
            Use simple query parsing vs. advanced query syntax when
            interpreting the query string. Defaults to False.

          ``params``
            Any aditional query params to pass. For internal use only.
        """
        uri = self._resolve_uri('/v1/search', index)
        return self.post(uri, json_body=data, params=params)

    def ingest(self, data, index=None, **params):
        """
        Perform a simple ``POST`` based ingest op.

        **Parameters**

          ``data`` (*dict*)
            A valid GIngest document to index.

          ``index`` (*string*)
            Optional unless ``default_index`` was not set.
            The search index to send data into.

          ``params``
            Any aditional query params to pass. For internal use only.
        """
        uri = self._resolve_uri('/v1/ingest', index)
        return self.post(uri, json_body=data, params=params)
