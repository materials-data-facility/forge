import json

from globus_sdk import AccessTokenAuthorizer, RefreshTokenAuthorizer
from globus_sdk.base import BaseClient, merge_params


class DataSearchClient(BaseClient):
    allowed_authorizer_types = [AccessTokenAuthorizer, RefreshTokenAuthorizer]

    def __init__(self, base_url, default_search_domain=None, **kwargs):
        app_name = kwargs.pop('app_name', 'DataSearch Client v0.1')
        BaseClient.__init__(self, "datasearch", app_name=app_name, **kwargs)
        # base URL lookup will fail, producing None, set it by hand
        self.base_url = base_url
        self._headers['Content-Type'] = 'application/json'
        self.default_search_domain = default_search_domain

    def _resolve_uri(self, base_uri, domain):
        domain = domain or self.default_search_domain
        if not domain:
            raise ValueError(
                ('You must either pass an explicit search_domain '
                 'or set a default one at the time that you create '
                 'a DataSearchClient'))
        return '{}/{}'.format(base_uri, domain)

    def search(self, q, limit=None, offset=None, resource_type=None,
               search_domain=None, **params):
        """
        Perform a simple ``GET`` based search.

        Does not support all of the behaviors and parameters of advanced
        searches.

        **Parameters**

          ``q`` (*string*)
            The user-query string. Required for simple searches (and most
            advanced searches).

          ``search_domain`` (*string*)
            Optional unless ``default_search_domain`` was not set.
            The domain to query.

          ``limit`` (*int*)
            Optional. The number of results to return.

          ``offset`` (*int*)
            Optional. An offset into the total result set for paging.

          ``resource_type`` (*string*)
            Optional. A resource_type name as defined within the DataSearch
            service.

          ``params``
            Any aditional query params to pass. For internal use only.
        """
        uri = self._resolve_uri('/v1/search', search_domain)
        merge_params(params, q=q, limit=limit, offset=offset,
                     resource_type=resource_type)
        return self.get(uri, params=params)

    def advanced_search(self, data, search_domain=None, **params):
        """
        Perform an advanced, ``POST``-based, search.

        **Parameters**

          ``data`` (*dict*)
            A valid GSearchRequest document to execute.

          ``search_domain`` (*string*)
            Optional unless ``default_search_domain`` was not set.
            The domain to query.

          ``params``
            Any aditional query params to pass. For internal use only.
        """
        uri = self._resolve_uri('/v1/search', search_domain)
        return self.post(uri, json_body=data, params=params)

    def ingest(self, data, search_domain=None, **params):
        """
        Perform a simple ``POST`` based ingest op.

        **Parameters**

          ``data`` (*dict*)
            A valid GIngest document to index.

          ``search_domain`` (*string*)
            Optional unless ``default_search_domain`` was not set.
            The search domain to send data into.

          ``params``
            Any aditional query params to pass. For internal use only.
        """
        uri = self._resolve_uri('/v1/ingest', search_domain)
        return self.post(uri, json_body=data, params=params)
