from globus_sdk import AccessTokenAuthorizer, RefreshTokenAuthorizer
from globus_sdk.base import BaseClient


class DataSearchClient(BaseClient):
    allowed_authorizer_types = [AccessTokenAuthorizer, RefreshTokenAuthorizer]

    def __init__(self, base_url, **kwargs):
        app_name = kwargs.pop('app_name', 'DataSearch Client v0.1')
        BaseClient.__init__(self, "datasearch", app_name=app_name, **kwargs)
        # base URL lookup will fail, producing None, set it by hand
        self.base_url = base_url
        self._headers['Content-Type'] = 'application/json'

    def search(self, **params):
        return self.get('/v1/search', params=params)

    def ingest(self, data, **params):
        return self.post('/v1/ingest', json_body=data, params=params)
