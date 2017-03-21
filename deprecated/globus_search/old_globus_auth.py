import os
import os.path
import json
import globus_sdk
import six
from six.moves import input

#from globus_datasearch_client.client import DataSearchClient
from globus_client import DataSearchClient

def prompt(s):
    print(s + ': ')
    return input().strip()


def _load_auth_client():
    client_id = 'ee929f5c-ed08-4320-b22f-c1aa5229e490'
    return globus_sdk.NativeAppAuthClient(
        client_id, app_name='DataSearch Client v0.1')


def _interactive_login():
    native_client = _load_auth_client()

    # and do the Native App Grant flow
    native_client.oauth2_start_flow(
        requested_scopes='urn:globus:auth:scope:datasearch.api.globus.org:all',
        refresh_tokens=True)
    linkprompt = 'Please login to Globus here'
    print('{0}:\n{1}\n{2}\n{1}\n'
          .format(linkprompt, '-' * len(linkprompt),
                  native_client.oauth2_get_authorize_url()))
    auth_code = prompt('Enter the resulting Authorization Code here').strip()
    tkn = native_client.oauth2_exchange_code_for_tokens(auth_code)

    return tkn.by_resource_server['datasearch.api.globus.org']


def login(base_url):
    tok_path = os.path.expanduser('~/.globus_datasearch_client_tokens.json')

    def _write_tokfile(tokens):
        # deny rwx to Group and World -- don't bother storing the returned old
        # mask value, since we'll never restore it anyway
        # do this on every call to ensure that we're always consistent about it
        os.umask(0o077)
        with open(tok_path, 'w') as f:
            f.write(json.dumps(tokens))

    def _update_tokfile(token_response):
        tokens = token_response.by_resource_server['datasearch.api.globus.org']
        _write_tokfile(tokens)

    if os.path.exists(tok_path):
        with open(tok_path) as f:
            tokens = json.load(f)
    else:
        tokens = _interactive_login()
        _write_tokfile(tokens)

    authorizer = globus_sdk.RefreshTokenAuthorizer(
        tokens['refresh_token'], _load_auth_client(),
        tokens['access_token'], tokens['expires_at_seconds'],
        on_refresh=_update_tokfile)

    return DataSearchClient(base_url, authorizer=authorizer)
