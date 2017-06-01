import os
import json
from globus_sdk import AccessTokenAuthorizer, RefreshTokenAuthorizer, NativeAppAuthClient, TransferClient, TransferData, GlobusConnectionError
def login():
    tok_path = os.path.expanduser('~/.mdf_agent_tokens.json')

    def _read_tokfile():
        tokens = {}
        if os.path.exists(tok_path):
            with open(tok_path) as f:
                tokens = json.load(f)
        return tokens

    def _write_tokfile(new_tokens):
        # We have multiple tokens in our tokens file, but on update we only
        # get the currently updated token, so read current and update with the
        # input tokens
        cur_tokens = _read_tokfile()
        for key in new_tokens:
            cur_tokens[key] = new_tokens[key]
        # deny rwx to Group and World -- don't bother storing the returned old
        # mask value, since we'll never restore it anyway
        # do this on every call to ensure that we're always consistent about it
        os.umask(0o077)
        with open(tok_path, 'w') as f:
            f.write(json.dumps(cur_tokens))

    def _update_tokfile(tokens):
        _write_tokfile(tokens.by_resource_server['transfer.api.globus.org'])

    tokens = _read_tokfile()
    client_id = "1e162bfc-ad52-4014-8844-b82841145fc4"
    native_client = NativeAppAuthClient(client_id, app_name='MDF Agents')

    if not tokens:
        # and do the Native App Grant flow
        native_client.oauth2_start_flow(
            requested_scopes='urn:globus:auth:scope:transfer.api.globus.org:all',
            refresh_tokens=True)
        linkprompt = 'Please login to Globus here'
        print('{0}:\n{1}\n{2}\n{1}\n'
              .format(linkprompt, '-' * len(linkprompt),
                      native_client.oauth2_get_authorize_url()), flush=True)
        auth_code = input(
            'Enter the resulting Authorization Code here').strip()
        tkns = native_client.oauth2_exchange_code_for_tokens(auth_code)
        tokens = tkns.by_resource_server['transfer.api.globus.org']

        _write_tokfile(tokens)

    transfer_tokens = tokens

    transfer_authorizer = RefreshTokenAuthorizer(
        transfer_tokens['refresh_token'], native_client,
        transfer_tokens['access_token'], transfer_tokens['expires_at_seconds'],
        on_refresh=_update_tokfile)

    transfer_client = TransferClient(authorizer=transfer_authorizer)
    return transfer_client
