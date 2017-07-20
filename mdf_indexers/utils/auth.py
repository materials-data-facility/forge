import os
import json

import globus_sdk

from auth_clients import search_client

NATIVE_CLIENT_ID = "98bfc684-977f-4670-8669-71f8337688e4"
DEFAULT_CRED_FILENAME = "globus_login.json"
SCOPES = {
    "transfer": "urn:globus:auth:scope:transfer.api.globus.org:all",
    "search": "urn:globus:auth:scope:search.api.globus.org:search",
    "search_ingest": "urn:globus:auth:scope:search.api.globus.org:all",
    "mdf": "urn:globus:auth:scope:data.materialsdatafacility.org:all" # urn:globus:auth:scope:api.materialsdatafacility.org:all"
    }

# Login to Globus services
# Args:
#   services: services to login to
#   credentials: A string filename, string JSON, or dictionary with credential information
#       name: name of client
#       services: list of services to auth for (default [])
#       Service-specific fields:
#       Search:
#           index: The default index
#   clear_old_tokens: delete old token file if it exists, forcing user to re-login
def login(*services, credentials=None, clear_old_tokens=False, **kwargs):

    def _get_tokens(client, scopes, app_name, force_refresh=False):
        token_path = os.path.expanduser("~/." + app_name + "_tokens.json")
        if force_refresh:
            if os.path.exists(token_path):
                os.remove(token_path)
        if os.path.exists(token_path):
            with open(token_path, "r") as tf:
                tokens = json.load(tf)
        else:
            client.oauth2_start_flow(requested_scopes=scopes, refresh_tokens=True)
            authorize_url = client.oauth2_get_authorize_url()

            print("It looks like this is the first time you're accessing this client.\nPlease log in to Globus at this link:\n", authorize_url)
            auth_code = input("Copy and paste the authorization code here: ").strip()
            print("Thanks!")

            token_response = client.oauth2_exchange_code_for_tokens(auth_code)
            tokens = token_response.by_resource_server

            os.umask(0o077)
            with open(token_path, "w") as tf:
                json.dump(tokens, tf)

        return tokens

    if type(credentials) is str:
        try:
            with open(credentials) as cred_file:
                creds = json.load(cred_file)
        except IOError:
            try:
                creds = json.loads(credentials)
            except JSONDecodeError:
                raise ValueError("Credential string unreadable")
    elif type(credentials) is dict:
        creds = credentials
    else:
        try:
            with open(os.path.join(os.getcwd(), DEFAULT_CRED_FILENAME)) as cred_file:
                creds = json.load(cred_file)
        except IOError:
            raise ValueError("Credentials/configuration must be passed as a filename string, JSON string, or dictionary, or provided in " + DEFAULT_CRED_FILENAME + ".")

    native_client = globus_sdk.NativeAppAuthClient(NATIVE_CLIENT_ID, app_name=creds["app_name"])

    servs = []
    for serv in list(services) + creds.get("services", []):
        serv = serv.lower().strip()
        if type(serv) is str:
            servs += serv.split(" ")
        else:
            servs += list(serv)
    scopes = " ".join([SCOPES[sc] for sc in servs])

    all_tokens = _get_tokens(native_client, scopes, creds["app_name"], force_refresh=clear_old_tokens)

    clients = {}
    if "transfer" in servs:
        transfer_authorizer = globus_sdk.RefreshTokenAuthorizer(all_tokens["transfer.api.globus.org"]["refresh_token"], native_client)
        clients["transfer"] = globus_sdk.TransferClient(authorizer=transfer_authorizer)
    if "search_ingest" in servs:
        ingest_authorizer = globus_sdk.RefreshTokenAuthorizer(all_tokens["search.api.globus.org"]["refresh_token"], native_client)
        clients["search"] = search_client.SearchClient(default_index=(creds.get("index", None) or kwargs.get("index", None)), authorizer=ingest_authorizer)
    elif "search" in servs:
        search_authorizer = globus_sdk.RefreshTokenAuthorizer(all_tokens["search.api.globus.org"]["refresh_token"], native_client)
        clients["search"] = search_client.SearchClient(default_index=(creds.get("index", None) or kwargs.get("index", None)), authorizer=search_authorizer)
    if "mdf" in servs:
        mdf_authorizer = globus_sdk.RefreshTokenAuthorizer(all_tokens["data.materialsdatafacility.org"]["refresh_token"], native_client)
        clients["mdf"] = mdf_authorizer

    return clients


# Auth for confidential clients (that have their own login credentials)
# Arg credentials must be a dict, JSON string, or path to JSON document (default "./globus_login.json") containing:
#   client_id: The Globus ID of the client
#   client_secret: The secret of the client
#   services: The services to auth for
#   index: For Globus Search only, the default index
def confidential_login(credentials=None):
    # Read credentials
    if type(credentials) is str:
        try:
            with open(credentials) as cred_file:
                creds = json.load(cred_file)
        except IOError:
            try:
                creds = json.loads(credentials)
            except JSONDecodeError:
                raise ValueError("Credentials unreadable or missing")
    elif type(credentials) is dict:
        creds = credentials
    else:
        try:
            with open(os.path.join(os.getcwd(), DEFAULT_CRED_FILENAME)) as cred_file:
                creds = json.load(cred_file)
        except IOError:
            raise ValueError("Credentials/configuration must be passed as a filename string, JSON string, or dictionary, or provided in " + DEFAULT_CRED_FILENAME + ".")

    conf_client = globus_sdk.ConfidentialAppAuthClient(creds["client_id"], creds["client_secret"])
    servs = []
    for serv in creds["services"]:
        serv = serv.lower().strip()
        if type(serv) is str:
            servs += serv.split(" ")
        else:
            servs += list(serv)
    scopes = " ".join([SCOPES[sc] for sc in servs])

    conf_authorizer = globus_sdk.ClientCredentialsAuthorizer(conf_client, scopes)

    clients = {}
    if "transfer" in servs:
        clients["transfer"] = globus_sdk.TransferClient(authorizer=conf_authorizer)
    if "search_ingest" in servs:
        clients["search"] = search_client.SearchClient(default_index=creds.get("index", None), authorizer=conf_authorizer)
    elif "search" in servs:
        clients["search"] = search_client.SearchClient(default_index=creds.get("index", None), authorizer=conf_authorizer)
    if "mdf" in servs:
        clients["mdf"] = conf_authorizer

    return clients



