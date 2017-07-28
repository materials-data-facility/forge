import gzip
import json
import os
import re
import tarfile
import zipfile

import globus_sdk
from globus_sdk.base import BaseClient, merge_params
from globus_sdk.response import GlobusHTTPResponse
from tqdm import tqdm



###################################################
##  Authentication utilities
###################################################

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
    NATIVE_CLIENT_ID = "98bfc684-977f-4670-8669-71f8337688e4"
    DEFAULT_CRED_FILENAME = "globus_login.json"
    SCOPES = {
        "transfer": "urn:globus:auth:scope:transfer.api.globus.org:all",
        "search": "urn:globus:auth:scope:search.api.globus.org:search",
        "search_ingest": "urn:globus:auth:scope:search.api.globus.org:all",
        "mdf": "urn:globus:auth:scope:data.materialsdatafacility.org:all" # urn:globus:auth:scope:api.materialsdatafacility.org:all"
        }

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
        clients["search"] = SearchClient(default_index=(creds.get("index", None) or kwargs.get("index", None)), authorizer=ingest_authorizer)
    elif "search" in servs:
        search_authorizer = globus_sdk.RefreshTokenAuthorizer(all_tokens["search.api.globus.org"]["refresh_token"], native_client)
        clients["search"] = SearchClient(default_index=(creds.get("index", None) or kwargs.get("index", None)), authorizer=search_authorizer)
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
    DEFAULT_CRED_FILENAME = "globus_login.json"
    SCOPES = {
        "transfer": "urn:globus:auth:scope:transfer.api.globus.org:all",
        "search": "urn:globus:auth:scope:search.api.globus.org:search",
        "search_ingest": "urn:globus:auth:scope:search.api.globus.org:all",
        "mdf": "urn:globus:auth:scope:data.materialsdatafacility.org:all" # urn:globus:auth:scope:api.materialsdatafacility.org:all"
        }
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
        clients["search"] = SearchClient(default_index=creds.get("index", None), authorizer=conf_authorizer)
    elif "search" in servs:
        clients["search"] = SearchClient(default_index=creds.get("index", None), authorizer=conf_authorizer)
    if "mdf" in servs:
        clients["mdf"] = conf_authorizer

    return clients



###################################################
##  File utilities
###################################################

#Finds files inside a given directory (recursively) and returns path and filename info.
#Arguments:
#   root: Path to the first dir to start with. Required.
#   file_pattern: regex string to search for. Default is None, which matches all files.
#   verbose: Should the script print status messages? Default False.
def find_files(root, file_pattern=None, verbose=False):
    # Add separator to end of root if not already supplied
    root += os.sep if root[-1:] != os.sep else ""
    dir_list = []
    for path, dirs, files in tqdm(os.walk(root), desc="Finding files", disable= not verbose):
        for one_file in files:
            if not file_pattern or re.search(file_pattern, one_file):  # Only care about dirs with desired data
                dir_list.append({
                    "path": path,
                    "filename": one_file,
                    "no_root_path": path.replace(root, "")
                    })
    return dir_list


#Uncompresses all tar, zip, and gzip archives in a directory (searched recursively). Very slow.
def uncompress_tree(root, verbose=False):
    for path, dirs, files in tqdm(os.walk(root), desc="Uncompressing files", disable= not verbose):
        for single_file in files:
            abs_path = os.path.join(path, single_file)
            if tarfile.is_tarfile(abs_path):
                tar = tarfile.open(abs_path)
                tar.extractall()
                tar.close()
            elif zipfile.is_zipfile(abs_path):
                z = zipfile.ZipFile(abs_path)
                z.extractall()
                z.close()
            else:
                try:
                    with gzip.open(abs_path) as gz:
                        file_data = gz.read()
                        with open(abs_path.rsplit('.', 1)[0], 'w') as newfile: #Opens the absolute path, including filename, for writing, but does not include the extension (should be .gz or similar)
                            newfile.write(str(file_data))
                except IOError: #This will occur at gz.read() if the file is not a gzip.
                    pass



###################################################
##  GMeta formatting utilities
###################################################

# Adds GMeta wrapping
# Args:
#   data: A dictionary to wrap in GMeta format
#         OR
#         A list of GMeta-wrapped dictionaries (GMetaEntrys)
def format_gmeta(data):
    ''' Formats input into GMeta.
    If data is a dict, returns a GMetaEntry.
    If data is a list (must be GMetaEntrys), returns a GMetaList.
    REQUIRED:
        GMetaEntry (dict):
            globus_subject (unique string, should be URI if possible)
            acl (list of UUID strings, or ["public"])
        GMetaList (list):
            Valid list of GMetaEntrys
    '''
    if type(data) is dict:
        gmeta = {
            "@datatype": "GMetaEntry",
            "@version": "2016-11-09",
            "subject": data["mdf"]["links"]["landing_page"],
            "visible_to": data["mdf"].pop("acl"),
            "content": data
            }

    elif type(data) is list:
        gmeta = {
            "@datatype": "GIngest",
            "@version": "2016-11-09",
            "ingest_type": "GMetaList",
            "ingest_data": {
                "@datatype": "GMetaList",
                "@version": "2016-11-09",
                "gmeta": data
                }
            }

    else:
        raise TypeError("Cannot format '" + str(type(data)) + "' into GMeta.")

    return gmeta


# Removes GMeta wrapping
# Args:
#   gmeta: Dict (or GlobusHTTPResponse, or JSON str) to unwrap
def gmeta_pop(gmeta):
    if type(gmeta) is str:
        gmeta = json.loads(gmeta)
    elif type(gmeta) is GlobusHTTPResponse:
        gmeta = json.loads(gmeta.text)
    elif type(gmeta) is not dict:
        raise TypeError("gmeta must be dict, GlobusHTTPResponse, or JSON string")
    results = []
    for res in gmeta["gmeta"]:
        for con in res["content"]:
            results.append(con)
    return results



###################################################
##  Globus utilities
###################################################

# Attempts to autodetect the local GCP endpoint ID
# If multiple candidates are found, prompt user to choose correct EP
def get_local_ep(transfer_client):
    pgr_res = transfer_client.endpoint_search(filter_scope="my-endpoints")
    ep_candidates = pgr_res.data
    if len(ep_candidates) < 1: #Nothing found
        raise globus_sdk.GlobusError("Error: No local endpoints found")
    elif len(ep_candidates) == 1: #Exactly one candidate
        if ep_candidates[0]["gcp_connected"] == False: #Is GCP, is not on
            raise globus_sdk.GlobusError("Error: Globus Connect is not running")
        else: #Is GCServer or GCP and connected
            return ep_candidates[0]["id"]
    else: # >1 found
        #Filter out disconnected GCP
        ep_connections = [candidate for candidate in ep_candidates if candidate["gcp_connected"] is not False]
        #Recheck list
        if len(ep_connections) < 1: #Nothing found
            raise globus_sdk.GlobusError("Error: No local endpoints running")
        elif len(ep_connections) == 1: #Exactly one candidate
            if ep_connections[0]["gcp_connected"] == False: #Is GCP, is not on
                raise globus_sdk.GlobusError("Error: Globus Connect is not active")
            else: #Is GCServer or GCP and connected
                return ep_connections[0]["id"]
        else: # >1 found
            #Prompt user
            print("Multiple endpoints found:")
            count = 0
            for ep in ep_connections:
                count += 1
                print(count, ": ", ep["display_name"], "\t", ep["id"])
            print("\nPlease choose the endpoint on this machine")
            ep_num = 0
            while ep_num == 0:
                usr_choice = input("Enter the number of the correct endpoint (-1 to cancel): ")
                try:
                    ep_choice = int(usr_choice)
                    if ep_choice == -1: #User wants to quit
                        ep_num = -1 #Will break out of while to exit program
                    elif ep_choice in range(1, count+1): #Valid selection
                        ep_num = ep_choice #Break out of while, return valid ID
                    else: #Invalid number
                        print("Invalid selection")
                except:
                    print("Invalid input")

            if ep_num == -1:
                print("Cancelling")
                raise SystemExit
            return ep_connections[ep_num-1]["id"]



###################################################
##  Clients
###################################################

# Client to access Globus Search
class SearchClient(BaseClient):

    def __init__(self, base_url="https://search.api.globus.org/", default_index=None, **kwargs):
        app_name = kwargs.pop('app_name', 'DataSearch Client v0.1.1')
        BaseClient.__init__(self, "datasearch", app_name=app_name, **kwargs)
        # base URL lookup will fail, producing None, set it by hand
        self.base_url = base_url
        self._headers['Content-Type'] = 'application/json'
        self.default_index = default_index

    def _resolve_uri(self, base_uri, index=None, *parts):
        index = index or self.default_index
        if not index:
            raise ValueError(
                ('You must either pass an explicit index'
                 'or set a default one at the time that you create '
                 'a DataSearchClient'))
        return '/'.join([base_uri, index] + list(parts))

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
            Optional. A resource_type name as defined within the DataSearch
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

    def remove(self, subject, index=None, **params):
        uri = self._resolve_uri('/v1/index', index, "subject")
        params["subject"] = subject
        return self.delete(uri, params=params)

