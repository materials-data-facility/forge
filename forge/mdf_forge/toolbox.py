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

def login(credentials=None, clear_old_tokens=False, **kwargs):
    """Login to Globus services

    Arguments:
    credentials (str or dict): A string filename, string JSON, or dictionary with credential and config information.
                               By default, looks in ~/mdf/credentials/globus_login.json.
        Contains:
        app_name (str): Name of script/client. This will form the name of the token cache file.
        services (list of str): Services to authenticate with (can be transfer, search, search_ingest, or mdf).
        index: The default Search index. Only required if services contains 'search' or 'search_ingest'.
    clear_old_tokens (bool): If True, delete old token file if it exists, forcing user to re-login.
                             If False, use existing token file if there is one.
                             Default False.

    Returns:
    dict: The clients and authorizers requested, indexed by service name.
          For example, if login() is told to auth with 'search' then the search client will be in the 'search' field.
    """
    NATIVE_CLIENT_ID = "98bfc684-977f-4670-8669-71f8337688e4"
    DEFAULT_CRED_FILENAME = "globus_login.json"
    DEFAULT_CRED_PATH = os.path.expanduser("~/mdf/credentials")
    SCOPES = {
        "transfer": "urn:globus:auth:scope:transfer.api.globus.org:all",
        "search": "urn:globus:auth:scope:search.api.globus.org:search",
        "search_ingest": "urn:globus:auth:scope:search.api.globus.org:all",
        "mdf": "urn:globus:auth:scope:data.materialsdatafacility.org:all" # urn:globus:auth:scope:api.materialsdatafacility.org:all"
        }

    def _get_tokens(client, scopes, app_name, force_refresh=False):
        token_path = os.path.join(DEFAULT_CRED_PATH, app_name + "_tokens.json")
        if force_refresh:
            if os.path.exists(token_path):
                os.remove(token_path)
        if os.path.exists(token_path):
            with open(token_path, "r") as tf:
                tokens = json.load(tf)
        else:
            os.makedirs(DEFAULT_CRED_PATH, exist_ok=True)
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
            except json.JSONDecodeError:
                raise ValueError("Credential string unreadable")
    elif type(credentials) is dict:
        creds = credentials
    else:
        try:
            with open(os.path.join(os.getcwd(), DEFAULT_CRED_FILENAME)) as cred_file:
                creds = json.load(cred_file)
        except IOError:
            try:
                with open(os.path.join(DEFAULT_CRED_PATH, DEFAULT_CRED_FILENAME)) as cred_file:
                    creds = json.load(cred_file)
            except IOError:
                raise ValueError("Credentials/configuration must be passed as a filename string, JSON string, or dictionary, or provided in '" + DEFAULT_CRED_FILENAME + "' or '" + DEFAULT_CRED_PATH + "'.")

    native_client = globus_sdk.NativeAppAuthClient(NATIVE_CLIENT_ID, app_name=creds["app_name"])

    servs = []
    for serv in creds.get("services", []):
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
        clients["search_ingest"] = SearchClient(default_index=(creds.get("index", None) or kwargs.get("index", None)), authorizer=ingest_authorizer)
    elif "search" in servs:
        search_authorizer = globus_sdk.RefreshTokenAuthorizer(all_tokens["search.api.globus.org"]["refresh_token"], native_client)
        clients["search"] = SearchClient(default_index=(creds.get("index", None) or kwargs.get("index", None)), authorizer=search_authorizer)
    if "mdf" in servs:
        mdf_authorizer = globus_sdk.RefreshTokenAuthorizer(all_tokens["data.materialsdatafacility.org"]["refresh_token"], native_client)
        clients["mdf"] = mdf_authorizer

    return clients


def confidential_login(credentials=None):
    """Login to Globus services as a confidential client (a client with its own login information).

    Arguments:
    credentials (str or dict): A string filename, string JSON, or dictionary with credential and config information.
                               By default, looks in ~/mdf/credentials/confidential_globus_login.json.
        Contains:
        client_id (str): The ID of the client.
        client_secret (str): The client's secret for authentication.
        services (list of str): Services to authenticate with (can be transfer, search, search_ingest, or mdf).
        index: The default Search index. Only required if services contains 'search' or 'search_ingest'.

    Returns:
    dict: The clients and authorizers requested, indexed by service name.
          For example, if login() is told to auth with 'search' then the search client will be in the 'search' field.
    """
    DEFAULT_CRED_FILENAME = "confidential_globus_login.json"
    DEFAULT_CRED_PATH = os.path.expanduser("~/mdf/credentials")
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
            except json.JSONDecodeError:
                raise ValueError("Credentials unreadable or missing")
    elif type(credentials) is dict:
        creds = credentials
    else:
        try:
            with open(os.path.join(os.getcwd(), DEFAULT_CRED_FILENAME)) as cred_file:
                creds = json.load(cred_file)
        except IOError:
            try:
                with open(os.path.join(DEFAULT_CRED_PATH, DEFAULT_CRED_FILENAME)) as cred_file:
                    creds = json.load(cred_file)
            except IOError:
                raise ValueError("Credentials/configuration must be passed as a filename string, JSON string, or dictionary, or provided in '" + DEFAULT_CRED_FILENAME + "' or '" + DEFAULT_CRED_PATH + "'.")

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
        clients["search_ingest"] = SearchClient(default_index=creds.get("index", None), authorizer=conf_authorizer)
    elif "search" in servs:
        clients["search"] = SearchClient(default_index=creds.get("index", None), authorizer=conf_authorizer)
    if "mdf" in servs:
        clients["mdf"] = conf_authorizer

    return clients



###################################################
##  File utilities
###################################################

def find_files(root, file_pattern=None, verbose=False):
    """Find files recursively in a given directory.

    Arguments:
    root (str): The path to the starting (root) directory.
    file_pattern (str): A regular expression to match files against, or None to match all files. Default None.
    verbose: If True, will print status messages.
             If False, will remain silent unless there is an error.
             Default False.

    Yields:
    dict: The matching file's path information.
        Contains:
        path (str): The path to the directory containing the file.
        no_root_path (str): The path to the directory containing the file, with the path to the root directory removed.
        filename (str): The name of the file.
    """
    # Add separator to end of root if not already supplied
    root += os.sep if root[-1:] != os.sep else ""
    for path, dirs, files in tqdm(os.walk(root), desc="Finding files", disable= not verbose):
        for one_file in files:
            if not file_pattern or re.search(file_pattern, one_file):  # Only care about dirs with desired data
                yield {
                    "path": path,
                    "filename": one_file,
                    "no_root_path": path.replace(root, "")
                    }


def uncompress_tree(root, verbose=False):
    """Uncompress all tar, zip, and gzip archives under a given directory.
    Note that this process tends to be very slow.

    Arguments:
    root (str): The path to the starting (root) directory.
    verbose: If True, will print status messages.
             If False, will remain silent unless there is an error.
             Default False.
    """
    for path, dirs, files in tqdm(os.walk(root), desc="Uncompressing files", disable= not verbose):
        for single_file in files:
            abs_path = os.path.join(path, single_file)
            if tarfile.is_tarfile(abs_path):
                tar = tarfile.open(abs_path)
                tar.extractall(path)
                tar.close()
            elif zipfile.is_zipfile(abs_path):
                z = zipfile.ZipFile(abs_path)
                z.extractall(path)
                z.close()
            else:
                try:
                    with gzip.open(abs_path) as gz:
                        file_data = gz.read()
                        # Opens the absolute path, including filename, for writing, but does not include the extension (should be .gz or similar)
                        with open(abs_path.rsplit('.', 1)[0], 'w') as newfile:
                            newfile.write(str(file_data))
                # An IOErrorwill occur at gz.read() if the file is not a gzip
                except IOError:
                    pass



###################################################
##  GMeta formatting utilities
###################################################

def format_gmeta(data):
    """Format input into GMeta format, suitable for ingesting into Globus Search.
    Format a dictionary into a GMetaEntry.
    Format a list of GMetaEntry into a GMetaList inside a GMetaIngest.

    Example usage:
        glist = []
        for document in all_my_documents:
            gmeta_entry = format_gmeta(document)
            glist.append(gmeta_entry)
        ingest_ready_document = format_gmeta(glist)

    Arguments:
    data (dict or list): The data to be formatted.
        If data is a dict, it must contain:
        data["mdf"]["links"]["landing_page"] (str): A URI to a web page for the entry.
        data["mdf"]["acl"] (list of str): A list of Globus UUIDs that are allowed to view the entry.
        If data is a list, it must consist of GMetaEntry documents.

    Returns:
    dict (if data is dict): The data as a GMetaEntry.
    dict (if data is list): The data as a GMetaIngest.
    """
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


def gmeta_pop(gmeta, info=False):
    """Remove GMeta wrapping from a Globus Search result.
    This function can be called on the raw GlobusHTTPResponse that Search returns, or a string or dictionary representation of it.

    Arguments:
    gmeta (dict, str, or GlobusHTTPResponse): The Globus Search result to unwrap.
    info (bool): If False, gmeta_pop will return a list of the results and discard the metadata.
                 If True, gmeta_pop will return a tuple containing the results list, and other information about the query.
                 Default False.

    Returns:
    list (if info=False): The unwrapped results.
    tuple (if info=True): The unwrapped results, and a dictionary of query information.
    """
    if type(gmeta) is GlobusHTTPResponse:
        gmeta = json.loads(gmeta.text)
    elif type(gmeta) is str:
        gmeta = json.loads(gmeta)
    elif type(gmeta) is not dict:
        raise TypeError("gmeta must be dict, GlobusHTTPResponse, or JSON string")
    results = []
    for res in gmeta["gmeta"]:
        for con in res["content"]:
            results.append(con)
    if info:
        fyi = {
            "total_query_matches": gmeta["total"]
            }
        return results, fyi
    else:
        return results



###################################################
##  Globus utilities
###################################################

def get_local_ep(transfer_client):
    """Discover the local Globus Connect Personal endpoint's ID, if possible.

    Arguments:
    transfer_client (TransferClient): An authenticated Transfer client.

    Returns:
    str: The local GCP EP ID if it was discovered.
    If the ID is not discovered, an exception (globus_sdk.GlobusError unless the user cancels the search) will be raised.
    """
    pgr_res = transfer_client.endpoint_search(filter_scope="my-endpoints")
    ep_candidates = pgr_res.data
    if len(ep_candidates) < 1:  # Nothing found
        raise globus_sdk.GlobusError("Error: No local endpoints found")
    elif len(ep_candidates) == 1:  # Exactly one candidate
        if ep_candidates[0]["gcp_connected"] == False:  # Is GCP, is not on
            raise globus_sdk.GlobusError("Error: Globus Connect is not running")
        else:  # Is GCServer or GCP and connected
            return ep_candidates[0]["id"]
    else: # >1 found
        #Filter out disconnected GCP
        ep_connections = [candidate for candidate in ep_candidates if candidate["gcp_connected"] is not False]
        #Recheck list
        if len(ep_connections) < 1:  # Nothing found
            raise globus_sdk.GlobusError("Error: No local endpoints running")
        elif len(ep_connections) == 1:  # Exactly one candidate
            if ep_connections[0]["gcp_connected"] == False:  # Is GCP, is not on
                raise globus_sdk.GlobusError("Error: Globus Connect is not active")
            else:  # Is GCServer or GCP and connected
                return ep_connections[0]["id"]
        else:  # >1 found
            # Prompt user
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
                    if ep_choice == -1:  # User wants to quit
                        ep_num = -1  # Will break out of while to exit program
                    elif ep_choice in range(1, count+1):  # Valid selection
                        ep_num = ep_choice  # Break out of while, return valid ID
                    else:  # Invalid number
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

class SearchClient(BaseClient):
    """Access (search and ingest) Globus Search."""

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

