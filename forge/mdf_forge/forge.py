import os

import requests
import globus_sdk
from tqdm import tqdm

from mdf_forge import toolbox

HTTP_NUM_LIMIT = 10


class Forge:
    """Fetch metadata from Globus Search and files from the Materials Data Facility.
    Forge is intended to be the best way to access MDF data for all users.
    An internal Query object is used to make queries. From the user's perspective,
    an instantiation of Forge will black-box searching.

    Public Variables:
    local_ep is the endpoint ID of the local Globus Connect Personal endpoint.

    Methods:
    __init__ handles authentication with Globus Auth.
    match_term adds simple terms to the query.
    match_field adds a field:value pair to the query.
    match_sources specifies `source_name`s to search for results in.
    match_elements specifies element abbreviations to match.
    search executes a search.
    search_by_elements executes a search for given elements in given sources.
    aggregate_source returns all records for a given source.
    reset_query destroys the current query and starts a fresh one.
    http_download saves the data files associated with results to disk with HTTPS.
    globus_download saves the data files associated with results to disk with Globus Transfer.
    http_stream yields a generator to fetch data files in sequence.
    http_return returns all the data files at once.
    """
    __index = "mdf"
    __services = ["mdf", "transfer", "search"]
    __app_name = "MDF Forge"

    def __init__(self, data={}):
        self.__index = data.get('index', self.__index)
        self.__services = data.get('services', self.__services)
        self.local_ep = data.get("local_ep", None)

        clients = toolbox.login(credentials={
                                "app_name": self.__app_name,
                                "services": self.__services,
                                "index": self.__index})
        self.__search_client = clients["search"]
        self.__transfer_client = clients["transfer"]
        self.__mdf_authorizer = clients["mdf"]

        self.__query = Query(self.__search_client)


    def match_term(self, term, match_all=True):
        """Add a term to the query.

        Arguments:
        term (str): The term to add.
        match_all (bool): If True, will add term with AND. If False, will use OR. Default True.

        Returns:
        self (Forge): For chaining.
        """
        self.__query.match_term(term=term, match_all=match_all)
        return self


    def match_field(self, field, value, match_all=True):
        """Add a field:value term to the query.
        Matches will have field == value.

        Arguments:
        field (str): The field to look in for the value.
            The field must be namespaced according to Elasticsearch rules, which means a dot to dive into dictionaries.
            Ex. "mdf.source_name" is the "source_name" field of the "mdf" dictionary.
            If no namespace is provided, the default ("mdf.") will be used.
        value (str): The value to match.
        match_all: If True, will add with AND. If False, will use OR. Default True.
 
        Returns:
        self (Forge): For chaining.
        """
        self.__query.match_field(field=field, value=value, match_all=match_all)
        return self


    def match_sources(self, sources):
        """Add sources to match to the query.
        match_sources(x) is equivalent to match_field("mdf.source_name", x, match_all=False)

        Arguments:
        sources (str or list): The sources to match.
 
        Returns:
        self (Forge): For chaining.
        """
        self.__query.match_field(field="mdf.source_name", value=sources, match_all=False)
        return self


    def match_elements(self, elements, match_all=True):
        """Add elemental abbreviations to the query.
        match_elements(x) is equivalent to match_field("mdf.elements", x)

        Arguments:
        elements (str or list): The elements to match.
        match_all: If True, will add with AND. If False, will use OR. Default True.
 
        Returns:
        self (Forge): For chaining.
        """
        self.__query.match_field(field="mdf.elements", value=elements, match_all=match_all)
        return self


    def search(self, q=None, advanced=False, limit=10000, info=False, reset_query=True):
        """Execute a search and return the results.

        Arguments:
        q (str): The query to execute. Defaults to the current query, if any. There must be some query to execute.
        advanced (bool): Submit the query in "advanced" mode, which enables searches other than basic fulltext. Default False.
            This value can change to True automatically if the query is built using advanced features, such as match_field.
        limit (int): The maximum number of results to return. The max for this argument is 10,000.
        info (bool): If False, search will return a list of the results.
                     If True, search will return a tuple containing the results list, and other information about the query.
                     Default False.
        reset_query (bool): If True, will destroy the query after execution and start a fresh one. Does nothing if False. Default True.

        Returns:
        list (if info=False): The results.
        tuple (if info=True): The results, and a dictionary of query information.
        """
        res = self.__query.search(q=q, advanced=advanced, limit=limit, info=info)
        if reset_query:
            self.reset_query()
        return res


    def search_by_elements(self, elements=[], sources=[], limit=None, match_all=False, info=False, reset_query=True):
        """Execute a search for the given elements in the given sources.
        search_by_elements([x], [y]) is equivalent to match_elements([x]).match_sources([y]).search()

        Arguments:
        elements (list of str): The elements to match. Default [].
        sources (list of str): The sources to match. Default [].
        limit (int): The maximum number of results to return. The max for this argument is 10,000.
        info (bool): If False, search will return a list of the results.
                     If True, search will return a tuple containing the results list, and other information about the query.
                     Default False.
        reset_query (bool): If True, will destroy the query after execution and start a fresh one. Does nothing if False. Default True.

        Returns:
        list (if info=False): The results.
        tuple (if info=True): The results, and a dictionary of query information.
        """
        res = self.__query.match_elements(elements, match_all=match_all).match_sources(sources, match_all=match_all).search(limit=limit, info=info)
        if reset_query:
            self.reset_query()
        return res


    def aggregate_source(self, source, limit=None):
        """Aggregate all records from a given source.
        There is no inherent limit to the number of results returned.
        Note that this method does not use or alter the current query.

        Arguments:
        source (str): The source to aggregate.
        limit (int): The maximum number of results to return. Default None, to return all results.
        
        Returns:
        list: All of the records from the source.
        """
        return Query(self.search_client).aggregate_source(source=source, limit=limit)


    def reset_query():
        """Destroy the current query and create a fresh, clean one."""
        del self.__query
        self.__query = Query(self.__search_client)


    def http_download(self, results, dest=".", preserve_dir=False, verbose=True):
        if type(results) is globus_sdk.GlobusHTTPResponse:
            results = toolbox.gmeta_pop(results)
        if len(results) > HTTP_NUM_LIMIT:
            return {
                "success": False,
                "message": "Too many results supplied. Use globus_download() for fetching more than " + str(HTTP_NUM_LIMIT) + " entries."
                }
        for res in tqdm(results, desc="Fetching files", disable= not verbose):
            for key in tqdm(res["mdf"]["links"].keys(), desc="Fetching files", disable=True):  # not verbose):
                dl = res["mdf"]["links"][key]
                host = dl.get("http_host", None) if type(dl) is dict else None
                if host:
                    remote_path = dl["path"]
                    # local_path should be either dest + whole path or dest + filename, depending on preserve_dir
                    local_path = os.path.normpath(dest + "/" + dl["path"]) if preserve_dir else os.path.normpath(dest + "/" + os.path.basename(dl["path"]))
                    # Make dirs for storing the file if they don't exist
                    # preserve_dir doesn't matter; local_path has accounted for it already
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)
                    # Check if file already exists, change filename if necessary
                    collisions = 0
                    while os.path.exists(local_path):
                        # Find period marking extension, if exists
                        # Will be after last slash
                        index = local_path.rfind(".", local_path.rfind("/"))
                        if index < 0:
                            ext = ""
                        else:
                            ext = local_path[index:]
                            local_path = local_path[:index]
                        # Check if already added number to end
                        old_add = "("+str(collisions)+")"
                        collisions += 1
                        new_add = "("+str(collisions)+")"
                        if local_path.endswith(old_add):
                            local_path = local_path[:-len(old_add)] + new_add + ext
                        else:
                            local_path = local_path + new_add + ext
                    headers = {}
                    self.mdf_authorizer.set_authorization_header(headers)
                    response = requests.get(host+remote_path, headers=headers)
                    # Handle first 401 by regenerating auth headers
                    if response.status_code == 401:
                        self.mdf_authorizer.handle_missing_authorization()
                        self.mdf_authorizer.set_authorization_header(headers)
                        self.response = requests.get(host+remote_path, headers=headers)
                    # Handle other errors by passing the buck to the user
                    if response.status_code != 200:
                        print("Error", response.status_code, " when attempting to access '", host+remote_path, "'", sep="")
                    else:
                        # Write out the binary response content
                        with open(local_path, 'wb') as output:
                            output.write(response.content)


    def globus_download(self, results, dest=".", local_ep=None, preserve_dir=False, verbose=True):
        if type(results) is globus_sdk.GlobusHTTPResponse:
            results = toolbox.gmeta_pop(results)
        if not local_ep:
            if not self.local_ep:
                self.local_ep = toolbox.get_local_ep(self.transfer_client)
            local_ep = self.local_ep
        tasks = {}
        filenames = []
        for res in tqdm(results, desc="Processing records", disable= not verbose):
            for key in tqdm(res["mdf"]["links"].keys(), desc="Fetching files", disable=True):  # not verbose):
                dl = res["mdf"]["links"][key]
                host = dl.get("globus_endpoint", None) if type(dl) is dict else None
                if host:
                    remote_path = dl["path"]
                    # local_path should be either dest + whole path or dest + filename, depending on preserve_dir
                    local_path = os.path.normpath(dest + "/" + dl["path"]) if preserve_dir else os.path.normpath(dest + "/" + os.path.basename(dl["path"]))
                    # Make dirs for storing the file if they don't exist
                    # preserve_dir doesn't matter; local_path has accounted for it already
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)
                    # Check if file already exists, change filename if necessary
                    collisions = 0
                    while os.path.exists(local_path) or local_path in filenames:
                        # Find period marking extension, if exists
                        # Will be after last slash
                        index = local_path.rfind(".", local_path.rfind("/"))
                        if index < 0:
                            ext = ""
                        else:
                            ext = local_path[index:]
                            local_path = local_path[:index]
                        # Check if already added number to end
                        old_add = "("+str(collisions)+")"
                        collisions += 1
                        new_add = "("+str(collisions)+")"
                        if local_path.endswith(old_add):
                            local_path = local_path[:-len(old_add)] + new_add + ext
                        else:
                            local_path = local_path + new_add + ext

                    if host not in tasks.keys():
                        tasks[host] = globus_sdk.TransferData(self.transfer_client, host, local_ep, verify_checksum=True)
                    tasks[host].add_item(remote_path, local_path)
                    filenames.append(local_path)
        submissions = []
        for td in tqdm(tasks.values(), desc="Submitting transfers", disable= not verbose):
            result = self.transfer_client.submit_transfer(td)
            if result["code"] != "Accepted":
                print("Error submitting transfer:", result["message"])
            else:
                submissions.append(result["submission_id"])
        if verbose:
            print("All transfers submitted")
            print("Submission IDs:", "\n".join(submissions))


    def http_stream(self, results, verbose=True):
        if type(results) is globus_sdk.GlobusHTTPResponse:
            results = toolbox.gmeta_pop(results)
        if len(results) > HTTP_NUM_LIMIT:
            return {
                "success": False,
                "message": "Too many results supplied. Use globus_download() for fetching more than " + str(HTTP_NUM_LIMIT) + " entries."
                }
        for res in results:
            for key in res["mdf"]["links"].keys():
                dl = res["mdf"]["links"][key]
                host = dl.get("http_host", None) if type(dl) is dict else None
                if host:
                    remote_path = dl["path"]
                    headers = {}
                    self.mdf_authorizer.set_authorization_header(headers)
                    response = requests.get(host+remote_path, headers=headers)
                    # Handle first 401 by regenerating auth headers
                    if response.status_code == 401:
                        self.mdf_authorizer.handle_missing_authorization()
                        self.mdf_authorizer.set_authorization_header(headers)
                        self.response = requests.get(host+remote_path, headers=headers)
                    # Handle other errors by passing the buck to the user
                    if response.status_code != 200:
                        print("Error", response.status_code, " when attempting to access '", host+remote_path, "'", sep="")
                    else:
                        yield response.text


    def http_return(self, results, verbose=True):
        return list(http_stream(results, verbose=verbose))



class Query:
    def __init__(self, search_client, q="", limit=None, advanced=False):
        self.search_client = search_client
        self.query = q
        self.limit = limit
        self.advanced = False


    def match_term(self, term, match_all=True):
        self.query += (" AND " if match_all else " OR ") + term
        return self


    def match_field(self, field, value, match_all=True):
        # match_all determines AND/OR
        match_join = " AND " if match_all else " OR "
        # If no namespacing provided, add default
        if "." not in field:
            field = "mdf." + field
        # If value list should be OR'd
        if type(value) is list and not match_all:
            value = ",".join(value)
        # Make value into list for processing
        if type(value) is not list:
            value = [value]

        for val in value:
            self.query += (match_join + field + ":" + val)
        self.advanced = True
        return self


    def search(self, q=None, advanced=None, limit=None, info=False):
        if q is None:
            q = self.query
        if not q:
            print("Error: No query specified")
            return []
        if advanced is None or self.advanced:
            advanced = self.advanced
        if limit is None:
            limit = self.limit or 10000

        # Clean query string
        q = q.strip()
        removes = ["AND", "OR"]
        for rterm in removes:
            if q.startswith(rterm):
                q = q[len(rterm):]
            if q.endswith(rterm):
                q = q[:-len(rterm)]
        q = q.strip()


        # Simple query (max 10k results)
        query = {
            "q": q,
            "advanced": advanced,
            "limit": limit
            }
        return toolbox.gmeta_pop(self.search_client.structured_search(query), info=info)


    def aggregate_source(self, source, limit=None):
        full_res = []
        res = True  # Start res as value that will pass while condition
        # Scroll while results are being returned and limit not reached
        while res and (limit is None or limit > 0):
            query = {
                "q": "mdf.source_name:" + source + " AND mdf.scroll_id:(>" + str(len(full_res)) + " AND <" + str( len(full_res) + (limit or 10000) ) + ")",
                "advanced": True,
                "limit": limit or 10000
                }
            res = toolbox.gmeta_pop(self.search_client.structured_search(query))
            num_res = len(res)
            full_res += res
            # If a limit was set, lower future limit by number of results saved
            if limit:
                limit -= num_res
        return full_res


