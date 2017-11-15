import os

import requests
import globus_sdk
from tqdm import tqdm

from six import print_, string_types

from mdf_toolbox import toolbox

# Maximum recommended number of HTTP file transfers
# Large transfers are much better suited to Globus Transfer use
HTTP_NUM_LIMIT = 2000
# Maximum number of results per search allowed by Globus Search
SEARCH_LIMIT = 10000


class Forge:
    """Fetch metadata from Globus Search and files from the Materials Data Facility.
    Forge is intended to be the best way to access MDF data for all users.
    An internal Query object is used to make queries. From the user's perspective,
    an instantiation of Forge will black-box searching.

    Public Variables:
    local_ep is the endpoint ID of the local Globus Connect Personal endpoint.

    """
    __default_index = "mdf"
    __services = ["mdf", "transfer", "search"]
    __app_name = "MDF_Forge"

    def __init__(self, index=__default_index, local_ep=None, **kwargs):
        """Initialize the Forge instance.

        Arguments:
        index (str): The Globus Search index to search on. Default "mdf".
        local_ep (str): The endpoint ID of the local Globus Connect Personal endpoint.
                        If not provided, may be autodetected as possible.

        Keyword Arguments:
        services (list of str): The services to authenticate for.
                                Advanced users only.
        """
        self.__index = index
        self.local_ep = local_ep
        self.__services = kwargs.get('services', self.__services)

        clients = toolbox.login(credentials={
                                "app_name": self.__app_name,
                                "services": self.__services,
                                "index": self.__index})
        self.__search_client = clients["search"]
        self.__transfer_client = clients["transfer"]
        self.__mdf_authorizer = clients["mdf"]

        self.__query = Query(self.__search_client)


    @property
    def search_client(self):
        return self.__search_client


    @property
    def transfer_client(self):
        return self.__transfer_client


    @property
    def mdf_authorizer(self):
        return self.__mdf_authorizer


#################################################
##  Core functions
#################################################

    def match_field(self, field, value, required=True, new_group=False):
        """Add a field:value term to the query.
        Matches will have field == value.

        Arguments:
        field (str): The field to check for the value.
            The field must be namespaced according to Elasticsearch rules using the dot syntax.
            Ex. "mdf.source_name" is the "source_name" field of the "mdf" dictionary.
        value (str): The value to match.
        required (bool): If True, will add term with AND. If False, will use OR. Default True.
        new_group (bool): If True, will separate term into new parenthetical group.
                          If False, will not.
                          Default False.

        Returns:
        self (Forge): For chaining.
        """
        # If not the start of the query string, add an AND or OR
        if self.__query.initialized:
            if required:
                self.__query.and_join(new_group)
            else:
                self.__query.or_join(new_group)
        self.__query.field(field, value)
        return self


    def exclude_field(self, field, value, new_group=False):
        """Exclude a field:value term from the query.
        Matches will NOT have field == value.

        Arguments:
        field (str): The field to check for the value.
            The field must be namespaced according to Elasticsearch rules using the dot syntax.
            Ex. "mdf.source_name" is the "source_name" field of the "mdf" dictionary.
        value (str): The value to exclude.
        new_group (bool): If True, will separate term into new parenthetical group.
                          If False, will not.
                          Default False.

        Returns:
        self (Forge): For chaining.
        """
        # If not the start of the query string, add an AND
        # OR would not make much sense for excluding
        if self.__query.initialized:
            self.__query.and_join(new_group)
        self.__query.negate().field(field, value)
        return self


    def search(self, q=None, advanced=False, limit=SEARCH_LIMIT, info=False,
               reset_query=True):
        """Execute a search and return the results.

        Arguments:
        q (str): The query to execute. Defaults to the current query, if any.
                 There must be some query to execute.
        advanced (bool): If True, will submit query in "advanced" mode to enable field matches.
                         If False, only basic fulltext term matches will be supported.
                         Default False.
                         This value will change to True automatically 
                            if the query is built with helpers.
        limit (int): The maximum number of results to return.
                     The max for this argument is the SEARCH_LIMIT imposed by Globus Search.
        info (bool): If False, search will return a list of the results.
                     If True, search will return a tuple containing the results list
                        and other information about the query.
                     Default False.
        reset_query (bool): If True, will destroy the query after execution and start a fresh one.
                            If False, keeps the current query alive.
                            Default True.

        Returns:
        list (if info=False): The results.
        tuple (if info=True): The results, and a dictionary of query information.
        """
        res = self.__query.search(q=q, advanced=advanced, limit=limit, info=info)
        if reset_query:
            self.reset_query()
        return res


    def aggregate(self, q=None, scroll_size=SEARCH_LIMIT, reset_query=True):
        """Perform an advanced query, and return all matching results.
        Will automatically preform multiple queries in order to retrieve all results.

        Note that all aggregate queries run in advanced mode.

        Arguments:
        q (str): The query to execute. Defaults to the current query, if any.
                 There must be some query to execute.
        scroll_size (int): Minimum number of records returned per query
        reset_query (bool): If True, will destroy the query after execution and start a fresh one.
                            If False, will keep the current query alive.
                            Default True.

        Returns:
        list of dict: All matching records
        """
        res = self.__query.aggregate(q=q, scroll_size=scroll_size)
        if reset_query:
            self.reset_query()
        return res


    def show_fields(self, block=None):
        """Retrieve and return the mapping for the given metadata block.

        Arguments:
        block (str): The top-level field to fetch the mapping for.
                     Default None, which lists just the blocks.

        Returns:
        dict: A set of field:datatype pairs.
        """
        mapping = self.__query.mapping()
        if not block:
            blocks = set()
            for key in mapping.keys():
                blocks.add(key.split(".")[0])
            block_map = {}
            for b in blocks:
                block_map[b] = "object"
        else:
            block_map = {}
            for key, value in mapping.items():
                if key.startswith(block):
                    block_map[key] = value
        return block_map


    def current_query(self):
        """Return the current query string.

        Returns:
        str: The current query string.
        """
        return self.__query.clean_query()


    def reset_query(self):
        """Destroy the current query and create a fresh one.
        
        Returns:
        None: Does not return self because this method should not be chained.
        """
        del self.__query
        self.__query = Query(self.__search_client)


#################################################
##  Expanded functions
#################################################

    def match_range(self, field, start, stop, inclusive=True, required=True, new_group=False):
        """Add a field:[some range] term to the query.
        Matches will have field == value in range.

        Arguments:
        field (str): The field to check for the value.
            The field must be namespaced according to Elasticsearch rules using the dot syntax.
            Ex. "mdf.source_name" is the "source_name" field of the "mdf" dictionary.
        start (str or int): The starting value. "*" is acceptable to make no lower bound.
        stop (str or int): The ending value. "*" is acceptable to have no upper bound.
        inclusive (bool): If True, the start and stop values will be included in the search.
                          If False, the start and stop values will not be included in the search.
        required (bool): If True, will add term with AND. If False, will use OR. Default True.
        new_group (bool): If True, will separate term into new parenthetical group.
                          If False, will not.
                          Default False.

        Returns:
        self (Forge): For chaining.
        """
        if inclusive:
            value = "[" + str(start) + " TO " + str(stop) + "]"
        else:
            value = "{" + str(start) + " TO " + str(stop) + "}"
        self.match_field(field, value, required=required, new_group=new_group)
        return self


    def exclude_range(self, field, start, stop, inclusive=True, new_group=False):
        """Exclude a field:[some range] term to the query.
        Matches will have field != values in range.

        Arguments:
        field (str): The field to check for the value.
            The field must be namespaced according to Elasticsearch rules using the dot syntax.
            Ex. "mdf.source_name" is the "source_name" field of the "mdf" dictionary.
        start (str or int): The starting value. "*" is acceptable to make no lower bound.
        stop (str or int): The ending value. "*" is acceptable to have no upper bound.
        inclusive (bool): If True, the start and stop values will not be included in the search.
                          If False, the start and stop values will be included in the search.
        new_group (bool): If True, will separate term into new parenthetical group.
                          If False, will not.
                          Default False.

        Returns:
        self (Forge): For chaining.
        """
        if inclusive:
            value = "[" + str(start) + " TO " + str(stop) + "]"
        else:
            value = "{" + str(start) + " TO " + str(stop) + "}"
        self.exclude_field(field, value, new_group=new_group)
        return self


#################################################
##  Helper functions
#################################################

    def exclusive_match(self, field, value):
        """Match exactly the given value, with no other data in the field.
        
        Arguments:
        field (str): The field to check for the value.
            The field must be namespaced according to Elasticsearch rules using the dot syntax.
            Ex. "mdf.source_name" is the "source_name" field of the "mdf" dictionary.
        value (str or list of str): The value to match exactly.

        Returns:
        self (Forge): For chaining
        """
        if isinstance(value, string_types):
            value = [value]
        value.sort()
        # Hacky way to get ES to do exclusive search
        # Essentially have a big range search that matches NOT anything
        # Except for the actual values
        # Example: [foo, bar, baz] => 
        #   (NOT {* TO foo} AND [foo TO foo] AND NOT {foo to bar} AND [bar TO bar]
        #    AND NOT {bar TO baz} AND [baz TO baz] AND NOT {baz TO *})
        # Except it must be sorted to not overlap
        
        # Start with removing everything before first value
        self.exclude_range(field, "*", value[0], inclusive=False, new_group=True)
        # Select first value
        self.match_range(field, value[0], value[0])
        # Do the rest of the values
        for index, val in enumerate(value[1:]):
            self.exclude_range(field, value[index-1], val, inclusive=False)
            self.match_range(field, val, val)
        # Add end
        self.exclude_range(field, value[-1], "*", inclusive=False)
        # Done
        return self


    def match_sources(self, sources):
        """Add sources to match to the query.

        Arguments:
        sources (str or list of str): The sources to match.
 
        Returns:
        self (Forge): For chaining.
        """
        # If no sources are supplied, nothing to match
        if not sources:
            return self
        if isinstance(sources, string_types):
            sources = [sources]
        # First source should be in new group and required
        self.match_field(field="mdf.source_name", value=sources[0], required=True, new_group=True)
        # Other sources should stay in that group, and not be required
        for src in sources[1:]:
            self.match_field(field="mdf.source_name", value=src, required=False, new_group=False)
        return self


    def match_ids(self, mdf_ids):
        """Match all the IDs in the given mdf_id list.

        Arguments:
        mdf_ids (str or list of str): The IDs to match.

        Returns:
        self (Forge): For chaining.
        """
        # If no IDs are supplied, nothing to match
        if not mdf_ids:
            return self
        if isinstance(mdf_ids, string_types):
            mdf_ids = [mdf_ids]
        # First ID should be in new group and required
        self.match_field(field="mdf.mdf_id", value=mdf_ids[0], required=True, new_group=True)
        # Other IDs should stay in that group, and not be required
        for mid in mdf_ids[1:]:
            self.match_field(field="mdf.mdf_id", value=mid, required=False, new_group=False)
        return self


    def match_elements(self, elements, match_all=True):
        """Add elemental abbreviations to the query.

        Arguments:
        elements (str or list of str): The elements to match.
        match_all (bool): If True, will add with AND. If False, will use OR. Default True.
 
        Returns:
        self (Forge): For chaining.
        """
        # If no elements are supplied, nothing to match
        if not elements:
            return self
        if isinstance(elements, string_types):
            elements = [elements]
        # First element should be in new group and required
        self.match_field(field="mdf.elements", value=elements[0], required=True, new_group=True)
        # Other elements should stay in that group
        for element in elements[1:]:
            self.match_field(field="mdf.elements", value=element, required=match_all,
                             new_group=False)
        return self


    def match_tags(self, tags, match_all=True):
        """Add tags to the query.

        Arguments:
        tags (str or list of str): The tags (keywords) to match.
        match_all (bool): If True, will add with AND. If False, will use OR. Default True.

        Returns:
        self (Forge): For chaining.
        """

        if not tags:
            return self
        if isinstance(tags, string_types):
            tags = [tags]

        self.match_field(field="mdf.tags", value=tags[0], required=True, new_group=True)
        for tag in tags[1:]:
            self.match_field(field="mdf.tags", value=tag, required=match_all,
                             new_group=False)
        return self


    def match_titles(self, titles):
        """Add titles to the query.

        Arguments:
        titles (str or list of str): The titles to match.

        Returns:
        self (Forge): For chaining.
        """
        if not titles:
            return self
        if not isinstance(titles, list):
            titles = [titles]

        self.match_field(field="mdf.title", value=titles[0], required=True, new_group=True)
        for title in titles[1:]:
            self.match_field(field="mdf.title", value=title, required=False, new_group=False)
        return self


    def match_resource_types(self, types):
        """Match the given resource types.

        Arguments:
        types (str or list of str): The resource_types to match.

        Returns:
        self (Forge): For chaining.
        """
        # If no types, nothing to match
        if not types:
            return self
        if isinstance(types, string_types):
            types = [types]
        # First type should be in new group and required
        self.match_field(field="mdf.resource_type", value=types[0], required=True, new_group=True)
        # Other IDs should stay in that group, and not be required
        for rt in types[1:]:
            self.match_field(field="mdf.resource_type", value=rt, required=False, new_group=False)
        return self
        

#################################################
##  Premade searches
#################################################

    def search_by_elements(self, elements, sources=[], limit=None, match_all=True, info=False):
        """Execute a search for the given elements in the given sources.
        search_by_elements([x], [y]) is equivalent to
            match_elements([x]).match_sources([y]).search()
        Note that this method does use terms from the current query.

        Arguments:
        elements (list of str): The elements to match. Default [].
        sources (list of str): The sources to match. Default [].
        limit (int): The maximum number of results to return.
                     The max for this argument is the SEARCH_LIMIT imposed by Globus Search.
        match_all (bool): If True, will add elements with AND. 
                          If False, will use OR.
                          Default True.
        info (bool): If False, search will return a list of the results.
                     If True, search will return a tuple containing the results list,
                        and other information about the query.
                     Default False.

        Returns:
        list (if info=False): The results.
        tuple (if info=True): The results, and a dictionary of query information.
        """
        return (self.match_elements(elements, match_all=match_all)
                    .match_sources(sources)
                    .search(limit=limit, info=info))


    def search_by_titles(self, titles, limit=None, info=False):
        """Execute a search for the given titles.
        search_by_titles([x]) is equivalent to match_titles([x]).search()

        Arguments:
        titles (list of str): The titles to match. Default [].
        limit (int): The maximum number of results to return.
                     The max for this argument is the SEARCH_LIMIT imposed by Globus Search.
        info (bool): If False, search will return a list of the results.
                     If True, search will return a tuple containing the results list,
                        and other information about the query.
                     Default False.

        Returns:
        list (if info=False): The results.
        tuple (if info=True): The results, and a dictionary of query information.
        """
        return self.match_titles(titles).search(limit=limit, info=info)


    def search_by_tags(self, tags, limit=None, match_all=True, info=False):
        """Execute a search for the given tag.
        search_by_tags([x]) is equivalent to match_tags([x]).search()

        Arguments:
        tags (list of str): The tags to match. Default [].
        limit (int): The maximum number of results to return.
                     The max for this argument is the SEARCH_LIMIT imposed by Globus Search.
        match_all (bool): If True, will add elements with AND.
                          If False, will use OR.
                          Default True.
        info (bool): If False, search will return a list of the results.
        If True, search will return a tuple containing the results list,
            and other information about the query.
        Default False.

        Returns:
        list (if info=False): The results.
        tuple (if info=True): The results, and a dictionary of query information.
        """
        return self.match_tags(tags, match_all=match_all).search(limit=limit, info=info)


    def aggregate_source(self, sources):
        """Aggregate all records from a given source.
        There is no limit to the number of results returned.
        Please beware of aggregating very large datasets.

        Arguments:
        sources (str or list of str): The source to aggregate.
        
        Returns:
        list of dict: All of the records from the source.
        """
        return self.match_sources(sources).aggregate()


    def fetch_datasets_from_results(self, entries=None, query=None, reset_query=True):
        """Retrieve the dataset entries for given records.
        Note that this method may use the current query.

        Arguments:
        entries (dict, list of dict, or tuple of dict): The records to parse to find the datasets.
            entries can be a single entry, a list of entries, or a tuple with a list of entries.
            The latter two options support both return values of the search() method.
            If entries is None, the current query is executed and those results are used instead.
        query (str): If entries is None:
                        Search using this query instead of the current query.
                        Default None, which uses the current query.
        reset_query (bool): If entries is None and query is None:
                                If True, will reset the current query after searching.
                                If False, will leave the current query in memory.
                                Default True.
                            Else:
                                Does nothing.

        Returns:
        list: The dataset entries.
        """
        if entries is None:
            entries = self.search(q=query, reset_query=(query is None or reset_query))
        # If entries is not a list of dict, make it one
        if isinstance(entries, dict):
            entries = [entries]
        elif isinstance(entries, tuple):
            entries = entries[0]
        ds_ids = set()
        # For every entry, extract the appropriate ID
        for entry in entries:
            # For records, extract the parent_id
            # Most entries should be records here
            if entry["mdf"]["resource_type"] == "record":
                ds_ids.add(entry["mdf"]["links"]["parent_id"])
            # For datasets, extract the mdf_id
            elif entry["mdf"]["resource_type"] == "dataset":
                ds_ids.add(entry["mdf"]["mdf_id"])
            # For anything else (collection), do nothing
            else:
                pass
        return self.match_ids(list(ds_ids)).search()


#################################################
##  Data retrieval functions
#################################################

    def http_download(self, results, dest=".", preserve_dir=False, verbose=True):
        """Download data files from the provided results using HTTPS.
        For more than HTTP_NUM_LIMIT (defined above) files, you should use globus_download(),
            which uses Globus Transfer.

        Arguments:
        results (dict): The records from which files should be fetched.
                        This should be the return value of a search method.
        dest (str): The destination path for the data files on the local machine.
                    Default current directory.
        preserve_dir (bool): If True, the directory structure for the data files will be
                                recreated at the destination.
                             If False, only the data files themselves will be saved.
                            Default False.
        verbose (bool): If True, status and progress messages will be printed.
                        If False, only error messages will be printed.
                        Default True.
        """
        # If user submitted single result, make into list
        if isinstance(results, dict):
            results = [results]
        # If results have info attached, remove it
        elif isinstance(results, tuple):
            results = results[0]
        if len(results) > HTTP_NUM_LIMIT:
            print_("Error: Too many results supplied. Use globus_download()"
                   + " for fetching more than "
                   + str(HTTP_NUM_LIMIT)
                   + " entries.")
            return {
                "success": False,
                "message": ("Too many results supplied. Use globus_download()"
                            + " for fetching more than "
                            + str(HTTP_NUM_LIMIT)
                            + " entries.")
                }
        for res in tqdm(results, desc="Fetching files", disable= not verbose):
            for key in res["mdf"]["links"].keys():
                dl = res["mdf"]["links"][key]
                host = dl.get("http_host", None) if type(dl) is dict else None
                if host:
                    remote_path = dl["path"]
                    # local_path should be either dest + whole path or dest + filename
                    if preserve_dir:
                        local_path = os.path.normpath(dest + "/" + dl["path"])
                    else: 
                        local_path = os.path.normpath(dest + "/" + os.path.basename(dl["path"]))
                    # Make dirs for storing the file if they don't exist
                    # preserve_dir doesn't matter; local_path has accounted for it already
                    try:
                        os.makedirs(os.path.dirname(local_path))
                    # If dest is current dir and preserve_dir=False, there are no dirs to make.
                    # os.makedirs() will raise FileNotFoundError (Python3 subclass of IOError).
                    # Since it means all dirs required exist, it can be swallowed.
                    except (IOError, OSError):
                        pass
                    # Check if file already exists, change filename if necessary
                    collisions = 0
                    while os.path.exists(local_path):
                        # Find period marking extension, if exists
                        # Will be after last slash
                        last_slash = local_path.rfind("/")
                        index = local_path.rfind(".", (last_slash if last_slash != -1 else 0))
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
                    self.__mdf_authorizer.set_authorization_header(headers)
                    response = requests.get(host+remote_path, headers=headers)
                    # Handle first 401 by regenerating auth headers
                    if response.status_code == 401:
                        self.__mdf_authorizer.handle_missing_authorization()
                        self.__mdf_authorizer.set_authorization_header(headers)
                        self.response = requests.get(host+remote_path, headers=headers)
                    # Handle other errors by passing the buck to the user
                    if response.status_code != 200:
                        print_("Error ", response.status_code, 
                               " when attempting to access '",
                               host+remote_path, "'", sep="")
                    else:
                        # Write out the binary response content
                        with open(local_path, 'wb') as output:
                            output.write(response.content)


    def globus_download(self, results, dest=".", dest_ep=None, preserve_dir=False,
                        wait_for_completion=True, verbose=True):
        """Download data files from the provided results using Globus Transfer.
        This method requires Globus Connect to be installed on the destination endpoint.

        Arguments:
        results (dict): The records from which files should be fetched.
                        This should be the return value of a search method.
        dest (str): The destination path for the data files on the local machine.
                    Default current directory.
        preserve_dir (bool): If True, the directory structure for the data files will be
                                recreated at the destination. The path to the new files 
                                will be relative to the `dest` path
                             If False, only the data files themselves will be saved.
                             Default False.
        wait_for_completion (bool): If True, will block until the transfer is finished.
                                    If False, will not block.
                                    Default True.
        verbose (bool): If True, status and progress messages will be print_ed.
                        If False, only error messages will be print_ed.
                        Default True.

        Returns:
        list of str: task IDs of the Gloubs transfers
        """
        dest = os.path.abspath(dest)
        # If results have info attached, remove it
        if type(results) is tuple:
            results = results[0]
        if not dest_ep:
            if not self.local_ep:
                self.local_ep = toolbox.get_local_ep(self.__transfer_client)
            dest_ep = self.local_ep

        # Assemble the transfer data
        tasks = {}
        filenames = set()
        for res in tqdm(results, desc="Processing records", disable= not verbose):
            found = False
            for key in tqdm(res["mdf"]["links"].keys(), desc="Fetching files", disable=True): 

                # Get the location of the data
                dl = res["mdf"]["links"][key]
                host = dl.get("globus_endpoint", None) if type(dl) is dict else None

                # If the data is on a Globus Endpoint
                # This filters keys that are not data links
                if host:
                    found = True
                    remote_path = dl["path"]
                    # local_path should be either dest + whole path or dest + filename
                    if preserve_dir:
                       # remote_path is absolute, so os.path.join does not work
                        local_path = os.path.abspath(dest + remote_path) 
                    else:
                        local_path = os.path.abspath(
                                        os.path.join(dest, 
                                            os.path.basename(remote_path)))

                    # Make dirs for storing the file if they don't exist
                    # preserve_dir doesn't matter; local_path has accounted for it already
                    try:
                        os.makedirs(os.path.dirname(local_path))
                    # If dest is current dir and preserve_dir=False, there are no dirs to make.
                    # os.makedirs() will raise FileNotFoundError (Python3 subclass of IOError).
                    # Since it means all dirs required exist, it can be swallowed.
                    except (IOError, OSError):
                        pass

                    if not preserve_dir:
                        # Check if file already exists, change filename if necessary
                        # The pattern is to add a number just before the extension
                        # (e.g., myfile(1).ext)
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

                    # Add data to a transfer data object
                    #   LW 11Aug17: TODO, handle transfers with huge number of files
                    #      - If a TransferData object is too large,
                    #           Globus might timeout before it can be completely uploaded
                    #        So, we need to be able to check the size of the TD object and,
                    #           if need be, send it early
                    if host not in tasks.keys():
                        tasks[host] = globus_sdk.TransferData(self.__transfer_client,
                                            host, dest_ep, verify_checksum=True)
                    tasks[host].add_item(remote_path, local_path)
                    filenames.add(local_path)
            if not found:
                print_("Error on record: No globus_endpoint provided.\nRecord: ", + str(res))


        # Submit the jobs
        submissions = []
        for td in tqdm(tasks.values(), desc="Submitting transfers", disable= not verbose):
            result = self.__transfer_client.submit_transfer(td)
            if result["code"] != "Accepted":
                raise globus_sdk.GlobusError("Error submitting transfer:", result["message"])
            else:
                if wait_for_completion:
                    while not self.__transfer_client.task_wait(result["task_id"], timeout=60,
                                                               polling_interval=10):
                        if verbose:
                            print_("Transferring...")
                        for event in transfer_client.task_event_list(res["task_id"]):
                            if event["is_error"]:
                                transfer_client.cancel_task(res["task_id"])
                                raise GlobusError("Error: " + event["description"])
                            if config_data["timeout"] and intervals >= timeout_intervals:
                                transfer_client.cancel_task(res["task_id"])
                                raise GlobusError("Transfer timed out.")

                submissions.append(result["task_id"])
        if verbose:
            print_("All transfers submitted")
            print_("Task IDs:", "\n".join(submissions))
        return submissions


    def http_stream(self, results, verbose=True):
        """Yield data files from the provided results using HTTPS, through a generator.
        For more than HTTP_NUM_LIMIT (defined above) files, you should use globus_download(),
            which uses Globus Transfer.

        Arguments:
        results (dict): The records from which files should be fetched.
                        This should be the return value of a search method.
        verbose (bool): If True, status and progress messages will be print_ed.
                        If False, only error messages will be print_ed.
                        Default True.

        Yields:
        str: Text of each data file.
        """
        # If results have info attached, remove it
        if type(results) is tuple:
            results = results[0]
        elif type(results) is not list:
            results = [results]
        if len(results) > HTTP_NUM_LIMIT:
            print_("Too many results supplied. Use globus_download()"
                   + " for fetching more than "
                   + str(HTTP_NUM_LIMIT)
                   + " entries.")
            yield {
                "success": False,
                "message": ("Too many results supplied. Use globus_download()"
                            + " for fetching more than "
                            + str(HTTP_NUM_LIMIT)
                            + " entries.")
                }
            return
        for res in results:
            for key in res["mdf"]["links"].keys():
                dl = res["mdf"]["links"][key]
                host = dl.get("http_host", None) if type(dl) is dict else None
                if host:
                    remote_path = dl["path"]
                    headers = {}
                    self.__mdf_authorizer.set_authorization_header(headers)
                    response = requests.get(host+remote_path, headers=headers)
                    # Handle first 401 by regenerating auth headers
                    if response.status_code == 401:
                        self.__mdf_authorizer.handle_missing_authorization()
                        self.__mdf_authorizer.set_authorization_header(headers)
                        self.response = requests.get(host+remote_path, headers=headers)
                    # Handle other errors by passing the buck to the user
                    if response.status_code != 200:
                        print_("Error ", response.status_code, " when attempting to access '",
                               host+remote_path, "'", sep="")
                        yield None
                    else:
                        yield response.text


    def http_return(self, results, verbose=True):
        """Return data files from the provided results using HTTPS.
        For more than HTTP_NUM_LIMIT (defined above) files, you should use globus_download(),
            which uses Globus Transfer.

        Arguments:
        results (dict): The records from which files should be fetched.
                        This should be the return value of a search method.
        verbose (bool): If True, status and progress messages will be print_ed.
                        If False, only error messages will be print_ed.
                        Default True.

        Returns:
        list of str: Text data of the data files.
        """
        return list(self.http_stream(results, verbose=verbose))



class Query:
    """The Query class is meant for internal Forge use. Users should not instantiate
            a Query object directly, as Forge already manages a Query,
            but advanced users may do so at their own risk.
            Using Query directly is an unsupported behavior 
            and may have unexpected results or unlisted changes in the future.

    Queries may end up wrapped in parentheses, which has no direct effect on the search.
    Adding terms must be chained with .and() or .or().
    Terms will not have spaces in between otherwise, and it is desirable to be explicit about
    which terms are required.
    """
    def __init__(self, search_client, q=None, limit=None, advanced=False):
        """Initialize the Query instance.

        Arguments:
        search_client (SearchClient): The Globus Search client to use for searching.
        q (str): The query string to start with. Default nothing.
        limit (int): The maximum number of results to return. Default None.
        advanced (bool): If True, will submit query in "advanced" mode ro enable field matches.
                         If False, only basic fulltext term matches will be supported.
                         Default False.
        """
        self.__search_client = search_client
        self.query = q or "("
        self.limit = limit
        self.advanced = advanced
        # initialized is True if something has been added to the query
        # __init__(), term(), and field() can change this value to True
        self.initialized = not self.query == "("


    def __clean_query_string(self, q):
        """Clean up a query string.
        This method does not access self, so that a search will not change state.
        """
        q = q.replace("()", "").strip()
        if q.endswith("("):
            q = q[:-1].strip()
        # Remove misplaced AND/OR/NOT at end
        if q[-3:] == "AND" or q[-3:] == "NOT":
            q = q[:-3]
        elif q[-2:] == "OR":
            q = q[:-2]

        # Balance parentheses
        while q.count("(") > q.count(")"):
            q += ")"
        while q.count(")") > q.count("("):
            q = "(" + q
        
        return q.strip()


    def clean_query(self):
        """Returns the current query, cleaned for user consumption,

        Returns:
        str: The clean current query.
        """
        return self.__clean_query_string(self.query)


    def term(self, term):
        """Add a term to the query.

        Arguments:
        term (str): The term to add.

        Returns:
        self (Query): For chaining.
        """
        self.query += term
        self.initialized = True
        return self


    def field(self, field, value):
        """Add a field:value term to the query.
        Matches will have field == value.
        This method sets advanced=True.

        Arguments:
        field (str): The field to look in for the value.
        value (str): The value to match.
 
        Returns:
        self (Query): For chaining.
        """
        # Cannot add field:value if one is blank
        if field and value:
            self.query += field + ":" + value
            # Field matches are advanced queries
            self.advanced = True
            self.initialized = True
        return self


    def operator(self, op, close_group=False):
        """Add operator between terms.
        There must be a term added before using this method.

        Arguments:
        op (str): The operator to add. Must be in the OP_LIST defined below.
        close_group (bool): If True, will end the current parenthetical group and start a new one.
                            If False, will continue current group.
                            Example: "(foo AND bar)" is one group.
                                "(foo) and (bar)" is two groups.

        Returns:
        self (Query): For chaining.
        """
        # List of allowed operators
        OP_LIST = ["AND", "OR", "NOT"]
        op = op.upper().strip()
        if op not in OP_LIST:
            print_("Error: '", op, "' is not a valid operator.", sep='')
        else:
            if close_group:
                op = ") " + op + " ("
            else:
                op = " " + op + " "
            self.query += op
        return self


    def and_join(self, close_group=False):
        """Combine terms with AND.
        There must be a term added before using this method.

        Arguments:
        close_group (bool): If True, will end the current group and start a new one.
                      If False, will continue current group.
                      Example: If the current query is "(term1"
                          .and(close_group=True) => "(term1) AND ("
                          .and(close_group=False) => "(term1 AND "

        Returns:
        self (Query): For chaining.
        """
        if not self.initialized:
            print_("Error: You must add a term before adding an operator.",
                   "The current query has not been changed.")
        else:
            self.operator("AND", close_group=close_group)
        return self


    def or_join(self, close_group=False):
        """Combine terms with OR.
        There must be a term added before using this method.

        Arguments:
        close_group (bool): If True, will end the current group and start a new one.
                      If False, will continue current group.
                      Example: If the current query is "(term1"
                          .or(close_group=True) => "(term1) OR("
                          .or(close_group=False) => "(term1 OR "

        Returns:
        self (Query): For chaining.
        """
        if not self.initialized:
            print_("Error: You must add a term before adding an operator.",
                   "The current query has not been changed.")
        else:
            self.operator("OR", close_group=close_group)
        return self


    def negate(self):
        """Negates the next term with NOT."""
        self.operator("NOT")
        return self


    def search(self, q=None, advanced=None, limit=SEARCH_LIMIT, info=False):
        """Execute a search and return the results.

        Arguments:
        q (str): The query to execute. Defaults to the current query, if any.
                 There must be some query to execute.
        advanced (bool): If True, will submit query in "advanced" mode to enable field matches.
                         If False, only basic fulltext term matches will be supported.
                         Default False.
                         This value will change to True automatically if
                            the query is built with helpers.
        limit (int): The maximum number of results to return.
                     The max for this argument is the SEARCH_LIMIT imposed by Globus Search.
        info (bool): If False, search will return a list of the results.
                     If True, search will return a tuple containing the results list
                        and other information about the query.
                     Default False.

        Returns:
        list (if info=False): The results.
        tuple (if info=True): The results, and a dictionary of query information.
        """
        if q is None:
            q = self.query
        if not q.strip("()"):
            print_("Error: No query")
            return ([], {"error": "No query"}) if info else []
        if advanced is None or self.advanced:
            advanced = self.advanced
        if limit is None:
            limit = self.limit or SEARCH_LIMIT
        if limit > SEARCH_LIMIT:
            limit = SEARCH_LIMIT

        q = self.__clean_query_string(q)

        # Simple query (max 10k results)
        qu = {
            "q": q,
            "advanced": advanced,
            "limit": limit,
            "offset": 0
            }
        res = toolbox.gmeta_pop(self.__search_client.structured_search(qu), info=info)
        # Add additional info
        if info:
            res[1]["query"] = qu
        return res


    def aggregate(self, q=None, scroll_size=SEARCH_LIMIT):
        """Gather all results that match a specific query

        Note that all aggregate queries run in advanced mode.

        Arguments:
        q (str): The query to execute. Defaults to the current query, if any.
                 There must be some query to execute.
        scroll_size (int): Maximum number of records requested per request.

        Returns:
        list of dict: All matching records
        """
        if q is None:
            q = self.query
        if not q.strip("()"):
            print_("Error: No query")
            return []

        q = self.__clean_query_string(q)
        #TODO: Remove record restriction (all entries require scroll_id)
        q += " AND mdf.resource_type:record"

        # Get the total number of records
        total = self.search(q, limit=0, advanced=True, info=True)[1]["total_query_matches"]

        # Scroll until all results are found
        output = []

        #TODO: scroll_pos = 0 (when all entries have scroll_id)
        scroll_pos = 1
        with tqdm(total=total) as pbar:
            while len(output) < total:

                # Scroll until the width is small enough to get all records
                #   `scroll_id`s are unique to each dataset. If multiple datasets
                #   match a certain query, the total number of matching records
                #   may exceed the maximum that search will return - even if the
                #   scroll width is much smaller than that maximum
                scroll_width = scroll_size
                while True:
                    struct_query = {
                        "q": "(" + q + ') AND mdf.scroll_id:>=%d AND mdf.scroll_id:<%d' % (
                                            scroll_pos, scroll_pos+scroll_width),
                        "advanced": True,
                        "limit": SEARCH_LIMIT,
                        "offset": 0
                        }
                    query = "(" + q + ') AND (mdf.scroll_id:>=%d AND mdf.scroll_id:<%d)' % (
                                            scroll_pos, scroll_pos+scroll_width)
                    results, info = self.search(query, advanced=True, info=True)

                    # Check to make sure that all the matching records were returned
                    if info["total_query_matches"] <= len(results):
                        break

                    # If not, reduce the scroll width
                    # new_width is proportional with the proportion of results returned
                    new_width = scroll_width * (len(results) // info["total_query_matches"])
                    # scroll_width should never be 0, and should only be 1 in rare circumstances
                    scroll_width = new_width if new_width > 1 else max(scroll_width//2, 1)

                # Append the results to the output
                output.extend(results)
                pbar.update(len(results))
                scroll_pos += scroll_width

        return output


    def mapping(self):
        """Fetch the mapping for the current index.

        Returns:
        dict: The full mapping for the index.
        """
        return self.__search_client.mapping()["mappings"]

