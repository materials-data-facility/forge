import os
from urllib.parse import urlparse

import globus_sdk
import mdf_toolbox
import requests


from tqdm import tqdm

# Maximum recommended number of HTTP file transfers
#  Large transfers are much better suited for Globus Transfer
HTTP_NUM_LIMIT = 50


class Forge(mdf_toolbox.AggregateHelper, mdf_toolbox.SearchHelper):
    """Forge fetches metadata and files from the Materials Data Facility.
    Forge is intended to be the best way to access MDF data for all users.
    An internal Query object is used to make queries. From the user's perspective,
    an instantiation of Forge will black-box searching.
    """
    __default_index = "mdf"
    __scroll_field = "mdf.scroll_id"
    __auth_services = ["data_mdf", "transfer", "search", "petrel"]
    __anon_services = ["search"]
    __app_name = "MDF_Forge"
    __transfer_interval = 60  # 1 minute, in seconds
    __inactivity_time = 1 * 60 * 60  # 1 hour, in seconds

    def __init__(self, index=__default_index, local_ep=None, anonymous=False,
                 clear_old_tokens=False, **kwargs):
        """Create an MDF Forge Client.

        Arguments:
            index (str): The Search index to search on. **Default:** ``"mdf"``.
            local_ep (str): The endpoint ID of the local Globus Connect Personal endpoint.
                    If needed but not provided, the local endpoint will be autodetected
                    if possible.
            anonymous (bool): If ``True``, will not authenticate with Globus Auth.
                    If ``False``, will require authentication.
                    **Default:** ``False``.

                    Caution:
                        Authentication is required for some Forge functionality,
                        including viewing private datasets and using Globus Transfer.

            clear_old_tokens (bool): If ``True``, will force reauthentication.
                    If ``False``, will use existing tokens if possible.
                    Has no effect if ``anonymous`` is ``True``.
                    **Default:** ``False``.

        Keyword Arguments:
            services (list of str): *Advanced users only.* The services to authenticate with,
                    using Toolbox. An empty list will disable authenticating with Toolbox.
                    Note that even overwriting clients (with other keyword arguments)
                    does not stop Toolbox authentication. Only a blank ``services`` argument
                    will disable Toolbox authentication.
            search_client (globus_sdk.SearchClient): An authenticated SearchClient
                    to overwrite the default.
            transfer_client (globus_sdk.TransferClient): An authenticated TransferClient
                    to override the default.
            data_mdf_authorizer (globus_sdk.GlobusAuthorizer): An authenticated GlobusAuthorizer
                    to overwrite the default for accessing the MDF NCSA endpoint.
            petrel_authorizer (globus_sdk.GlobusAuthorizer): An authenticated GlobusAuthorizer
                    to override the default.
        """
        self.__anonymous = anonymous
        self.local_ep = local_ep

        if self.__anonymous:
            services = kwargs.get('services', self.__anon_services)
            clients = (mdf_toolbox.anonymous_login(services) if services else {})
        else:
            services = kwargs.get('services', self.__auth_services)
            clients = (mdf_toolbox.login(
                                        credentials={
                                            "app_name": self.__app_name,
                                            "services": services},
                                        clear_old_tokens=clear_old_tokens) if services else {})
        search_client = kwargs.pop("search_client", clients.get("search", None))
        self.__transfer_client = kwargs.get("transfer_client", clients.get("transfer", None))
        self.__data_mdf_authorizer = kwargs.get("data_mdf_authorizer",
                                                clients.get("data_mdf",
                                                            globus_sdk.NullAuthorizer()))
        self.__petrel_authorizer = kwargs.get("petrel_authorizer",
                                              clients.get("petrel",
                                                          globus_sdk.NullAuthorizer()))
        super().__init__(index=index, search_client=search_client,
                         scroll_field=self.__scroll_field, **kwargs)

    # ***********************************************
    # * Field-specific helpers
    # ***********************************************

    def match_source_names(self, source_names):
        """Add sources to match to the query.

        Arguments:
            source_names (str or list of str): The ``source_name`` values to match.

        Returns:
            Forge: Self
        """
        # If no source_names are supplied, nothing to match
        if not source_names:
            return self
        if isinstance(source_names, str):
            source_names = [source_names]
        # First source should be in new group and required
        self.match_field(field="mdf.source_name", value=source_names[0],
                         required=True, new_group=True)
        # Other sources should stay in that group, and not be required
        for src in source_names[1:]:
            self.match_field(field="mdf.source_name", value=src, required=False, new_group=False)
        return self

    def match_ids(self, mdf_ids):
        """Match the IDs in the given ``mdf_id`` list.

        Arguments:
            mdf_ids (str or list of str): The IDs to match.

        Returns:
            Forge: Self
        """
        # If no IDs are supplied, nothing to match
        if not mdf_ids:
            return self
        if isinstance(mdf_ids, str):
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
            elements (str or list of str): The elements to match. For example, `"Fe"` for iron.
            match_all (bool): If ``True``, will add with ``AND``.
                    If ``False``, will use ``OR``.
                    Default ``True``.

        Returns:
            Forge: Self
        """
        # If no elements are supplied, nothing to match
        if not elements:
            return self
        if isinstance(elements, str):
            elements = [elements]
        # First element should be in new group and required
        self.match_field(field="material.elements", value=elements[0],
                         required=True, new_group=True)
        # Other elements should stay in that group
        for element in elements[1:]:
            self.match_field(field="material.elements", value=element, required=match_all,
                             new_group=False)
        return self

    def match_titles(self, titles):
        """Add titles to the query.

        Arguments:
            titles (str or list of str): The titles to match.

        Returns:
            Forge: Self
        """
        if not titles:
            return self
        if not isinstance(titles, list):
            titles = [titles]

        self.match_field(field="dc.titles.title", value=titles[0], required=True, new_group=True)
        for title in titles[1:]:
            self.match_field(field="dc.titles.title", value=title, required=False, new_group=False)
        return self

    def match_years(self, years=None, start=None, stop=None, inclusive=True):
        """Add years and limits to the query.

        Arguments:
            years (int or string, or list of int or strings): The years to match.
                    Note that this argument overrides the start, stop, and inclusive arguments.
            start (int or string): The lower range of years to match.
            stop (int or string): The upper range of years to match.
            inclusive (bool): If ``True``, the start and stop values will be included
                    in the search. If ``False``, they will be excluded.
                    **Default:** ``True``.
        Returns:
            Forge: Self
        """
        # If nothing supplied, nothing to match
        if years is None and start is None and stop is None:
            return self  # No filtering if no filters provided

        if years is not None and years != []:
            if not isinstance(years, list):
                years = [years]
            years_int = []
            for year in years:
                try:
                    y_int = int(year)
                    years_int.append(y_int)
                except ValueError:
                    raise AttributeError("Invalid year: '{}'".format(year))

            # Only match years if valid years were supplied
            if len(years_int) > 0:
                self.match_field(field="dc.publicationYear", value=years_int[0], required=True,
                                 new_group=True)
                for year in years_int[1:]:
                    self.match_field(field="dc.publicationYear",
                                     value=year, required=False, new_group=False)
        else:
            if start is not None:
                try:
                    start = int(start)
                except ValueError:
                    raise AttributeError("Invalid start year: '{}'".format(start))
            if stop is not None:
                try:
                    stop = int(stop)
                except ValueError:
                    raise AttributeError("Invalid stop year: '{}'".format(stop))

            self.match_range(field="dc.publicationYear", start=start, stop=stop,
                             inclusive=inclusive, required=True, new_group=True)
        return self

    def match_resource_types(self, types):
        """Match the given resource types.

        Arguments:
            types (str or list of str): The ``resource_type`` values to match.

        Returns:
            Forge: Self
        """
        # If no types, nothing to match
        if not types:
            return self
        if isinstance(types, str):
            types = [types]
        # First type should be in new group and required
        self.match_field(field="mdf.resource_type", value=types[0], required=True, new_group=True)
        # Other IDs should stay in that group, and not be required
        for rt in types[1:]:
            self.match_field(field="mdf.resource_type", value=rt, required=False, new_group=False)
        return self

    def match_organizations(self, organizations, match_all=True):
        """Match the given Organizations.
        Organizations are MDF-registered groups that can apply rules to datasets.

        Arguments:
            organizations (str or list of str): The organizations to match.
            match_all (bool): If ``True``, will add with ``AND``.
                    If ``False``, will use ``OR``.
                    **Default:** ``True``.

        Returns:
            Forge: Self
        """
        # If no orgs, nothing to match
        if not organizations:
            return self
        if isinstance(organizations, str):
            organizations = [organizations]
        # First org should be in new group and required
        self.match_field(field="mdf.organizations", value=organizations[0],
                         required=True, new_group=True)
        # Other elements should stay in that group
        for org in organizations[1:]:
            self.match_field(field="mdf.organizations", value=org, required=match_all,
                             new_group=False)
        return self

    def match_dois(self, dois):
        """Match the given Digital Object Identifiers.

        Arguments:
            dois (str or list of str): DOIs to match and return.

        Returns:
            Forge: self
        """
        if not dois:
            return self
        if isinstance(dois, str):
            dois = [dois]
        # Sanitize DOIs - usually contain problem characters
        # First doi should be in new group and required
        self.match_field(field="dc.identifier.identifier", value=dois[0],
                         required=True, new_group=True)
        # Other sources should stay in that group, and not be required
        for doi in dois[1:]:
            self.match_field(field="dc.identifier.identifier", value=doi,
                             required=False, new_group=False)
        return self

    # ***********************************************
    # * Premade searches
    # ***********************************************

    def search_by_elements(self, elements, source_names=[], index=None, limit=None,
                           match_all=True, info=False):
        """Execute a search for the given elements in the given sources.
        ``search_by_elements([x], [y])`` is equivalent to
        ``match_elements([x]).match_source_names([y]).search()``.

        Note:
            This method will use terms from the current query, and resets the current query.

        Arguments:
            elements (list of str): The elements to match. For example, `"Fe"` for iron.
            source_names (list of str): The ``source_name``s to match.
                    **Default:** ``[]``.
            index (str): The Search index to search on. **Default:** The current index.
            limit (int): The maximum number of results to return.
                    The max for this argument is the ``SEARCH_LIMIT`` imposed by Globus Search.
                    **Default:** ``SEARCH_LIMIT``.
            match_all (bool): If ``True``, will add elements with ``AND``.
                    If ``False``, will use ``OR``.
                    **Default:** ``True``.
            info (bool): If ``False``, search will return a list of the results.
                    If ``True``, search will return a tuple containing the results list
                    and other information about the query.
                    **Default:** ``False``.

        Returns:
            If ``info`` is ``False``, *list*: The search results.
            If ``info`` is ``True``, *tuple*: The search results,
            and a dictionary of query information.
        """
        return (self.match_elements(elements, match_all=match_all)
                    .match_source_names(source_names)
                    .search(limit=limit, info=info))

    def search_by_titles(self, titles, index=None, limit=None, info=False):
        """Execute a search for the given titles.
        ``search_by_titles([x])`` is equivalent to ``match_titles([x]).search()``

        Note:
            This method will use terms from the current query, and resets the current query.

        Arguments:
            titles (list of str): The titles to match.
            index (str): The Search index to search on. **Default:** The current index.
            limit (int): The maximum number of results to return.
                    The max for this argument is the ``SEARCH_LIMIT`` imposed by Globus Search.
                    **Default:** ``SEARCH_LIMIT``.
            info (bool): If ``False``, search will return a list of the results.
                    If ``True``, search will return a tuple containing the results list
                    and other information about the query.
                    **Default:** ``False``.

        Returns:
            If ``info`` is ``False``, *list*: The search results.
            If ``info`` is ``True``, *tuple*: The search results,
            and a dictionary of query information.
        """
        return self.match_titles(titles).search(limit=limit, info=info)

    def search_by_dois(self, dois, index=None, limit=None, info=False):
        """Execute a search for the given Digital Object Identifiers.
        ``search_by_dois([x])`` is equivalent to ``match_dois([x]).search()``

        Note:
            This method will use terms from the current query, and resets the current query.

        Arguments:
            dois (list of str): The DOIs to find.
            index (str): The Search index to search on. **Default:** The current index.
            limit (int): The maximum number of results to return.
                    The max for this argument is the ``SEARCH_LIMIT`` imposed by Globus Search.
                    **Default:** ``SEARCH_LIMIT``.
            info (bool): If ``False``, search will return a list of the results.
                    If ``True``, search will return a tuple containing the results list
                    and other information about the query.
                    **Default:** ``False``.

        Returns:
            If ``info`` is ``False``, *list*: The search results.
            If ``info`` is ``True``, *tuple*: The search results,
            and a dictionary of query information.
        """
        return self.match_dois(dois).search(limit=limit, info=info)

    def aggregate_sources(self, source_names, index=None):
        """Aggregate all records with the given ``source_name`` values.
        There is no limit to the number of results returned.
        Please beware of aggregating very large datasets.

        Caution:
            It is recommended that you check how many entries will be returned from your chosen
            datasets by running ``match_source_names(source_names).search(limit=0, info=True)``
            before using ``aggregate_sources()``.

        Note:
            This method will use terms from the current query, and resets the current query.

        Arguments:
            source_names (str or list of str): The ``source_name`` values to aggregate.
            index (str): The Search index to search on. **Default:** The current index.

        Returns:
            list of dict: All of the entries from the ``source_name`` matches.
        """
        return self.match_source_names(source_names).aggregate(index=index)

    def fetch_datasets_from_results(self, entries=None, query=None, reset_query=True):
        """Retrieve the dataset entries for given records.
        Note that this method may use the current query.

        Note:
            This method will use terms from the current query, and resets the current query.

        Arguments:
            entries (dict, list of dict, or tuple of dict): The records to parse
                    to find the datasets. This argument can be a single entry,
                    a list of entries, or a tuple with a list of entries.
                    The latter two options support both return values of the ``search()`` method.
                    If entries is ``None``, the current query is executed and those
                    results are used instead. **Default:** ``None``.
            query (str): If not ``None``, search for entries using this query
                    instead of the current query. Has no effect if ``entries`` is not ``None``.
                    **Default:** ``None``.
            reset_query (bool): Has no effect unless ``entries`` and ``query`` are both ``None``.
                    If ``True``, will reset the current query after searching for entries.
                    If ``False``, will not reset the current query.
                    **Default:** ``True``.

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
                ds_ids.add(entry["mdf"]["parent_id"])
            # For datasets, extract the mdf_id
            elif entry["mdf"]["resource_type"] == "dataset":
                ds_ids.add(entry["mdf"]["mdf_id"])
            # For anything else (collection), do nothing
            else:
                pass

        # If no ids are preset, raise an error
        if len(ds_ids) == 0:
            raise AttributeError('No dataset records found in these entries')
        return self.match_ids(list(ds_ids)).search()

    def get_dataset_version(self, source_name):
        """Get the version of a certain dataset.

        Arguments:
            source_name (string): The ``source_name`` of the dataset.

        Returns:
            int: Version of the dataset in question.
        """

        hits = self.search("mdf.source_name:{} AND"
                           " mdf.resource_type:dataset".format(source_name),
                           advanced=True, limit=2)

        # Some error checking
        if len(hits) == 0:
            raise ValueError("No such dataset found: " + source_name)
        elif len(hits) > 1:
            raise ValueError("Unexpectedly matched multiple datasets with source_name '{}'. "
                             "Please contact MDF support.".format(source_name))
        else:
            return hits[0]['mdf']['version']

    # ***********************************************
    # * Data retrieval functions
    # ***********************************************

    def http_download(self, results, dest=".", preserve_dir=False, verbose=True):
        """Download data files from the provided results using HTTPS.
        For a large number of files, you should use ``globus_download()`` instead,
        which uses Globus Transfer.

        Arguments:
            results (dict): The records from which files should be fetched.
                    This should be the return value of a search method.
            dest (str): The destination path for the data files on the local machine.
                    **Default:** The current directory.
            preserve_dir (bool): If ``True``, the directory structure for the data files will be
                    recreated at the destination.
                    If ``False``, only the data files themselves will be saved.
                    **Default:** ``False``.
            verbose (bool): If ``True``, status and progress messages will be printed.
                    If ``False``, only error messages will be printed.
                    **Default:** ``True``.

        Returns:
            *dict*: The status information for the download:
                    * **success** (*bool*): ``True`` if the download succeeded. ``False``
                        if it failed.
                    * **message** (*str*): The error message, if the download failed.
        """
        if self.__anonymous:
            print("Error: Anonymous HTTP download not yet supported.")
            return {
                "success": False,
                "message": "Anonymous HTTP download not yet supported."
                }
        # If user submitted single result, make into list
        if isinstance(results, dict):
            results = [results]
        # If results have info attached, remove it
        elif isinstance(results, tuple):
            results = results[0]
        if len(results) > HTTP_NUM_LIMIT:
            print("Error: Too many results supplied. Use globus_download()"
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
        for res in tqdm(results, desc="Fetching files", disable=(not verbose)):
            if res["mdf"]["resource_type"] == "dataset":
                print("Skipping datset entry for '{}': Cannot download dataset over HTTPS. "
                      "Use globus_download() for datasets.".format(res["mdf"]["source_id"]))
            elif res["mdf"]["resource_type"] == "record":
                for dl in res.get("files", []):
                    url = dl.get("url", None)
                    if url:
                        parsed_url = urlparse(url)
                        remote_path = parsed_url.path
                        # local_path should be either dest + whole path or dest + filename
                        if preserve_dir:
                            local_path = os.path.normpath(dest + "/" + remote_path)
                        else:
                            local_path = os.path.normpath(dest + "/"
                                                          + os.path.basename(remote_path))
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
                            # Save and remove extension
                            local_path, ext = os.path.splitext(local_path)
                            # Check if already added number to end
                            old_add = "("+str(collisions)+")"
                            collisions += 1
                            new_add = "("+str(collisions)+")"
                            # Remove old number if exists
                            if local_path.endswith(old_add):
                                local_path = local_path[:-len(old_add)]
                            # Add new number
                            local_path = local_path + new_add + ext

                        headers = {}
                        # Check for Petrel vs. NCSA url for authorizer
                        # Petrel
                        if parsed_url.netloc == "e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org":
                            authorizer = self.__petrel_authorizer
                        elif parsed_url.netloc == "data.materialsdatafacility.org":
                            authorizer = self.__data_mdf_authorizer
                        else:
                            authorizer = globus_sdk.NullAuthorizer()
                        authorizer.set_authorization_header(headers)
                        response = requests.get(url, headers=headers)
                        # Handle first 401 by regenerating auth headers
                        if response.status_code == 401:
                            authorizer.handle_missing_authorization()
                            authorizer.set_authorization_header(headers)
                            response = requests.get(url, headers=headers)
                        # Handle other errors by passing the buck to the user
                        if response.status_code != 200:
                            print("Error {} when attempting to access "
                                  "'{}'".format(response.status_code, url))
                        else:
                            # Write out the binary response content
                            with open(local_path, 'wb') as output:
                                output.write(response.content)
            else:
                print("Error: Found unknown resource_type '{}'. "
                      "Skipping entry.".format(res["mdf"]["resource_type"]))
        return {
            "success": True
            }

    def globus_download(self, results, dest=".", dest_ep=None, preserve_dir=False,
                        inactivity_time=None, download_datasets=False, verbose=True):
        """Download data files from the provided results using Globus Transfer.
        This method requires Globus Connect to be installed on the destination endpoint.

        Arguments:
            results (dict): The records from which files should be fetched.
                    This should be the return value of a search method.
            dest (str): The destination path for the data files on the local machine.
                    **Default:** The current directory.
            dest_ep (str): The destination endpoint ID. **Default:** The autodetected local GCP.
            preserve_dir (bool): If ``True``, the directory structure for the data files will be
                    recreated at the destination. The path to the new files
                    will be relative to the ``dest`` path
                    If ``False``, only the data files themselves will be saved.
                    **Default:** ``False``.
            inactivity_time (int): Number of seconds the Transfer is allowed to go without progress
                    before being cancelled.
                    **Default:** ``self.__inactivity_time``.
            download_datasets (bool): If ``True``, will download the full dataset for any dataset
                    entries given.
                    If ``False``, will skip dataset entries with a notification.
                    **Default:** ``False``.

                    Caution:
                        Datasets can be large. Additionally, if you do not
                        filter out records from a dataset you provide, you may end
                        up with duplicate files. Use with care.

            verbose (bool): If ``True``, status and progress messages will be printed,
                    and errors will prompt for continuation confirmation.
                    If ``False``, only error messages will be printed,
                    and the Transfer will always continue.
                    **Default:** ``True``.

        Returns:
            list of str: The task IDs of the Globus transfers.
        """
        if self.__anonymous:
            print("Error: Anonymous Globus Transfer not supported.")
            return {
                "success": False,
                "message": "Anonymous Globus Transfer not supported."
                }
        dest = os.path.abspath(dest)
        # If results have info attached, remove it
        if type(results) is tuple:
            results = results[0]
        if not dest_ep:
            if not self.local_ep:
                self.local_ep = globus_sdk.LocalGlobusConnectPersonal().endpoint_id
            dest_ep = self.local_ep
        if not inactivity_time:
            inactivity_time = self.__inactivity_time

        # Assemble the transfer data
        tasks = {}
        filenames = set()
        links_processed = set()
        for res in tqdm(results, desc="Processing records", disable=(not verbose)):
            file_list = []
            if res["mdf"]["resource_type"] == "dataset":
                if download_datasets:
                    g_link = res.get("data", {}).get("endpoint_path", None)
                    if g_link:
                        file_list.append(g_link)
                elif verbose:
                    print("Skipping dataset '{}' because argument 'download_datasets' is "
                          "False. Use caution if enabling.".format(res["mdf"]["source_id"]))
            elif res["mdf"]["resource_type"] == "record":
                for dl in res.get("files", []):
                    g_link = dl.get("globus", None)
                    if g_link:
                        file_list.append(g_link)
            else:
                print("Error: Found unknown resource_type '{}'. "
                      "Skipping entry.".format(res["mdf"]["resource_type"]))
            for globus_link in file_list:
                # If the data is on a Globus Endpoint
                if globus_link not in links_processed:
                    links_processed.add(globus_link)
                    ep_id = urlparse(globus_link).netloc
                    ep_path = urlparse(globus_link).path
                    # local_path should be either dest + whole path or dest + filename
                    if preserve_dir:
                        # ep_path is absolute, so os.path.join does not work
                        local_path = os.path.abspath(dest + ep_path)
                    else:
                        # If ep_path is to dir, basename is ''
                        # basename(dirname(ep_path)) gives just first dir's name
                        base_name = os.path.basename(ep_path)
                        if not base_name:
                            base_name = os.path.basename(os.path.dirname(ep_path))
                        local_path = os.path.abspath(os.path.join(dest, base_name))

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
                            # Get extension, if exists
                            base_file, ext = os.path.splitext(local_path)
                            # Check if already added number to end
                            old_add = "("+str(collisions)+")"
                            collisions += 1
                            new_add = "("+str(collisions)+")"
                            if base_file.endswith(old_add):
                                local_path = base_file[:-len(old_add)] + new_add + ext
                            else:
                                local_path = base_file + new_add + ext

                    # If ep_path points to a dir, the trailing slash has been removed
                    if ep_path.endswith("/"):
                        local_path += "/"
                    # Add data to list of transfer files
                    if ep_id not in tasks.keys():
                        tasks[ep_id] = []
                    tasks[ep_id].append((ep_path, local_path))
                    filenames.add(local_path)

        # Submit the jobs
        success = 0
        failed = 0
        for task_ep, task_paths in tqdm(tasks.items(), desc="Transferring data",
                                        disable=(not verbose)):
            transfer = mdf_toolbox.custom_transfer(self.__transfer_client, task_ep, dest_ep,
                                                   task_paths, interval=self.__transfer_interval,
                                                   inactivity_time=inactivity_time)
            cont = True
            # Prime loop
            event = next(transfer)
            try:
                # Loop ends on StopIteration from generator exhaustion
                while True:
                    if not event["success"] and cont:
                        print("Error: {} - {}".format(event["code"], event["description"]))
                        if verbose:
                            # Allow user to abort transfer if verbose, else cont is always True
                            user_cont = input("Continue Transfer (y/n)?\n")
                            cont = (user_cont.strip().lower() == "y"
                                    or user_cont.strip().lower() == "yes")
                    event = transfer.send(cont)
            except StopIteration:
                pass
            if not event["success"]:
                print("Error transferring with endpoint '{}': {} - "
                      "{}".format(task_ep, event["status"],
                                  event["nice_status_short_description"]))
                failed += 1
                # Allow cancellation of remaining Transfers if Transfer are remaining
                if verbose and list(tasks.keys())[-1] != task_ep:
                    user_cont = input("Continue Transfer (y/n)?\n")
                    if not (user_cont.strip().lower() == "y"
                            or user_cont.strip().lower() == "yes"):
                        break
            else:
                success += 1

        if verbose:
            print("All transfers processed\n{} transfers succeeded\n"
                  "{} transfers failed".format(success, failed))
        return

    def http_stream(self, results, verbose=True):
        """Yield data files from the provided results using HTTPS, through a generator.
        For a large number of files, you should use ``globus_download()`` instead,
        which uses Globus Transfer.

        Arguments:
            results (dict): The records from which files should be fetched.
                    This should be the return value of a search method.
            verbose (bool): If ``True``, status and progress messages will be printed.
                    If ``False``, only error messages will be printed.
                    **Default:** ``True``.

        Yields:
            str: Text of each data file.
        """
        if self.__anonymous:
            print("Error: Anonymous HTTP download not yet supported.")
            yield {
                "success": False,
                "message": "Anonymous HTTP download not yet supported."
                }
            return
        # If results have info attached, remove it
        if type(results) is tuple:
            results = results[0]
        if type(results) is not list:
            results = [results]
        if len(results) > HTTP_NUM_LIMIT:
            print("Too many results supplied. Use globus_download()"
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
            for dl in res.get("files", []):
                url = dl.get("url", None)
                if url:
                    parsed_url = urlparse(url)
                    headers = {}
                    # Check for Petrel vs. NCSA url for authorizer
                    # Petrel
                    if parsed_url.netloc == "e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org":
                        authorizer = self.__petrel_authorizer
                    elif parsed_url.netloc == "data.materialsdatafacility.org":
                        authorizer = self.__data_mdf_authorizer
                    else:
                        authorizer = globus_sdk.NullAuthorizer()
                    authorizer.set_authorization_header(headers)
                    response = requests.get(url, headers=headers)
                    # Handle first 401 by regenerating auth headers
                    if response.status_code == 401:
                        authorizer.handle_missing_authorization()
                        authorizer.set_authorization_header(headers)
                        response = requests.get(url, headers=headers)
                    # Handle other errors by passing the buck to the user
                    if response.status_code != 200:
                        print("Error ", response.status_code, " when attempting to access '",
                              url, "'", sep="")
                        yield None
                    else:
                        yield response.text
