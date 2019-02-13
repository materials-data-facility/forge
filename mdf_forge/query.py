import globus_sdk
import mdf_toolbox
from tqdm import tqdm

# Maximum number of results per search allowed by Globus Search
SEARCH_LIMIT = 10000

# Maximum number of results to return when advanced=False
NONADVANCED_LIMIT = 10


class Query:
    """Utility class for performing queries using a GlobusSearchClient

    Notes:
        Query strings may end up wrapped in parentheses, which has no direct effect on the search.
        Adding terms must be chained with ``and()`` or ``or()``.
        Terms will not have spaces in between otherwise, and it is desirable to be explicit about
        which terms are required.
    """

    def __init__(self, search_client, q=None, limit=None, advanced=False):
        """Create a Query object.

        Arguments:
            search_client (globus_sdk.SearchClient): The Globus Search client to use for searching.
            q (str): The query string to start with. **Default:** Not set.
            limit (int): The maximum number of results to return. **Default:** Not set.
            advanced (bool): If ``True``, will submit query in "advanced" mode to
                enable field matches.
                If ``False``, only basic fulltext term matches will be supported (unless
                ``advanced`` is set after instantiation).
                **Default:** ``False``.
        """
        self.__search_client = search_client
        self.query = q or "("
        self.limit = limit
        self.advanced = advanced
        # initialized is True if something has been added to the query
        # __init__(), term(), and field() can change this value to True
        self.initialized = not self.query == "("

    def __clean_query_string(self, q):
        """Clean up a query string for searching.
        This method does not access self, so that a search will not change state.

        Returns:
            str: The clean query string.
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
        """Returns the current query, cleaned for user consumption.

        Returns:
            str: The clean current query.
        """
        return self.__clean_query_string(self.query)

    def term(self, term):
        """Add a term to the query.

        Arguments:
            term (str): The term to add.

        Returns:
            Query: Self
        """
        self.query += term
        self.initialized = True
        return self

    def field(self, field, value):
        """Add a ``field:value`` term to the query.
        Matches will have the ``value`` in the ``field``.

        Note:
            This method triggers advanced mode.

        Arguments:
            field (str): The field to check for the value, in Elasticsearch dot syntax.
            value (str): The value to match.

        Returns:
            Query: Self
        """
        # Cannot add field:value if one is blank
        if field and value:
            self.query += field + ":" + value
            # Field matches are advanced queries
            self.advanced = True
            self.initialized = True
        return self

    def operator(self, op, close_group=False):
        """Add an operator between terms.
        There must be a term added before using this method.
        All operators have helpers, so this method is usually not necessary to directly invoke.

        Arguments:
            op (str): The operator to add. Must be in the OP_LIST.
                close_group (bool): If ``True``, will end the current parenthetical
                group and start a new one.
                If ``False``, will continue current group.

                Example::
                    "(foo AND bar)" is one group.
                    "(foo) AND (bar)" is two groups.

        Returns:
            Query: Self
        """
        # List of allowed operators
        OP_LIST = ["AND", "OR", "NOT"]
        op = op.upper().strip()
        if op not in OP_LIST:
            print("Error: '", op, "' is not a valid operator.", sep='')
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
            close_group (bool): If ``True``, will end the current group and start a new one.
                    If ``False``, will continue current group.

                    Example::

                        If the current query is "(term1"
                        .and(close_group=True) => "(term1) AND ("
                        .and(close_group=False) => "(term1 AND "

        Returns:
            Query: Self
        """
        if not self.initialized:
            print("Error: You must add a term before adding an operator.",
                  "The current query has not been changed.")
        else:
            self.operator("AND", close_group=close_group)
        return self

    def or_join(self, close_group=False):
        """Combine terms with OR.
        There must be a term added before using this method.

        Arguments:
            close_group (bool): If ``True``, will end the current group and start a new one.
                    If ``False``, will continue current group.

                    Example:

                        If the current query is "(term1"
                        .or(close_group=True) => "(term1) OR("
                        .or(close_group=False) => "(term1 OR "

        Returns:
            Query: Self
        """
        if not self.initialized:
            print("Error: You must add a term before adding an operator.",
                  "The current query has not been changed.")
        else:
            self.operator("OR", close_group=close_group)
        return self

    def negate(self):
        """Negates the next added term with NOT.

        Returns:
            Query: Self
        """
        self.operator("NOT")
        return self

    def search(self, q=None, index=None, advanced=None, limit=None, info=False, retries=3):
        """Execute a search and return the results, up to the ``SEARCH_LIMIT``.

        Arguments:
            q (str): The query to execute. **Default:** The current helper-formed query, if any.
                    There must be some query to execute.
            index (str): The Search index to search on. **Required**.
            advanced (bool): If ``True``, will submit query in "advanced" mode
                to enable field matches and other advanced features.
                If ``False``, only basic fulltext term matches will be supported.
                **Default:** ``False`` if no helpers have been used to build the query, or
                ``True`` if helpers have been used.
            limit (int): The maximum number of results to return.
                    The max for this argument is the ``SEARCH_LIMIT`` imposed by Globus Search.
                    **Default:** ``SEARCH_LIMIT`` for advanced-mode queries,
                    ``NONADVANCED_LIMIT`` for limited-mode queries.
            info (bool): If ``False``, search will return a list of the results.
                    If ``True``, search will return a tuple containing the results list
                    and other information about the query.
                    **Default:** ``False``.
            retries (int): The number of times to retry a Search query if it fails.
                           **Default:** 3.

        Returns:
            If ``info`` is ``False``, *list*: The search results.
            If ``info`` is ``True``, *tuple*: The search results,
            and a dictionary of query information.
        """

        if q is None:
            q = self.query
        if not q.strip("()"):
            print("Error: No query")
            return ([], {"error": "No query"}) if info else []
        if index is None:
            print("Error: No index specified")
            return ([], {"error": "No index"}) if info else []
        else:
            uuid_index = mdf_toolbox.translate_index(index)
        if advanced is None or self.advanced:
            advanced = self.advanced
        if limit is None:
            limit = self.limit or (SEARCH_LIMIT if advanced else NONADVANCED_LIMIT)
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
        tries = 0
        errors = []
        while True:
            try:
                search_res = self.__search_client.post_search(uuid_index, qu)
            except globus_sdk.SearchAPIError as e:
                if tries >= retries:
                    raise
                else:
                    errors.append(repr(e))
            except Exception as e:
                if tries >= retries:
                    raise
                else:
                    errors.append(repr(e))
            else:
                break
            tries += 1
        res = mdf_toolbox.gmeta_pop(search_res, info=info)
        # Add additional info
        if info:
            res[1]["query"] = qu
            res[1]["index"] = index
            res[1]["index_uuid"] = uuid_index
            res[1]["retries"] = tries
            res[1]["errors"] = errors
        return res

    def aggregate(self, q=None, index=None, retries=1, scroll_size=SEARCH_LIMIT):
        """Perform an advanced query, and return *all* matching results.
        Will automatically perform multiple queries in order to retrieve all results.

        Note:
            All ``aggregate`` queries run in advanced mode, and ``info`` is not available.

        Arguments:
            q (str): The query to execute. The current helper-formed query, if any.
                    There must be some query to execute.
            index (str): The Search index to search on. Required.
            retries (int): The number of times to retry a Search query if it fails.
                           **Default:** 1.
            scroll_size (int): Maximum number of records returned per query. Must be
                    between one and the ``SEARCH_LIMIT`` (inclusive).
                    **Default:** ``SEARCH_LIMIT``.

        Returns:
            list of dict: All matching entries.
        """
        if q is None:
            q = self.query
        if not q.strip("()"):
            print("Error: No query")
            return []
        if index is None:
            print("Error: No index specified")
            return []
        if scroll_size <= 0:
            scroll_size = 1

        q = self.__clean_query_string(q)

        # Get the total number of records
        total = self.search(q, index=index, limit=0, advanced=True,
                            info=True)[1]["total_query_matches"]

        # If aggregate is unnecessary, use Search automatically instead
        if total <= SEARCH_LIMIT:
            return self.search(q, index=index, limit=SEARCH_LIMIT, advanced=True)

        # Scroll until all results are found
        output = []

        scroll_pos = 0
        with tqdm(total=total) as pbar:
            while len(output) < total:

                # Scroll until the width is small enough to get all records
                #   `scroll_id`s are unique to each dataset. If multiple datasets
                #   match a certain query, the total number of matching records
                #   may exceed the maximum that search will return - even if the
                #   scroll width is much smaller than that maximum
                scroll_width = scroll_size
                while True:
                    query = "(" + q + ') AND (mdf.scroll_id:>=%d AND mdf.scroll_id:<%d)' % (
                                            scroll_pos, scroll_pos+scroll_width)
                    results, info = self.search(query, index=index, advanced=True,
                                                info=True, retries=retries)

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

    def mapping(self, index):
        """Fetch the entire mapping for the specified index.

        Arguments:
            index (str): The index to map.

        Returns:
            dict: The full mapping for the index.
        """
        return (self.__search_client.get(
                    "/unstable/index/{}/mapping".format(mdf_toolbox.translate_index(index)))
                ["mappings"])
