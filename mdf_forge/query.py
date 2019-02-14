import warnings
import globus_sdk
import mdf_toolbox
from tqdm import tqdm

# Maximum number of results per search allowed by Globus Search
SEARCH_LIMIT = 10000

# Maximum number of results to return when advanced=False
NONADVANCED_LIMIT = 10

# List of allowed operators
OP_LIST = ["AND", "OR", "NOT"]


def _clean_query_string(q):
    """Clean up a query string for searching.

    Removes unmatched parentheses and joining operators.

    Args:
        q (str): Query string to be cleaned

    Returns:
        (str) The clean query string.
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


class Query:
    """Utility class for performing queries using a GlobusSearchClient

    Notes:
        Query strings may end up wrapped in parentheses, which has no direct effect on the search.
        Adding terms must be chained with ``and()`` or ``or()``.
        Terms will not have spaces in between otherwise, and it is desirable to be explicit about
        which terms are required.
    """

    def __init__(self, search_client, q=None, advanced=False):
        """Create a Query object.

        Arguments:
            search_client (globus_sdk.SearchClient): The Globus Search client to use for searching.
            q (str): The query string to start with. **Default:** No query information
            advanced (bool): If ``True``, will submit query in "advanced" mode to
                enable field matches.
                If ``False``, only basic fulltext term matches will be supported (unless
                ``advanced`` is set after instantiation).
                **Default:** ``False``.
        """
        self.__search_client = search_client
        self.query = q or "("
        self.advanced = advanced
        # initialized is True if something has been added to the query
        # __init__(), term(), and field() can change this value to True

    @property
    def initialized(self):
        """Whether the query has been initialized"""
        return not self.query == "("

    def clean_query(self):
        """Returns the current query, cleaned for user consumption.

        Returns:
            str: The clean current query.
        """
        return _clean_query_string(self.query)

    def term(self, term):
        """Add a term to the query.

        Arguments:
            term (str): The term to add.

        Returns:
            Query: Self
        """
        self.query += term
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
            raise ValueError("You must add a search term before adding an operator.")
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
            raise ValueError("You must add a search term before adding an operator.")
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

    def search(self, index, limit=None, info=False, retries=3):
        """Execute a search and return the results, up to the ``SEARCH_LIMIT``.

        Uses the configuration currently defined by the helper arguments

        Arguments:
            index (str): Name of the index to query
            limit (int): Maximum number of entries to return. Default 10 for basic queries,
                and 10000 for advanced.
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

        # Make sure there is query information present
        q = self.clean_query()
        if not q.strip("()"):
            raise ValueError('Query not set')

        # Get the UUID fo the index from its short name
        uuid_index = mdf_toolbox.translate_index(index)

        # Set default size, if needed
        if limit is None:
            limit = SEARCH_LIMIT if self.advanced else NONADVANCED_LIMIT

        # Make the query size smaller if it is larger than what Search supports
        if limit > SEARCH_LIMIT:
            warnings.warn('Reduced result limit to from {} to the search maximum: {}'.format(
                limit, SEARCH_LIMIT
            ), RuntimeWarning)
            limit = SEARCH_LIMIT

        # Simple query (max 10k results)
        qu = {
            "q": q,
            "advanced": self.advanced,
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

        # Remove the wrapping on each entry from Globus search
        res = mdf_toolbox.gmeta_pop(search_res, info=info)

        # Add more information to output
        if info:
            res[1]["query"] = qu
            res[1]["index"] = index
            res[1]["index_uuid"] = uuid_index
            res[1]["retries"] = tries
            res[1]["errors"] = errors
        return res

    def aggregate(self, index, scroll_size=SEARCH_LIMIT):
        """Perform an advanced query, and return *all* matching results.
        Will automatically perform multiple queries in order to retrieve all results.

        Note: All ``aggregate`` queries run in advanced mode, and the state of Query will
        be changed to advanced if not already defined

        Arguments:
            index (str): Name of the index to query
            scroll_size (int): Maximum number of records returned per query. Must be
                    between one and the ``SEARCH_LIMIT`` (inclusive).
                    **Default:** ``SEARCH_LIMIT``.

        Returns:
            list of dict: All matching entries.
        """

        # Warn the user we are changing the setting of advanced
        if not self.advanced:
            warnings.warn('Changing the setting of this query to advanced', RuntimeWarning)

        # Make sure the query has been set
        q = self.clean_query()
        if not q.strip("()"):
            raise ValueError('Query not set')

        # Inform the user if they set an invalid value for the query size
        if scroll_size <= 0:
            raise AttributeError('Scroll size must greater than zero')

        # Get the total number of records
        total = Query(self.__search_client, q,
                      advanced=True).search(index, limit=0, info=True)[1]["total_query_matches"]

        # If aggregate is unnecessary, use Search automatically instead
        if total <= SEARCH_LIMIT:
            return Query(self.__search_client, q, advanced=True).search(index, limit=total)

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
                    results, info = Query(self.__search_client, query,
                                          advanced=True).search(index, info=True, limit=scroll_size)

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
        # TODO: Move elsewhere, I'm not sure if this has anything to do with a Query
        return (self.__search_client.get(
                    "/unstable/index/{}/mapping".format(mdf_toolbox.translate_index(index)))
                ["mappings"])
