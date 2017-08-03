import os

import requests
import globus_sdk
from tqdm import tqdm

from mdf_forge import toolbox

HTTP_NUM_LIMIT = 10


class Forge:
    index = "mdf"
    services = ["mdf", "transfer", "search"]
    app_name = "MDF Forge"

    def __init__(self, data={}):
        self.index = data.get('index', self.index)
        self.services = data.get('services', self.services)
        self.local_ep = data.get("local_ep", None)

        clients = toolbox.login(credentials={
                                "app_name": self.app_name,
                                "services": self.services,
                                "index": self.index})
        self.search_client = clients["search"]
        self.transfer_client = clients["transfer"]
        self.mdf_authorizer = clients["mdf"]


    def match_term(self, term, match_all=True):
        return Query(self.search_client).match_term(term=term, match_all=match_all)


    def match_field(self, field, value, match_all=True):
        return Query(self.search_client).match_field(field=field, value=value, match_all=match_all)


    def match_sources(self, sources, match_all=True):
        return Query(self.search_client).match_sources(sources=sources, match_all=match_all)


    def match_elements(self, elements, match_all=True):
        return Query(self.search_client).match_elements(elements=elements, match_all=match_all)


    def search(self, q, advanced=False, limit=None, info=False):
        return Query(self.search_client).search(q=q, advanced=advanced, limit=limit, info=info)


    def search_by_elements(self, elements=[], sources=[], limit=None, match_all=False, info=False):
        return Query(self.search_client).match_elements(elements, match_all=match_all).match_sources(sources, match_all=match_all).search(limit=limit, info=info)


    def aggregate_source(self, source, limit=None):
        return Query(self.search_client).aggregate_source(source=source, limit=limit)


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


    def match_sources(self, sources, match_all=True):
        self.match_field("mdf.source_name", sources, match_all)
        return self


    def match_elements(self, elements, match_all=True):
        self.match_field("mdf.elements", elements, match_all)
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


    def execute(self, q=None, advanced=None, limit=None, info=False):
        return self.search(q, advanced, limit, info)


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


