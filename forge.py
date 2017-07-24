import os

import requests
import globus_sdk
from tqdm import tqdm

# MDF Utils
from mdf_indexers.utils import auth
from mdf_indexers.utils.gmeta_utils import gmeta_pop
from mdf_indexers.utils.globus_utils import get_local_ep

HTTP_NUM_LIMIT = 10


def build_source_list(sources=[], match_all=False):
    join_term = "AND" if match_all else "OR"
    sources = ["mdf.source_name:"+s for s in sources]
    return "("+ (' ' + join_term + ' ').join(sources) + ")"

def get_content_block(res):
    #TODO Check if GlobusHTTP ressonse
    tf = [ r['content'][0] for r in res['gmeta']]
    return tf

def get_file_http(loc, save_files=True):
    response = requests.get(loc.get('from'), stream=True)
    if not response.ok: return False
    
    # Throw an error for bad status codes
    response.raise_for_status()

    with open(loc.get('to'), 'wb') as handle:
        for block in response.iter_content(1024):
            handle.write(block)
    #return response.ok         
    
def get_files(locs=[],by_http=True, by_globus=False, n_workers=1):
    tasks = res['gmeta']
    #pbar = tqdm.tqdm(total=len(tasks)/n_workers)

    if by_globus is True:
        #perform Globus trans
        pass
    elif by_http is True:
        if n_workers>1:
            mp = Pool(n_workers)
            mdf_data = mp.map(get_file_http, locs)
            mp.close()
            mp.join()
        else:
            for loc in locs:
                get_file_http(loc)
    else:
        pass
    

class Forge:
    index = "mdf"
    services = ["mdf", "transfer", "search_ingest"]
    app_name = "MDF Forge"

    def __init__(self, data={}):
        self.index = data.get('index', self.index)
        self.services = data.get('services', self.services)
        self.local_ep = data.get("local_ep", None)

        clients = auth.login(credentials={
                                "app_name": self.app_name,
                                "services": self.services,
                                "index": self.index})
        self.search_client = clients["search"]
        self.transfer_client = clients["transfer"]
        self.mdf_authorizer = clients["mdf"]
        self.qu = Query(search_client)


    def search(self, q, raw=False, advanced=False, limit=None):
        return self.qu.search(q=q, raw=raw, advanced=advanced, limit=limit)
        self.qu = Query(self.search_client)
        '''
        # Negative limits are treated as no limit
        if limit and limit < 0:
            limit = None
        full_res = []
        # Simple query (max 10k results)
        if not advanced:
            if not limit or limit > 10000:
                limit = 10000
            query = {
                "q": q,
                "advanced": False,
                "limit": limit
                }
            res = self.search_client.structured_search(query)
            full_res += (res if raw else gmeta_pop(res))
        # Advanced query
        else:
            # If there is no limit, iterate "forever"
            # If there is a limit, iterate until that many records are returned
            while limit is None or limit > 0:
                # Perform search
                query = {
                    "q": q + " AND mdf.scroll_id:(>" + str(len(full_res)) + " AND <=" + str(len(full_res) + min(limit or 10000, 10000)) + ")",
                    "advanced": True,
                    "limit": 10000
                    }
                res = self.search_client.structured_search(query)
                num_res = len(gmeta_pop(res))
                print(num_res)
                # If results were returned, add to full_res
                if num_res > 0:
                    full_res += (res if raw else gmeta_pop(res))
                    # If a limit was set, lower future limit by number of results saved
                    if limit:
                        limit -= len(gmeta_pop(res))
                # If no results were returned, none remain, so break while loop
                else:
                    break
        return full_res
        '''


    def search_by_elements(self, elements=[], sources=[], limit=None, match_all=False, raw=False):
        self.qu.match_elements(elements, match_all=match_all)
        self.qu.match_sources(sources, match_all=match_all)
        return self.qu.search(limit=limit, raw=raw)
        self.qu = Query(self.search_client)
        '''
        q_sources = (build_source_list(sources) + " AND ") if sources else ""
        if match_all:
            q_elements = " AND ".join(["mdf.elements:"+elem for elem in elements])
        else:
            q_elements = "mdf.elements:" + ','.join(elements)
        q = {
            "q": (q_sources +
                  "mdf.resource_type:record AND " +
                  q_elements),
            "advanced": True,
            "limit":limit
        }            
        
        res = self.search_client.structured_search(q)
        
        return res if raw else gmeta_pop(res)
        '''


    def get_http(self, results, dest=".", preserve_dir=False, verbose=True):
        if type(results) is globus_sdk.GlobusHTTPResponse:
            results = gmeta_pop(results)
        if len(results) > HTTP_NUM_LIMIT:
            return {
                "success": False,
                "message": "Too many results supplied. Use get_globus() for fetching more than " + str(HTTP_NUM_LIMIT) + " entries."
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


    def get_globus(self, results, dest=".", local_ep=None, preserve_dir=False, verbose=True):
        if type(results) is globus_sdk.GlobusHTTPResponse:
            results = gmeta_pop(results)
        if not local_ep:
            if not self.local_ep:
                self.local_ep = get_local_ep(self.transfer_client)
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


class Query:
    def __init__(self, search_client, q="", limit=None, raw=False, advanced=False):
        self.search_client = search_client
        self.query = q
        self.limit = limit
        self.raw = raw
        self.advanced = False


    def match_term(self, term, match_all=True):
        self.query += (" AND " if match_all else " OR ") + term


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


    def match_sources(self, sources, match_all=True):
        self.match_field("mdf.source_name", sources, match_all)


    def match_elements(self, elements, match_all=True):
        self.match_field("mdf.elements", elements, match_all)


    def search(self, q=None, raw=None, advanced=None, limit=None):
        if q is None:
            q = self.query
        if not q:
            print("Error: No query specified")
            return []
        if raw is None:
            raw = self.raw
        if advanced is None or self.advanced:
            advanced = self.advanced
        if limit is None:
            limit = self.limit

        # Clean query string
        q = q.strip()
        removes = ["AND", "OR"]
        for rterm in removes:
            if q.startswith(rterm):
                q = q[len(rterm):]
            if q.endswith(rterm):
                q = q[:-len(rterm)]
        q = q.strip()


        full_res = []
        # Simple query (max 10k results)
        if not advanced:
            if not limit or limit > 10000:
                limit = 10000
            query = {
                "q": q,
                "advanced": False,
                "limit": limit
                }
            res = self.search_client.structured_search(query)
            full_res += (res if raw else gmeta_pop(res))
        # Advanced query
        else:
            # If there is no limit, iterate "forever"
            # If there is a limit, iterate until that many records are returned
            while limit is None or limit > 0:
                # Perform search
                query = {
                    "q": q + " AND mdf.scroll_id:(>" + str(len(full_res)) + " AND <=" + str(len(full_res) + min(limit or 10000, 10000)) + ")",
                    "advanced": True,
                    "limit": 10000
                    }
                res = self.search_client.structured_search(query)
                num_res = len(gmeta_pop(res))
                print(num_res)
                # If results were returned, add to full_res
                if num_res > 0:
                    full_res += (res if raw else gmeta_pop(res))
                    # If a limit was set, lower future limit by number of results saved
                    if limit:
                        limit -= len(gmeta_pop(res))
                # If no results were returned, none remain, so break while loop
                else:
                    break
        return full_res

    
    def execute(self, q=None, raw=None, advanced=None, limit=None):
        self.search(q, raw, advanced, limit)



