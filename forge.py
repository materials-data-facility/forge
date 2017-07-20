import requests
import globus_sdk

# MDF Utils
from utils import globus_auth
from utils.gmeta_utils import gmeta_pop

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
    base_url = "https://search.api.globus.org/"
    index = "mdf"
    
    def __init__(self, data={}):
        self.base_url = data.get('base_url', self.base_url)
        self.index = data.get('index',self.index)
        self.search_client = 
        self.search_client = globus_auth.login(self.base_url, self.index)
    

    def search(self, q, raw=False, advanced=False):
        if not advanced:
            res = self.search_client.search(q)
        else:
            query = {
                "q": q,
                "advanced": True,
                "limit": 9999
                }
            res = self.search_client.structured_search(query)
        return res if raw else gmeta_pop(res)


    def search_by_elements(self, *elements, sources=[], match_all=False, raw=False):
        elements = list(elements)
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
            "limit":200
        }            
        
        res = self.search_client.structured_search(q)
        
        return res if raw else gmeta_pop(res)


    def get_http(self, results, dest=".", preserve_dir=False):
        if len(results) > HTTP_NUM_LIMIT:
            return {
                "success": False,
                "message": "Too many results supplied. Use get_globus() for fetching more than " + str(HTTP_NUM_LIMIT) + " entries."
                }
#        links = [ res["mdf"]["links"]["http_host"] + res["mdf"]["links"]["path"] for res in results if res["mdf"]["links"].get("http_host", None) ]
        for res in results:
            data_links = [ {"http_host": dl["http_host"], "path": dl["path"]} for dl in res["mdf"]["links"]
            if res["mdf"]["links"].get("http_host", None):
                pass
            else:
                print("No http_host found for mdf_id:" + res["mdf"]["mdf_id"] + " (" + res["mdf"]["title"] + ")")
        



    def get_globus(mdf_ids, dest=".", preserve_dir=False):
        pass





