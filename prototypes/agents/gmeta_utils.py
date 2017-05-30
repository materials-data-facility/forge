import json
from globus_sdk.response import GlobusHTTPResponse
# Removes GMeta wrapping
# Args:
#   gmeta: Dict (or GlobusHTTPResponse, or JSON str) to unwrap
#   clean_namespaces: Should the script clean the URL namespaces from dict keys? Default False.
def gmeta_pop(gmeta, clean_namespaces=False):
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
    if clean_namespaces:
        results = remove_namespace(results)
    return results

# Removes the namespaces from dict keys
# Same thing as add_namespaces in data_ingester.py, but in reverse
def remove_namespace(data):
    if type(data) is list:
        return [remove_namespace(elem) for elem in data]
    elif type(data) is dict:
        new_dict = {}
        for key, value in data.items():
            if key.startswith("http"):
                new_key = key[key.find("#")+1:]
                new_dict[new_key] = remove_namespace(value)
        return new_dict
    else:
        return data

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
        data["mdf-publish.publication.community"] = "Materials Data Facility"  # Community for filtering
        gmeta = {
            "@datatype": "GMetaEntry",
            "@version": "2016-11-09",
            "subject": data.pop("globus_subject"),
            "visible_to": data.pop("acl"),
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
        sys.exit("Error: Cannot format '" + str(type(data)) + "' into GMeta.")

    return gmeta
