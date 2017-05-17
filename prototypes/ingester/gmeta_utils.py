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


