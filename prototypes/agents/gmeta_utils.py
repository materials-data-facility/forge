import json
from globus_sdk.response import GlobusHTTPResponse


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
            "subject": data["mdf-links"]["mdf-landing_page"],
            "visible_to": data.pop("mdf-acl"),
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
        raise TypeError("Error: Cannot format '" + str(type(data)) + "' into GMeta.")

    return gmeta

"""
# Obsolete
def add_namespace(data):
    ''' Adds or expands namespaces as appropriate. '''
    namespaces = {
        "dc." : "http://datacite.org/schema/kernel-3#",
        "mdf-base." : "http://globus.org/publication-schemas/mdf-base/0.1#",
        "mdf-publish." : "http://globus.org/publish-terms/#"
        }
    default_namespace = "http://materialsdatafacility.org/#"

    if type(data) is list:
        return [add_namespace(elem) for elem in data]
    elif type(data) is dict:
        new_dict = {}
        for key, value in data.items():
            expanded = False
            for base in namespaces.keys():
                if key.startswith(base):
                    # Remove base, replace '.' with '/', prepend expanded
                    new_key = namespaces[base] + key.replace(base, "").replace(".", "/")
                    expanded = True
                    break  # Found namespace, do not need to check rest
            if not expanded:
                new_key = default_namespace + key
            
            new_dict[new_key] = add_namespace(value)
        return new_dict
    else:
        return data
"""


# Removes GMeta wrapping
# Args:
#   gmeta: Dict (or GlobusHTTPResponse, or JSON str) to unwrap
# Obsolete #   clean_namespaces: Should the script clean the URL namespaces from dict keys? Default False.
def gmeta_pop(gmeta): #, clean_namespaces=False):
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
#    if clean_namespaces:
#        results = remove_namespace(results)
    return results


"""
# Obsolete
# Removes the namespaces from dict keys
# Same thing as add_namespace, but in reverse
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
"""

