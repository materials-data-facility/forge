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
        gmeta = {
            "@datatype": "GMetaEntry",
            "@version": "2016-11-09",
            "subject": data["mdf"]["links"]["landing_page"],
            "visible_to": data["mdf"].pop("acl"),
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


# Removes GMeta wrapping
# Args:
#   gmeta: Dict (or GlobusHTTPResponse, or JSON str) to unwrap
def gmeta_pop(gmeta):
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
    return results

