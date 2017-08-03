import os
import pytest
import globus_sdk
from mdf_forge import toolbox


############################
# Toolbox tests
############################
'''
def test_login():
error if not credentials in any checked location
error if app_name, services not in credentials
error if bad credentials
assert proper clients returned
'''

'''
def test_confidential_login():
same as login
'''

def test_find_files():
    root = os.path.join(os.path.dirname(__file__), "testing_files")
    # Get everything
    res1 = list(toolbox.find_files(root))
    fn1 = [r["filename"] for r in res1]
    assert all([name in fn1 for name in ["2_toolbox.txt", "3_toolbox_3.txt", "4toolbox4.txt", "6_toolbox.dat", "toolbox_1.txt", "toolbox_5.csv", "txttoolbox.csv"]])
    # Check paths and no_root_paths
    for res in res1:
        assert res["path"] == os.path.join(root, res["no_root_path"])
        assert os.path.isfile(os.path.join(res["path"], res["filename"]))

    # Get everything (by regex)
    res2 = list(toolbox.find_files(root, "toolbox"))
    fn2 = [r["filename"] for r in res2]
    correct2 = ["2_toolbox.txt", "3_toolbox_3.txt", "4toolbox4.txt", "6_toolbox.dat", "toolbox_1.txt", "toolbox_5.csv", "txttoolbox.csv"]
    fn2.sort()
    correct2.sort()
    assert fn2 == correct2

    # Get only txt files
    res3 = list(toolbox.find_files(root, "txt$"))
    fn3 = [r["filename"] for r in res3]
    correct3 = ["2_toolbox.txt", "3_toolbox_3.txt", "4toolbox4.txt", "toolbox_1.txt"]
    fn3.sort()
    correct3.sort()
    assert fn3 == correct3

'''
uncompress_tree
assert all uncompressible files uncompressed
assert uncompresses tar, zip, and gzip
'''

def test_format_gmeta():
    # Simple GMetaEntry
    md1 = {
        "mdf": {
            "acl": ["public"],
            "links": {
                "landing_page": "https://example.com"
                }
            }
        }
    # More complex GMetaEntry
    md2 = {
        "mdf": {
                "title":"test",
                "acl":["public"],
                "source_name":"source name",
                "citation":["abc"],
                "links": {
                    "landing_page":"http://www.globus.org"
                },
                "data_contact":{
                    "given_name": "Test",
                    "family_name": "McTesterson",
                    "full_name": "Test McTesterson",
                    "email": "test@example.com"
                },
                "data_contributor":[{
                    "given_name": "Test",
                    "family_name": "McTesterson",
                    "full_name": "Test McTesterson",
                    "email": "test@example.com"
                }],
                "ingest_date":"Jan 1, 2017",
                "metadata_version":"1.1",
                "mdf_id":"1",
                "resource_type":"dataset"
        },
        "dc": {},
        "misc": {}
    }

    # Format both
    gme1 = toolbox.format_gmeta(md1)
    assert gme1 == {
            "@datatype": "GMetaEntry",
            "@version": "2016-11-09",
            "subject": "https://example.com",
            "visible_to": ["public"],
            "content": {
                "mdf": {
                "links": {
                    "landing_page": "https://example.com"
                    }
                }
            }
        }
    gme2 = toolbox.format_gmeta(md2)
    assert gme2 == {
            "@datatype": "GMetaEntry",
            "@version": "2016-11-09",
            "subject": "http://www.globus.org",
            "visible_to": ["public"],
            "content": {
                "mdf": {
                    "title":"test",
                    "source_name":"source name",
                    "citation":["abc"],
                    "links": {
                        "landing_page":"http://www.globus.org"
                    },
                    "data_contact":{
                        "given_name": "Test",
                        "family_name": "McTesterson",
                        "full_name": "Test McTesterson",
                        "email": "test@example.com"
                    },
                    "data_contributor":[{
                        "given_name": "Test",
                        "family_name": "McTesterson",
                        "full_name": "Test McTesterson",
                        "email": "test@example.com"
                    }],
                    "ingest_date":"Jan 1, 2017",
                    "metadata_version":"1.1",
                    "mdf_id":"1",
                    "resource_type":"dataset"
                },
            "dc": {},
            "misc": {}
            }
        }
    # Format into GMetaList
    gmlist = toolbox.format_gmeta([gme1, gme2])
    assert gmlist == {
        "@datatype": "GIngest",
        "@version": "2016-11-09",
        "ingest_type": "GMetaList",
        "ingest_data": {
            "@datatype": "GMetaList",
            "@version": "2016-11-09",
            "gmeta": [gme1, gme2]
            }
        }

    # Error if bad type
    with pytest.raises(TypeError):
        toolbox.format_gmeta(1)


def test_gmeta_pop():
    class TestResponse():
        status_code = 200
        headers = {
            "Content-Type": "json"
            }
    ghttp = globus_sdk.GlobusHTTPResponse(TestResponse())
    print(ghttp)
    assert False
#assert dict, json string, or GlobusHTTPReponse processed
#assert results properly returned from inside GMeta

'''
get_local_ep
?

SearchClient?
'''

