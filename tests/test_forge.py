import os
import time
import types
import pytest
from mdf_forge import forge
from mdf_forge import toolbox


# Manually logging in for Query testing
query_search_client = toolbox.login(credentials={"app_name": "MDF_Forge", "services": ["search"], "index": "mdf"})["search"]


############################
# Query tests
############################
def test_query_match_term():
    q = forge.Query(query_search_client)
    # Single match test
    q.match_term("term1")
    assert q.query == "() AND (term1"
    # Multi-match test
    q.match_term("term2")
    assert q.query == "() AND (term1) AND (term2"
    # match_all test
    q.match_term("term3", match_all=False)
    assert q.query == "() AND (term1) AND (term2 OR term3"


def test_query_match_field():
    q = forge.Query(query_search_client)
    # Single field and return value test
    assert type(q.match_field("mdf.source_name", "oqmd")) is forge.Query
    assert q.query == "() AND (mdf.source_name:oqmd"
    # Multi-field and match_all test
    q.match_field("dc.title", "sample", match_all=False)
    assert q.query == "() AND (mdf.source_name:oqmd OR dc.title:sample"
    # Auto-namespacing test
    q.match_field("composition", "Al")
    assert q.query == "() AND (mdf.source_name:oqmd OR dc.title:sample) AND (mdf.composition:Al"
    # Ensure advanced is set
    assert q.advanced


def test_query_search(capsys):
    # Error on no query
    q1 = forge.Query(query_search_client)
    assert q1.search() == []
    out, err = capsys.readouterr()
    assert "Error: No query specified" in out

    # Return info if requested
    q2 = forge.Query(query_search_client)
    res2 = q2.search(q="Al", info=False)
    assert type(res2) is list
    assert type(res2[0]) is dict
    q3 = forge.Query(query_search_client)
    res3 = q3.search(q="Al", info=True)
    assert type(res3) is tuple
    assert type(res3[0]) is list
    assert type(res3[0][0]) is dict
    assert type(res3[1]) is dict

    # Check limit
    q4 = forge.Query(query_search_client)
    res4 = q4.search("oqmd", limit=3)
    assert len(res4) == 3


def test_query_aggregate_source():
    # Test limit
    q1 = forge.Query(query_search_client)
    res1 = q1.aggregate_source("nist_xps_db", limit=30)
    assert type(res1) is list
    assert len(res1) == 30
    assert type(res1[0]) is dict
    # Test fetching whole thing
    res2 = q1.aggregate_source("nist_xps_db")
    xps_len = len(res2)
    assert xps_len > 20000
    # Test does not use query
    q3 = forge.Query(query_search_client)
    q3.match_field("mdf.notreal", "badval")
    res3 = q3.aggregate_source("nist_xps_db")
    assert len(res3) == xps_len


def test_query_aggregate():
    q = forge.Query(query_search_client)
    r = q.aggregate('mdf.source_name:oqmd AND '
                        '(oqmd.configuration:static OR oqmd.configuration:standard) '
                        'AND oqmd.converged:True AND oqmd.band_gap.value:>2')
    assert isinstance(r[0], dict)


def test_query_chaining():
    q1 = forge.Query(query_search_client)
    q1.match_field("source_name", "hopv")
    res1 = q1.search(limit=10000)
    res2 = forge.Query(query_search_client).match_field("source_name", "hopv").search(limit=10000)
    assert all([r in res2 for r in res1]) and all([r in res1 for r in res2])


############################
# Forge tests
############################

# Sample results for download testing
example_result1 = [{
        'mdf': {
            'links': {
                'landing_page': 'https://data.materialsdatafacility.org/test/test_fetch.txt',
                'txt': {
                    'globus_endpoint': '82f1b5c6-6e9b-11e5-ba47-22000b92c6ec',
                    'http_host': 'https://data.materialsdatafacility.org',
                    'path': '/test/test_fetch.txt'
                }
            }
        }
    }]
example_result2 = [{
        'mdf': {
            'links': {
                'landing_page': 'https://data.materialsdatafacility.org/test/test_fetch.txt',
                'txt': {
                    'globus_endpoint': '82f1b5c6-6e9b-11e5-ba47-22000b92c6ec',
                    'http_host': 'https://data.materialsdatafacility.org',
                    'path': '/test/test_fetch.txt'
                }
            }
        }
    }, {
        'mdf': {
            'links': {
                'landing_page': 'https://data.materialsdatafacility.org/test/test_multifetch.txt',
                'txt': {
                    'globus_endpoint': '82f1b5c6-6e9b-11e5-ba47-22000b92c6ec',
                    'http_host': 'https://data.materialsdatafacility.org',
                    'path': '/test/test_multifetch.txt'
                }
            }
        }
    }]


def test_forge_match_term():
    pass


def test_forge_match_field():
    pass


def test_forge_match_sources():
    pass
    #assert match_field with mdf.source_name


def test_forge_match_elements():
    pass
    #assert match_field with mdf.elements


def test_forge_search(capsys):
    # Error on no query
    f1 = forge.Forge()
    assert f1.search() == []
    out, err = capsys.readouterr()
    assert "Error: No query specified" in out

    # Return info if requested
    f2 = forge.Forge()
    res2 = f2.search(q="Al", info=False)
    assert type(res2) is list
    assert type(res2[0]) is dict
    f3 = forge.Forge()
    res3 = f3.search(q="Al", info=True)
    assert type(res3) is tuple
    assert type(res3[0]) is list
    assert type(res3[0][0]) is dict
    assert type(res3[1]) is dict

    # Check limit
    f4 = forge.Forge()
    res4 = f4.search("oqmd", limit=3)
    assert len(res4) == 3


def test_forge_search_by_elements():
    f1 = forge.Forge()
    f2 = forge.Forge()
    elements = ["Fe", "Al"]
    sources = ["hopv", "gw100", "nist_janaf"]
    res1, info1 = f1.match_elements(elements).match_sources(sources).search(limit=10000, info=True)
    res2, info2 = f2.search_by_elements(elements, sources, limit=10000, info=True)
    assert info1 == info2
    assert all([r in res2 for r in res1]) and all([r in res1 for r in res2])


def test_forge_aggregate_source():
    # Test limit
    f1 = forge.Forge()
    res1 = f1.aggregate_source("nist_xps_db", limit=30)
    assert type(res1) is list
    assert len(res1) == 30
    assert type(res1[0]) is dict
    # Test fetching whole thing
    res2 = f1.aggregate_source("nist_xps_db")
    xps_len = len(res2)
    assert xps_len > 20000
    # Test does not use query
    f3 = forge.Forge()
    f3.match_field("mdf.notreal", "badval")
    res3 = f3.aggregate_source("nist_xps_db")
    assert len(res3) == xps_len


def test_forge_aggregate():
    f = forge.Forge()
    r = f.aggregate('mdf.source_name:oqmd AND '
                        '(oqmd.configuration:static OR oqmd.configuration:standard) '
                        'AND oqmd.converged:True AND oqmd.band_gap.value:>2')
    assert isinstance(r[0], dict)


def test_forge_reset_query():
    f = forge.Forge()
    # Term will return results
    f.match_term("data")
    f.reset_query()
    # Specifying no query will return no results
    assert f.search() == []


def test_forge_http_download():
    f = forge.Forge()
    # Simple case
    f.http_download(example_result1)
    assert os.path.exists("./test_fetch.txt")
    os.remove("./test_fetch.txt")
    # With dest and preserve_dir
    dest_path = os.path.expanduser("~/mdf")
    f.http_download(example_result1, dest=dest_path, preserve_dir=True)
    assert os.path.exists(os.path.join(dest_path, "test", "test_fetch.txt"))
    os.remove(os.path.join(dest_path, "test", "test_fetch.txt"))
    os.rmdir(os.path.join(dest_path, "test"))
    # With multiple files
    f.http_download(example_result2, dest=dest_path)
    assert os.path.exists(os.path.join(dest_path, "test_fetch.txt"))
    assert os.path.exists(os.path.join(dest_path, "test_multifetch.txt"))
    os.remove(os.path.join(dest_path, "test_fetch.txt"))
    os.remove(os.path.join(dest_path, "test_multifetch.txt"))


def test_forge_globus_download():
    f = forge.Forge()
    # Simple case
    res1 = f.globus_download(example_result1)
    assert os.path.exists("./test_fetch.txt")
    os.remove("./test_fetch.txt")
    # With dest and preserve_dir
    dest_path = os.path.expanduser("~/mdf")
    f.globus_download(example_result1, dest=dest_path, preserve_dir=True)
    assert os.path.exists(os.path.join(dest_path, "test", "test_fetch.txt"))
    os.remove(os.path.join(dest_path, "test", "test_fetch.txt"))
    os.rmdir(os.path.join(dest_path, "test"))
    # With multiple files
    f.globus_download(example_result2, dest=dest_path)
    assert os.path.exists(os.path.join(dest_path, "test_fetch.txt"))
    assert os.path.exists(os.path.join(dest_path, "test_multifetch.txt"))
    os.remove(os.path.join(dest_path, "test_fetch.txt"))
    os.remove(os.path.join(dest_path, "test_multifetch.txt"))


def test_forge_http_stream():
    f = forge.Forge()
    # Simple case
    res1 = f.http_stream(example_result1)
    assert isinstance(res1, types.GeneratorType)
    assert res1.__next__() == "This is a test document for Forge testing. Please do not remove.\n"
    # With multiple files
    res2 = f.http_stream(example_result2)
    assert isinstance(res2, types.GeneratorType)
    assert res2.__next__() == "This is a test document for Forge testing. Please do not remove.\n"
    assert res2.__next__() == "This is a second test document for Forge testing. Please do not remove.\n"


def test_forge_http_return():
    f = forge.Forge()
    # Simple case
    res1 = f.http_return(example_result1)
    assert isinstance(res1, list)
    assert res1 == ["This is a test document for Forge testing. Please do not remove.\n"]
    # With multiple files
    res2 = f.http_return(example_result2)
    assert isinstance(res2, list)
    assert res2 == ["This is a test document for Forge testing. Please do not remove.\n", "This is a second test document for Forge testing. Please do not remove.\n"]


def test_forge_chaining():
    f1 = forge.Forge()
    f1.match_field("source_name", "hopv")
    res1 = f1.search()
    res2 = forge.Forge().match_field("source_name", "hopv").search()
    assert all([r in res2 for r in res1]) and all([r in res1 for r in res2])

