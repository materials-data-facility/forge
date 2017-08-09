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
    assert q.query == " AND term1"
    # Multi-match test
    q.match_term("term2")
    assert q.query == " AND term1 AND term2"
    # match_all test
    q.match_term("term3", match_all=False)
    assert q.query == " AND term1 AND term2 OR term3"


def test_aggregate():
    f = forge.Forge()
    r = forge.aggregate('mdf.source_name:oqmd AND '
                        '(oqmd.configuration:static OR oqmd.configuration:standard) '
                        'AND oqmd.converged:True AND oqmd.band_gap.value:>2')
    assert isinstance(r[0], dict)

def test_query_match_field():
    q = forge.Query(query_search_client)
    # Single field and return value test
    assert type(q.match_field("mdf.source_name", "oqmd")) is forge.Query
    assert q.query == " AND mdf.source_name:oqmd"
    # Multi-field and match_all test
    q.match_field("dc.title", "sample", match_all=False)
    assert q.query == " AND mdf.source_name:oqmd OR dc.title:sample"
    # Auto-namespacing test
    q.match_field("composition", "Al")
    assert q.query == " AND mdf.source_name:oqmd OR dc.title:sample AND mdf.composition:Al"
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
    pass



############################
# Forge tests
############################

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


def test_forge_search():
    pass


def test_forge_search_by_elements():
    pass


def test_forge_http_download():
    pass
    #given correct data_link, assert files download
    #assert GlobusHTTPResponse gets processed


def globus_download():
    pass
    #given correct data_link, assert transfers submit
    #assert GlobusHTTPResponse gets processed

def http_stream():
    pass
    #given correct data_link, assert files yield
    #assert GlobusHTTPResponse gets processed

def http_return():
    pass
    #given correct data_link, assert files return in list
    #assert GlobusHTTPResponse gets processed


