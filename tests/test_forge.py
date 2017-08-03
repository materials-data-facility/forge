from mdf_forge import forge


############################
# Forge tests
############################
def test_forge_match_term():
    f1 = forge.Forge()
    # Single match test
    f1.match_term("term1")
    assert f1.query == " AND term1"
    # Multi-match test
    f1.match_term("term2")
    assert f1.query == " AND term1 AND term2"
    # match_all test
    f1.match_term("term3", match_all=False)
    assert f1.query == " AND term1 AND term2 OR term3"


def test_forge_match_field():
    f1 = forge.Forge()
    # Single field test
    f1.match_field("mdf.source_name", "oqmd")
    assert f1.query == " AND mdf.source_name:oqmd"
    # Multi-field test
    f1.match_field
assert return Query has proper self.query (field1:term1 AND/OR mdf.field:term2) and advanced=True

match_sources
assert match_field with mdf.source_name

match_elements
assert match_field with mdf.elements

search
error if no query
assert results returned
assert one correct result
assert no incorrect results
assert correct total number of results (if <10k)

search_by_elements
?

http_download
given correct data_link, assert files download
assert GlobusHTTPResponse gets processed

globus_download
given correct data_link, assert transfers submit
assert GlobusHTTPResponse gets processed

http_stream
given correct data_link, assert files yield
assert GlobusHTTPResponse gets processed

http_return
given correct data_link, assert files return in list
assert GlobusHTTPResponse gets processed

############################
# Query tests
############################
def test_forge_match_term():
    f1 = forge.Forge()
    # Single match test
    f1.match_term("term1")
    assert f1.query == " AND term1"
    # Multi-match test
    f1.match_term("term2")
    assert f1.query == " AND term1 AND term2"
    # match_all test
    f1.match_term("term3", match_all=False)
    assert f1.query == " AND term1 AND term2 OR term3"

same as Forge

match_field
same as Forge

match_sources
same as Forge

match_elements
same as Forge

search
same as Forge

execute
same as search



