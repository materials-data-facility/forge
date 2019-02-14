import mdf_toolbox
import pytest
from globus_sdk import SearchAPIError
from mdf_forge.query import Query

# Manually logging in for Query testing
query_search_client = mdf_toolbox.login(credentials={"app_name": "MDF_Forge",
                                                     "services": ["search"]})["search"]


def test_query_init():
    q1 = Query(query_search_client)
    assert q1.query == "("
    assert q1.advanced is False
    assert q1.initialized is False

    q2 = Query(query_search_client, q="mdf.source_name:oqmd", advanced=True)
    assert q2.query == "mdf.source_name:oqmd"
    assert q2.advanced is True
    assert q2.initialized is True


def test_query_term():
    q = Query(query_search_client)
    # Single match test
    assert isinstance(q.term("term1"), Query)
    assert q.query == "(term1"
    assert q.initialized is True
    # Multi-match test
    q.and_join().term("term2")
    assert q.query == "(term1 AND term2"
    # Grouping test
    q.or_join(close_group=True).term("term3")
    assert q.query == "(term1 AND term2) OR (term3"


def test_query_field():
    q1 = Query(query_search_client)
    # Single field and return value test
    assert isinstance(q1.field("mdf.source_name", "oqmd"), Query)
    assert q1.query == "(mdf.source_name:oqmd"
    # Multi-field and grouping test
    q1.and_join(close_group=True).field("dc.title", "sample")
    assert q1.query == "(mdf.source_name:oqmd) AND (dc.title:sample"
    # Negation test
    q1.negate()
    assert q1.query == "(mdf.source_name:oqmd) AND (dc.title:sample NOT "
    # Explicit operator test
    # Makes invalid query for this case
    q1.operator("NOT")
    assert q1.query == "(mdf.source_name:oqmd) AND (dc.title:sample NOT  NOT "
    # Ensure advanced is set
    assert q1.advanced

    # Test noop on blanks
    q2 = Query(query_search_client)
    assert q2.query == "("
    q2.field(field="", value="value")
    assert q2.query == "("
    q2.field(field="field", value="")
    assert q2.query == "("
    q2.field(field="", value="")
    assert q2.query == "("
    q2.field(field="field", value="value")
    assert q2.query == "(field:value"


def test_query_operator(capsys):
    q = Query(query_search_client)
    assert q.query == "("
    # Add bad operator
    assert q.operator("FOO") == q
    out, err = capsys.readouterr()
    assert "Error: 'FOO' is not a valid operator" in out
    assert q.query == "("
    # Test operator cleaning
    q.operator("   and ")
    assert q.query == "( AND "
    # Test close_group
    q.operator("OR", close_group=True)
    assert q.query == "( AND ) OR ("


def test_query_and_join(capsys):
    q = Query(query_search_client)
    # Test not initialized
    with pytest.raises(ValueError) as excinfo:
        q.and_join()
    assert 'before adding an operator' in str(excinfo.value)

    # Regular join
    q.term("foo").and_join()
    assert q.query == "(foo AND "
    # close_group
    q.term("bar").and_join(close_group=True)
    assert q.query == "(foo AND bar) AND ("


def test_query_or_join(capsys):
    q = Query(query_search_client)
    # Test not initialized
    with pytest.raises(ValueError) as excinfo:
        q.or_join()
    assert 'before adding an operator' in str(excinfo.value)

    # Regular join
    q.term("foo").or_join()
    assert q.query == "(foo OR "

    # close_group
    q.term("bar").or_join(close_group=True)
    assert q.query == "(foo OR bar) OR ("


def test_query_search(capsys):
    # Error on no query
    q = Query(query_search_client)
    with pytest.raises(ValueError) as excinfo:
        q.search("mdf")
    assert "Query not set" in str(excinfo.value)

    # Return info if requested
    res2 = Query(query_search_client, q="Al").search(index="mdf", info=False)
    assert isinstance(res2, list)
    assert isinstance(res2[0], dict)
    res3 = Query(query_search_client, q="Al").search(index="mdf", info=True)
    assert isinstance(res3, tuple)
    assert isinstance(res3[0], list)
    assert isinstance(res3[0][0], dict)
    assert isinstance(res3[1], dict)

    # Check limit
    res4 = Query(query_search_client, q="Al").search(index="mdf", info=False, limit=3)
    assert len(res4) == 3

    # Check default limits
    res5 = Query(query_search_client, q="Al").search(index="mdf")
    assert len(res5) == 10
    res6 = Query(query_search_client, q="mdf.source_name:nist_xps_db",
                 advanced=True).search(index="mdf")
    assert len(res6) == 10000

    # Check limit correction (should throw a warning)
    with pytest.warns(RuntimeWarning):
        res7 = Query(query_search_client, advanced=True,
                     q="mdf.source_name:nist_xps_db").search("mdf", limit=20000)
    assert len(res7) == 10000

    # Test index translation
    # mdf = 1a57bbe5-5272-477f-9d31-343b8258b7a5
    res8 = Query(query_search_client, q="data").search(index="mdf", info=True, limit=1)
    assert len(res8[0]) == 1
    assert res8[1]["index"] == "mdf"
    assert res8[1]["index_uuid"] == "1a57bbe5-5272-477f-9d31-343b8258b7a5"
    with pytest.raises(SearchAPIError):
        Query(query_search_client, q="data").search(index="invalid", info=True, limit=1)


def test_query_aggregate(capsys):
    q = Query(query_search_client, advanced=True)
    # Error on no query
    with pytest.raises(ValueError) as excinfo:
        q.aggregate("mdf")
    assert "Query not set" in str(excinfo.value)

    # Basic aggregation
    q.query = "mdf.source_name:nist_xps_db"
    res1 = q.aggregate("mdf")
    assert len(res1) > 10000
    assert isinstance(res1[0], dict)

    # Multi-dataset aggregation
    q.query = "(mdf.source_name:nist_xps_db OR mdf.source_name:khazana_vasp)"
    res2 = q.aggregate(index="mdf")
    assert len(res2) > 10000
    assert len(res2) > len(res1)

    # Unnecessary aggregation fallback to .search()
    # Check success in Coveralls
    q.query = "mdf.source_name:khazana_vasp"
    assert len(q.aggregate("mdf")) < 10000


def test_query_chaining():
    q = Query(query_search_client)
    q.field("source_name", "cip")
    q.and_join()
    q.field("elements", "Al")
    res1 = q.search(index="mdf", limit=10000)
    res2 = (Query(query_search_client)
            .field("source_name", "cip")
            .and_join()
            .field("elements", "Al")
            .search(index="mdf", limit=10000))
    assert all([r in res2 for r in res1]) and all([r in res1 for r in res2])


def test_query_cleaning():
    # Imbalanced/improper parentheses
    q1 = Query(query_search_client, q="() term ")
    assert q1.clean_query() == "term"
    q2 = Query(query_search_client, q="(term)(")
    assert q2.clean_query() == "(term)"
    q3 = Query(query_search_client, q="(term) AND (")
    assert q3.clean_query() == "(term)"
    q4 = Query(query_search_client, q="(term AND term2")
    assert q4.clean_query() == "(term AND term2)"
    q5 = Query(query_search_client, q="term AND term2)")
    assert q5.clean_query() == "(term AND term2)"
    q6 = Query(query_search_client, q="((((term AND term2")
    assert q6.clean_query() == "((((term AND term2))))"
    q7 = Query(query_search_client, q="term AND term2))))")
    assert q7.clean_query() == "((((term AND term2))))"

    # Correct trailing operators
    q8 = Query(query_search_client, q="term AND NOT term2 OR")
    assert q8.clean_query() == "term AND NOT term2"
    q9 = Query(query_search_client, q="term OR NOT term2 AND")
    assert q9.clean_query() == "term OR NOT term2"
    q10 = Query(query_search_client, q="term OR term2 NOT")
    assert q10.clean_query() == "term OR term2"


def test_sort():
    # Sort ascending by atomic number
    q = Query(query_search_client, q="mdf.source_name:=oqmd", advanced=True)
    q.add_sort('crystal_structure.number_of_atoms', True)
    res = q.search('mdf', limit=1)
    assert res[0]['crystal_structure']['number_of_atoms'] == 1

    # Sort descending by composition
    q.add_sort('material.composition', False)
    res = q.search('mdf', limit=1)
    assert res[0]['crystal_structure']['number_of_atoms'] == 1
    assert res[0]['material']['composition'].startswith('Zr')
