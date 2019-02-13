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
    assert q1.limit is None
    assert q1.advanced is False
    assert q1.initialized is False

    q2 = Query(query_search_client, q="mdf.source_name:oqmd", limit=5, advanced=True)
    assert q2.query == "mdf.source_name:oqmd"
    assert q2.limit == 5
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
    assert q.and_join() == q
    out, err = capsys.readouterr()
    assert ("Error: You must add a term before adding an operator. "
            "The current query has not been changed.") in out
    # Regular join
    q.term("foo").and_join()
    assert q.query == "(foo AND "
    # close_group
    q.term("bar").and_join(close_group=True)
    assert q.query == "(foo AND bar) AND ("


def test_query_or_join(capsys):
    q = Query(query_search_client)
    # Test not initialized
    assert q.or_join() == q
    out, err = capsys.readouterr()
    assert ("Error: You must add a term before adding an operator. "
            "The current query has not been changed.") in out
    # Regular join
    q.term("foo").or_join()
    assert q.query == "(foo OR "
    # close_group
    q.term("bar").or_join(close_group=True)
    assert q.query == "(foo OR bar) OR ("


def test_query_search(capsys):
    # Error on no query
    q = Query(query_search_client)
    assert q.search(index="mdf") == []
    out, err = capsys.readouterr()
    assert "Error: No query" in out
    assert q.search(info=True) == ([], {"error": "No query"})

    # Error on no index
    assert q.search(q="abc") == []
    out, err = capsys.readouterr()
    assert "Error: No index specified" in out
    assert q.search(q="abc", info=True) == ([], {"error": "No index"})

    # Return info if requested
    res2 = q.search(q="Al", index="mdf", info=False)
    assert isinstance(res2, list)
    assert isinstance(res2[0], dict)
    res3 = q.search(q="Al", index="mdf", info=True)
    assert isinstance(res3, tuple)
    assert isinstance(res3[0], list)
    assert isinstance(res3[0][0], dict)
    assert isinstance(res3[1], dict)

    # Check limit
    res4 = q.search("Al", index="mdf", limit=3)
    assert len(res4) == 3

    # Check default limits
    res5 = q.search("Al", index="mdf")
    assert len(res5) == 10
    res6 = q.search("mdf.source_name:nist_xps_db", advanced=True, index="mdf")
    assert len(res6) == 10000

    # Check limit correction
    res7 = q.search("mdf.source_name:nist_xps_db", advanced=True, index="mdf", limit=20000)
    assert len(res7) == 10000

    # Test index translation
    # mdf = 1a57bbe5-5272-477f-9d31-343b8258b7a5
    res8 = q.search(q="data", index="mdf", limit=1, info=True)
    assert len(res8[0]) == 1
    assert res8[1]["index"] == "mdf"
    assert res8[1]["index_uuid"] == "1a57bbe5-5272-477f-9d31-343b8258b7a5"
    res9 = q.search(q="data", index="1a57bbe5-5272-477f-9d31-343b8258b7a5", limit=1, info=True)
    assert len(res9[0]) == 1
    assert res9[1]["index"] == "1a57bbe5-5272-477f-9d31-343b8258b7a5"
    assert res9[1]["index_uuid"] == "1a57bbe5-5272-477f-9d31-343b8258b7a5"
    with pytest.raises(SearchAPIError):
        q.search(q="data", index="invalid", limit=1, info=True)


def test_query_aggregate(capsys):
    q = Query(query_search_client)
    # Error on no query
    assert q.aggregate() == []
    out, err = capsys.readouterr()
    assert "Error: No query" in out

    # Error on no index
    assert q.aggregate(q="abc") == []
    out, err = capsys.readouterr()
    assert "Error: No index specified" in out

    # Basic aggregation
    res1 = q.aggregate("mdf.source_name:nist_xps_db", index="mdf")
    assert len(res1) > 10000
    assert isinstance(res1[0], dict)

    # Multi-dataset aggregation
    res2 = q.aggregate("(mdf.source_name:nist_xps_db OR mdf.source_name:khazana_vasp)",
                       index="mdf")
    assert len(res2) > 10000
    assert len(res2) > len(res1)

    # Unnecessary aggregation fallback to .search()
    # Check success in Coveralls
    assert len(q.aggregate("mdf.source_name:khazana_vasp")) < 10000


def test_query_chaining():
    q = Query(query_search_client)
    q.field("source_name", "cip")
    q.and_join()
    q.field("elements", "Al")
    res1 = q.search(limit=10000, index="mdf")
    res2 = (Query(query_search_client)
            .field("source_name", "cip")
            .and_join()
            .field("elements", "Al")
            .search(limit=10000, index="mdf"))
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
