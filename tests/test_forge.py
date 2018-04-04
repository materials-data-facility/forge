import os
import types
import pytest
import globus_sdk
from globus_sdk.exc import SearchAPIError
from mdf_forge import forge
import mdf_toolbox


# Manually logging in for Query testing
query_search_client = mdf_toolbox.login(credentials={"app_name": "MDF_Forge",
                                                     "services": ["search"]})["search"]


def test_query_init():
    q1 = forge.Query(query_search_client)
    assert q1.query == "("
    assert q1.limit is None
    assert q1.advanced is False
    assert q1.initialized is False

    q2 = forge.Query(query_search_client, q="mdf.source_name:oqmd", limit=5, advanced=True)
    assert q2.query == "mdf.source_name:oqmd"
    assert q2.limit == 5
    assert q2.advanced is True
    assert q2.initialized is True


def test_query_term():
    q = forge.Query(query_search_client)
    # Single match test
    assert isinstance(q.term("term1"), forge.Query)
    assert q.query == "(term1"
    assert q.initialized is True
    # Multi-match test
    q.and_join().term("term2")
    assert q.query == "(term1 AND term2"
    # Grouping test
    q.or_join(close_group=True).term("term3")
    assert q.query == "(term1 AND term2) OR (term3"


def test_query_field():
    q1 = forge.Query(query_search_client)
    # Single field and return value test
    assert isinstance(q1.field("mdf.source_name", "oqmd"), forge.Query)
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
    q2 = forge.Query(query_search_client)
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
    q = forge.Query(query_search_client)
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
    q = forge.Query(query_search_client)
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
    q = forge.Query(query_search_client)
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
    q = forge.Query(query_search_client)
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
    res4 = q.search("oqmd", index="mdf", limit=3)
    assert len(res4) == 3

    # Check limit correction
    res5 = q.search("nist_xps_db", index="mdf", limit=20000)
    assert len(res5) == 10000

    # Test index translation
    # mdf = d6cc98c3-ff53-4ee2-b22b-c6f945c0d30c
    res6 = q.search(q="data", index="mdf", limit=1, info=True)
    assert len(res6[0]) == 1
    assert res6[1]["index"] == "mdf"
    assert res6[1]["index_uuid"] == "d6cc98c3-ff53-4ee2-b22b-c6f945c0d30c"
    res7 = q.search(q="data", index="d6cc98c3-ff53-4ee2-b22b-c6f945c0d30c", limit=1, info=True)
    assert len(res7[0]) == 1
    assert res7[1]["index"] == "d6cc98c3-ff53-4ee2-b22b-c6f945c0d30c"
    assert res7[1]["index_uuid"] == "d6cc98c3-ff53-4ee2-b22b-c6f945c0d30c"
    with pytest.raises(SearchAPIError):
        q.search(q="data", index="invalid", limit=1, info=True)


def test_query_aggregate(capsys):
    q = forge.Query(query_search_client)
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
    q = forge.Query(query_search_client)
    q.field("source_name", "cip")
    q.and_join()
    q.field("elements", "Al")
    res1 = q.search(limit=10000, index="mdf")
    res2 = (forge.Query(query_search_client)
                 .field("source_name", "cip")
                 .and_join()
                 .field("elements", "Al")
                 .search(limit=10000, index="mdf"))
    assert all([r in res2 for r in res1]) and all([r in res1 for r in res2])


def test_query_cleaning():
    # Imbalanced/improper parentheses
    q1 = forge.Query(query_search_client, q="() term ")
    assert q1.clean_query() == "term"
    q2 = forge.Query(query_search_client, q="(term)(")
    assert q2.clean_query() == "(term)"
    q3 = forge.Query(query_search_client, q="(term) AND (")
    assert q3.clean_query() == "(term)"
    q4 = forge.Query(query_search_client, q="(term AND term2")
    assert q4.clean_query() == "(term AND term2)"
    q5 = forge.Query(query_search_client, q="term AND term2)")
    assert q5.clean_query() == "(term AND term2)"
    q6 = forge.Query(query_search_client, q="((((term AND term2")
    assert q6.clean_query() == "((((term AND term2))))"
    q7 = forge.Query(query_search_client, q="term AND term2))))")
    assert q7.clean_query() == "((((term AND term2))))"

    # Correct trailing operators
    q8 = forge.Query(query_search_client, q="term AND NOT term2 OR")
    assert q8.clean_query() == "term AND NOT term2"
    q9 = forge.Query(query_search_client, q="term OR NOT term2 AND")
    assert q9.clean_query() == "term OR NOT term2"
    q10 = forge.Query(query_search_client, q="term OR term2 NOT")
    assert q10.clean_query() == "term OR term2"


# Test properties
def test_forge_properties():
    f = forge.Forge(index="mdf")
    assert type(f.search_client) is globus_sdk.SearchClient
    assert type(f.transfer_client) is globus_sdk.TransferClient
    assert type(f.mdf_authorizer) is globus_sdk.RefreshTokenAuthorizer


# Sample results for download testing
example_result1 = {
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
    }
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
# NOTE: This example file does not exist
example_result_missing = [{
        'mdf': {
            'links': {
                'landing_page': 'https://data.materialsdatafacility.org/test/missing.txt',
                'txt': {
                    'globus_endpoint': '82f1b5c6-6e9b-11e5-ba47-22000b92c6ec',
                    'http_host': 'https://data.materialsdatafacility.org',
                    'path': '/test/missing.txt'
                }
            }
        }
    }]


# Helper
# Return codes:
#  -1: No match, the value was never found
#   0: Exclusive match, no values other than argument found
#   1: Inclusive match, some values other than argument found
#   2: Partial match, value is found in some but not all results
def check_field(res, field, value):
    dict_path = ""
    for key in field.split("."):
        dict_path += "['{}']".format(key)
    # If no results, set matches to false
    all_match = (len(res) > 0)
    only_match = (len(res) > 0)
    some_match = False
    for r in res:
        vals = eval("r"+dict_path)
        if type(vals) is not list:
            vals = [vals]
        # If a result does not contain the value, no match
        if value not in vals:
            all_match = False
            only_match = False
        # If a result contains other values, inclusive match
        elif len(vals) != 1:
            only_match = False
            some_match = True
        else:
            some_match = True

    if only_match:
        # Exclusive match
        return 0
    elif all_match:
        # Inclusive match
        return 1
    elif some_match:
        # Partial match
        return 2
    else:
        # No match
        return -1


def test_forge_match_field():
    f = forge.Forge(index="mdf")
    # Basic usage
    f.match_field("mdf.source_name", "khazana_vasp")
    res1 = f.search()
    assert check_field(res1, "mdf.source_name", "khazana_vasp") == 0
    # Check that query clears
    assert f.search() == []

    # Also checking check_field
    f.match_field("material.elements", "Al")
    res2 = f.search()
    assert check_field(res2, "material.elements", "Al") == 1


def test_forge_exclude_field():
    f = forge.Forge(index="mdf")
    # Basic usage
    f.exclude_field("material.elements", "Al")
    f.match_field("mdf.source_name", "ab_initio_solute_database")
    res1 = f.search()
    assert check_field(res1, "material.elements", "Al") == -1


def test_forge_match_range():
    # Single-value use
    f = forge.Forge(index="mdf")
    f.match_range("material.elements", "Al", "Al")
    res1, info1 = f.search(info=True)
    assert check_field(res1, "material.elements", "Al") == 1

    res2, info2 = f.search("material.elements:Al", advanced=True, info=True)
    assert info1["total_query_matches"] == info2["total_query_matches"]

    # Non-matching use, test inclusive
    f.match_range("material.elements", "Al", "Al", inclusive=False)
    assert f.search() == []

    # Actual range
    f.match_range("material.elements", "Al", "Cu")
    res4, info4 = f.search(info=True)
    assert info1["total_query_matches"] < info4["total_query_matches"]
    assert (check_field(res4, "material.elements", "Al") >= 0 or
            check_field(res4, "material.elements", "Cu") >= 0)

    # Nothing to match
    assert f.match_range("field", start=None, stop=None) == f


def test_forge_exclude_range():
    # Single-value use
    f = forge.Forge(index="mdf")
    f.exclude_range("material.elements", "Am", "*")
    f.exclude_range("material.elements", "*", "Ak")
    res1, info1 = f.search(info=True)
    assert (check_field(res1, "material.elements", "Al") == 0 or
            check_field(res1, "material.elements", "Al") == 2)

    res2, info2 = f.search("material.elements:Al", advanced=True, info=True)
    assert info1["total_query_matches"] <= info2["total_query_matches"]

    # Non-matching use, test inclusive
    f.exclude_range("material.elements", "Am", "*")
    f.exclude_range("material.elements", "*", "Ak")
    f.exclude_range("material.elements", "Al", "Al", inclusive=False)
    res3, info3 = f.search(info=True)
    assert info1["total_query_matches"] == info3["total_query_matches"]

    # Nothing to match
    assert f.exclude_range("field", start=None, stop=None) == f


def test_forge_exclusive_match():
    f = forge.Forge(index="mdf")
    f.exclusive_match("material.elements", "Al")
    res1 = f.search()
    assert check_field(res1, "material.elements", "Al") == 0

    f.exclusive_match("material.elements", ["Al", "Cu"])
    res2 = f.search()
    assert check_field(res2, "material.elements", "Al") == 1
    assert check_field(res2, "material.elements", "Cu") == 1
    assert check_field(res2, "material.elements", "Cp") == -1
    assert check_field(res2, "material.elements", "Fe") == -1


def test_forge_match_source_names():
    f = forge.Forge(index="mdf")
    # One source
    f.match_source_names("khazana_vasp")
    res1 = f.search()
    assert res1 != []
    assert check_field(res1, "mdf.source_name", "khazana_vasp") == 0

    # Multi-source
    f.match_source_names(["khazana_vasp", "ta_melting"])
    res2 = f.search()
    # res1 is a subset of res2
    assert len(res2) > len(res1)
    assert all([r1 in res2 for r1 in res1])
    assert check_field(res2, "mdf.source_name", "ta_melting") == 2

    # No source
    assert f.match_source_names("") == f


def test_forge_match_ids():
    # Get a couple IDs
    f = forge.Forge(index="mdf")
    res0 = f.search("mdf.source_name:khazana_vasp", advanced=True, limit=2)
    id1 = res0[0]["mdf"]["mdf_id"]
    id2 = res0[1]["mdf"]["mdf_id"]

    # One ID
    f.match_ids(id1)
    res1 = f.search()
    assert res1 != []
    assert check_field(res1, "mdf.mdf_id", id1) == 0

    # Multi-ID
    f.match_ids([id1, id2])
    res2 = f.search()
    # res1 is a subset of res2
    assert len(res2) > len(res1)
    assert all([r1 in res2 for r1 in res1])
    assert check_field(res2, "mdf.mdf_id", id2) == 2

    # No id
    assert f.match_ids("") == f


def test_forge_match_elements():
    f = forge.Forge(index="mdf")
    # One element
    f.match_elements("Al")
    res1 = f.search()
    assert res1 != []
    check_val1 = check_field(res1, "material.elements", "Al")
    assert check_val1 == 0 or check_val1 == 1

    # Multi-element
    f.match_elements(["Al", "Cu"])
    res2 = f.search()
    assert check_field(res2, "material.elements", "Al") == 1
    assert check_field(res2, "material.elements", "Cu") == 1

    # No elements
    assert f.match_elements("") == f


def test_forge_match_titles():
    # One title
    f = forge.Forge(index="mdf")
    titles1 = '"High-throughput Ab-initio Dilute Solute Diffusion Database"'
    res1 = f.match_titles(titles1).search()
    assert res1 != []
    assert check_field(res1, "dc.titles.title",
                       "High-throughput Ab-initio Dilute Solute Diffusion Database") == 0

    # Multiple titles
    titles2 = [
        '"High-throughput Ab-initio Dilute Solute Diffusion Database"',
        '"Khazana (VASP)"'
    ]
    res2 = f.match_titles(titles2).search()
    assert res2 != []
    assert check_field(res2, "dc.titles.title", "Khazana (VASP)") == 2

    # No titles
    assert f.match_titles("") == f


def test_forge_match_years(capsys):
    # One year of data/results
    f = forge.Forge(index="mdf")
    res1 = f.match_years("2015").search()
    assert res1 != []
    assert check_field(res1, "dc.publicationYear", 2015) == 0

    # Multiple years
    res2 = f.match_years(years=["2015", 2011]).search()
    assert check_field(res2, "dc.publicationYear", 2011) == 2

    # Wrong input
    f.match_years(["20x5"]).search()
    out, err = capsys.readouterr()
    assert "Invalid year: '20x5'" in out

    f.match_years(start="20x5").search()
    out, err = capsys.readouterr()
    assert "Invalid start year: '20x5'" in out

    f.match_years(stop="20x5").search()
    out, err = capsys.readouterr()
    assert "Invalid stop year: '20x5'" in out

    assert f.match_years() == f

    # Test range
    res4 = f.match_years(start=2015, stop=2015, inclusive=True).search()
    assert check_field(res4, "dc.publicationYear", 2015) == 0

    res5 = f.match_years(start=2014, stop=2017, inclusive=False).search()
    assert check_field(res5, "dc.publicationYear", 2013) == -1
    assert check_field(res5, "dc.publicationYear", 2014) == -1
    assert check_field(res5, "dc.publicationYear", 2015) == 2
    assert check_field(res5, "dc.publicationYear", 2016) == 2
    assert check_field(res5, "dc.publicationYear", 2017) == -1

    assert f.match_years(start=2015, stop=2015, inclusive=False).search() == []


def test_forge_match_resource_types():
    f = forge.Forge(index="mdf")
    # Test one type
    f.match_resource_types("record")
    res1 = f.search(limit=10)
    assert check_field(res1, "mdf.resource_type", "record") == 0

    # Test two types
    f.match_resource_types(["collection", "dataset"])
    res2 = f.search()
    assert check_field(res2, "mdf.resource_type", "record") == -1
    # TODO: Re-enable this assert after we get collections in MDF
#    assert check_field(res2, "mdf.resource_type", "dataset") == 2

    # Test zero types
    assert f.match_resource_types("") == f


def test_forge_search(capsys):
    # Error on no query
    f = forge.Forge(index="mdf")
    assert f.search() == []
    out, err = capsys.readouterr()
    assert "Error: No query" in out

    # Return info if requested
    res2 = f.search(q="Al", info=False, index="mdf")
    assert isinstance(res2, list)
    assert isinstance(res2[0], dict)

    res3 = f.search(q="Al", info=True)
    assert isinstance(res3, tuple)
    assert isinstance(res3[0], list)
    assert isinstance(res3[0][0], dict)
    assert isinstance(res3[1], dict)

    # Check limit
    res4 = f.search("oqmd", limit=3)
    assert len(res4) == 3

    # Check reset_query
    f.match_field("mdf.source_name", "ta_melting")
    res5 = f.search(reset_query=False)
    res6 = f.search()
    assert all([r in res6 for r in res5]) and all([r in res5 for r in res6])

    # Check default index
    f2 = forge.Forge()
    assert f2.search("data", limit=1, info=True)[1]["index"] == "mdf"


def test_forge_search_by_elements():
    f = forge.Forge(index="mdf")
    elements = ["Cu", "Al"]
    sources = ["oqmd", "nist_xps_db"]
    res1, info1 = f.match_source_names(sources).match_elements(elements).search(limit=10000,
                                                                                 info=True)
    res2, info2 = f.search_by_elements(elements, sources, limit=10000, info=True)
    assert all([r in res2 for r in res1]) and all([r in res1 for r in res2])
    assert check_field(res1, "material.elements", "Al") == 1
    assert check_field(res1, "mdf.source_name", "oqmd") == 2


def test_forge_search_by_titles():
    f = forge.Forge(index="mdf")
    titles1 = ['"High-throughput Ab-initio Dilute Solute Diffusion Database"']
    res1 = f.search_by_titles(titles1)
    assert check_field(res1, "dc.titles.title",
                       "High-throughput Ab-initio Dilute Solute Diffusion Database") == 0

    titles2 = ["Diffusion"]
    res2 = f.search_by_titles(titles2)
    assert check_field(res2, "mdf.title",
                       "High-throughput Ab-initio Dilute Solute Diffusion Database") == 2


def test_forge_aggregate_sources():
    # Test limit
    f = forge.Forge(index="mdf")
    res1 = f.aggregate_sources("nist_xps_db")
    assert isinstance(res1, list)
    assert len(res1) > 10000
    assert isinstance(res1[0], dict)


def test_forge_fetch_datasets_from_results():
    # Get some results
    f = forge.Forge(index="mdf")
    # Record from OQMD
    res01 = f.search("mdf.source_name:oqmd AND mdf.resource_type:record", advanced=True, limit=1)
    # Record from OQMD with info
    res02 = f.search("mdf.source_name:oqmd AND mdf.resource_type:record",
                     advanced=True, limit=1, info=True)
    # Records from JANAF
    res03 = f.search("mdf.source_name:nist_janaf AND mdf.resource_type:record",
                     advanced=True, limit=2)
    # Dataset for NIST XPS DB
    res04 = f.search("mdf.source_name:nist_xps_db AND mdf.resource_type:dataset", advanced=True)

    # Get the correct dataset entries
    oqmd = f.search("mdf.source_name:oqmd AND mdf.resource_type:dataset", advanced=True)[0]
    nist_janaf = f.search("mdf.source_name:nist_janaf AND mdf.resource_type:dataset",
                          advanced=True)[0]

    # Fetch single dataset
    res1 = f.fetch_datasets_from_results(res01[0])
    assert res1[0] == oqmd

    # Fetch dataset with results + info
    res2 = f.fetch_datasets_from_results(res02)
    assert res2[0] == oqmd

    # Fetch multiple datasets
    rtemp = res01+res03
    res3 = f.fetch_datasets_from_results(rtemp)
    assert len(res3) == 2
    assert oqmd in res3
    assert nist_janaf in res3

    # Fetch dataset from dataset
    res4 = f.fetch_datasets_from_results(res04)
    assert res4 == res04

    # Fetch entries from current query
    f.match_source_names("nist_xps_db")
    assert f.fetch_datasets_from_results() == res04

    # Fetch nothing
    unknown_entry = {"mdf": {"resource_type": "unknown"}}
    assert f.fetch_datasets_from_results(unknown_entry) == []


def test_forge_aggregate():
    # Test that aggregate uses the current query properly
    # And returns results
    # And respects the reset_query arg
    f = forge.Forge(index="mdf")
    f.match_field("mdf.source_name", "nist_xps_db")
    res1 = f.aggregate(reset_query=False, index="mdf")
    assert len(res1) > 10000
    assert check_field(res1, "mdf.source_name", "nist_xps_db") == 0
    res2 = f.aggregate()
    assert len(res2) == len(res1)
    assert check_field(res2, "mdf.source_name", "nist_xps_db") == 0


def test_forge_reset_query():
    f = forge.Forge(index="mdf")
    # Term will return results
    f.match_field("material.elements", "Al")
    f.reset_query()
    # Specifying no query will return no results
    assert f.search() == []


def test_forge_current_query():
    f = forge.Forge(index="mdf")
    # Query.clean_query() is already tested, just need to check basic functionality
    f.match_field("field", "value")
    assert f.current_query() == "(field:value)"


def test_forge_http_download(capsys):
    f = forge.Forge(index="mdf")
    # Simple case
    f.http_download(example_result1)
    assert os.path.exists("./test_fetch.txt")

    # Test conflicting filenames
    f.http_download(example_result1)
    assert os.path.exists("./test_fetch(1).txt")
    f.http_download(example_result1)
    assert os.path.exists("./test_fetch(2).txt")
    os.remove("./test_fetch.txt")
    os.remove("./test_fetch(1).txt")
    os.remove("./test_fetch(2).txt")

    # With dest and preserve_dir, and tuple of results
    dest_path = os.path.expanduser("~/mdf")
    f.http_download(([example_result1], {"info": None}), dest=dest_path, preserve_dir=True)
    assert os.path.exists(os.path.join(dest_path, "test", "test_fetch.txt"))
    os.remove(os.path.join(dest_path, "test", "test_fetch.txt"))
    os.rmdir(os.path.join(dest_path, "test"))

    # With multiple files
    f.http_download(example_result2, dest=dest_path)
    assert os.path.exists(os.path.join(dest_path, "test_fetch.txt"))
    assert os.path.exists(os.path.join(dest_path, "test_multifetch.txt"))
    os.remove(os.path.join(dest_path, "test_fetch.txt"))
    os.remove(os.path.join(dest_path, "test_multifetch.txt"))

    # Too many files
    assert f.http_download(list(range(10001)))["success"] is False

    # "Missing" files
    f.http_download(example_result_missing)
    out, err = capsys.readouterr()
    assert not os.path.exists("./missing.txt")
    assert ("Error 404 when attempting to access "
            "'https://data.materialsdatafacility.org/test/missing.txt'") in out


@pytest.mark.xfail(reason="Test relies on get_local_ep() which can require user input.")
def test_forge_globus_download():
    f = forge.Forge(index="mdf")
    # Simple case
    f.globus_download(example_result1)
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


def test_forge_http_stream(capsys):
    f = forge.Forge(index="mdf")
    # Simple case
    res1 = f.http_stream(example_result1)
    assert isinstance(res1, types.GeneratorType)
    assert next(res1) == "This is a test document for Forge testing. Please do not remove.\n"

    # With multiple files
    res2 = f.http_stream((example_result2, {"info": {}}))
    assert isinstance(res2, types.GeneratorType)
    assert next(res2) == "This is a test document for Forge testing. Please do not remove.\n"
    assert next(res2) == "This is a second test document for Forge testing. Please do not remove.\n"

    # Too many results
    res3 = f.http_stream(list(range(10001)))
    assert next(res3)["success"] is False
    out, err = capsys.readouterr()
    assert ("Too many results supplied. Use globus_download() for "
            "fetching more than 2000 entries.") in out
    with pytest.raises(StopIteration):
        next(res3)

    # "Missing" files
    assert next(f.http_stream(example_result_missing)) is None
    out, err = capsys.readouterr()
    assert not os.path.exists("./missing.txt")
    assert ("Error 404 when attempting to access "
            "'https://data.materialsdatafacility.org/test/missing.txt'") in out


def test_forge_chaining():
    f = forge.Forge(index="mdf")
    f.match_field("source_name", "cip")
    f.match_field("material.elements", "Al")
    res1 = f.search()
    res2 = f.match_field("source_name", "cip").match_field("material.elements", "Al").search()
    assert all([r in res2 for r in res1]) and all([r in res1 for r in res2])


def test_forge_show_fields():
    f = forge.Forge(index="mdf")
    res1 = f.show_fields()
    assert "mdf" in res1.keys()
    res2 = f.show_fields(block="mdf", index="mdf")
    assert "mdf.source_name" in res2.keys()


def test_forge_anonymous(capsys):
    f = forge.Forge(anonymous=True)
    # Test search
    assert len(f.search("mdf.source_name:oqmd", advanced=True, limit=300)) == 300

    # Test aggregation
    assert len(f.aggregate("mdf.source_name:nist_xps_db")) > 10000

    # Error on auth-only functions
    # http_download
    assert f.http_download({})["success"] is False
    out, err = capsys.readouterr()
    assert "Error: Anonymous HTTP download not yet supported." in out
    # globus_download
    assert f.globus_download({})["success"] is False
    out, err = capsys.readouterr()
    assert "Error: Anonymous Globus Transfer not supported." in out
    # http_stream
    res = f.http_stream({})
    assert next(res)["success"] is False
    out, err = capsys.readouterr()
    assert "Error: Anonymous HTTP download not yet supported." in out
    with pytest.raises(StopIteration):
        next(res)
