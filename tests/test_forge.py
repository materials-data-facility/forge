import os
import time
import types
import pytest
import globus_sdk
from mdf_forge import forge
from mdf_toolbox import toolbox


# Manually logging in for Query testing
query_search_client = toolbox.login(credentials={"app_name": "MDF_Forge", 
                                        "services": ["search"], "index": "mdf"})["search"]


############################
# Query tests
############################

def test_query_init():
    q1 = forge.Query(query_search_client)
    assert q1.query == "("
    assert q1.limit == None
    assert q1.advanced == False
    assert q1.initialized == False

    q2 = forge.Query(query_search_client, q="mdf.source_name:oqmd", limit=5, advanced=True)
    assert q2.query == "mdf.source_name:oqmd"
    assert q2.limit == 5
    assert q2.advanced == True
    assert q2.initialized == True


def test_query_term():
    q = forge.Query(query_search_client)
    # Single match test
    assert isinstance(q.term("term1"), forge.Query)
    assert q.query == "(term1"
    assert q.initialized == True
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
    q1 = forge.Query(query_search_client)
    assert q1.query == "("
    # Add bad operator
    assert q1.operator("FOO") == q1
    out, err = capsys.readouterr()
    assert "Error: 'FOO' is not a valid operator"
    assert q1.query == "("
    # Test operator cleaning
    q1.operator("   and ")
    assert q1.query == "( AND "
    # Test close_group
    q1.operator("OR", close_group=True)
    assert q1.query == "( AND ) OR ("


def test_query_and_join(capsys):
    q1 = forge.Query(query_search_client)
    # Test not initialized
    assert q1.and_join() == q1
    out, err = capsys.readouterr()
    assert ("Error: You must add a term before adding an operator. "
            "The current query has not been changed.") in out
    # Regular join
    q1.term("foo").and_join()
    assert q1.query == "(foo AND "
    # close_group
    q1.term("bar").and_join(close_group=True)
    assert q1.query == "(foo AND bar) AND ("


def test_query_or_join(capsys):
    q1 = forge.Query(query_search_client)
    # Test not initialized
    assert q1.or_join() == q1
    out, err = capsys.readouterr()
    assert ("Error: You must add a term before adding an operator. "
            "The current query has not been changed.") in out
    # Regular join
    q1.term("foo").or_join()
    assert q1.query == "(foo OR "
    # close_group
    q1.term("bar").or_join(close_group=True)
    assert q1.query == "(foo OR bar) OR ("


def test_query_search(capsys):
    # Error on no query
    q1 = forge.Query(query_search_client)
    assert q1.search() == []
    out, err = capsys.readouterr()
    assert "Error: No query" in out
    assert q1.search(info=True) == ([], {"error": "No query"})

    # Return info if requested
    q2 = forge.Query(query_search_client)
    res2 = q2.search(q="Al", info=False)
    assert isinstance(res2, list)
    assert isinstance(res2[0], dict)
    q3 = forge.Query(query_search_client)
    res3 = q3.search(q="Al", info=True)
    assert isinstance(res3, tuple)
    assert isinstance(res3[0], list)
    assert isinstance(res3[0][0], dict)
    assert isinstance(res3[1], dict)

    # Check limit
    q4 = forge.Query(query_search_client)
    res4 = q4.search("oqmd", limit=3)
    assert len(res4) == 3

    # Check limit correction
    q5 = forge.Query(query_search_client)
    res5 = q5.search("nist_xps_db", limit=20000)
    assert len(res5) == 10000


def test_query_aggregate(capsys):
    q1 = forge.Query(query_search_client)
    # Error on no query
    assert q1.aggregate() == []
    out, err = capsys.readouterr()
    assert "Error: No query" in out

    # Basic aggregation
    res1 = q1.aggregate("mdf.source_name:nist_xps_db")
    assert len(res1) > 10000
    assert isinstance(res1[0], dict)

    # Multi-dataset aggregation
    q2 = forge.Query(query_search_client)
    res2 = q2.aggregate("(mdf.source_name:nist_xps_db OR mdf.source_name:nist_janaf)")
    assert len(res2) > 10000
    assert len(res2) > len(res1)


def test_query_chaining():
    q1 = forge.Query(query_search_client)
    q1.field("source_name", "cip")
    q1.and_join()
    q1.field("elements", "Al")
    res1 = q1.search(limit=10000)
    res2 = (forge.Query(query_search_client)
                 .field("source_name", "cip")
                 .and_join()
                 .field("elements", "Al")
                 .search(limit=10000))
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



############################
# Forge tests
############################

# Test properties
def test_forge_properties():
    f1 = forge.Forge()
    assert type(f1.search_client) is toolbox.SearchClient
    assert type(f1.transfer_client) is globus_sdk.TransferClient
    assert type(f1.mdf_authorizer) is globus_sdk.RefreshTokenAuthorizer


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
    supported_fields = [
        "mdf.elements",
        "mdf.source_name",
        "mdf.mdf_id",
        "mdf.resource_type",
        "mdf.title",
        "mdf.tags"
    ]
    if field not in supported_fields:
        raise ValueError("Implement or re-spell "
                         + field
                         + "because check_field only works on " 
                         + str(supported_fields))
    # If no results, set matches to false
    all_match = (len(res) > 0)
    only_match = (len(res) > 0)
    some_match = False
    for r in res:
        if field == "mdf.elements":
            try:
                vals = r["mdf"]["elements"]
            except KeyError:
                vals = []
        elif field == "mdf.source_name":
            vals = [r["mdf"]["source_name"]]
        elif field == "mdf.mdf_id":
            vals = [r["mdf"]["mdf_id"]]
        elif field == "mdf.resource_type":
            vals = [r["mdf"]["resource_type"]]
        elif field == "mdf.title":
            vals = [r["mdf"]["title"]]
        elif field == "mdf.tags":
            # mdf.tags field is already a list
            try:
                vals = r["mdf"]["tags"]
            except KeyError:
                vals = []

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
        #print("Exclusive match")
        return 0
    elif all_match:
        #print("Inclusive match")
        return 1
    elif some_match:
        #print("Partial match")
        return 2
    else:
        #print("No match")
        return -1


def test_forge_match_field():
    f1 = forge.Forge()
    # Basic usage
    f1.match_field("mdf.source_name", "nist_janaf")
    res1 = f1.search()
    assert check_field(res1, "mdf.source_name", "nist_janaf") == 0
    # Check that query clears
    assert f1.search() == []
    # Also checking check_field
    f2 = forge.Forge()
    f2.match_field("mdf.elements", "Al")
    res2 = f2.search()
    assert check_field(res2, "mdf.elements", "Al") == 1


def test_forge_exclude_field():
    f1 = forge.Forge()
    # Basic usage
    f1.exclude_field("mdf.elements", "Al")
    f1.match_field("mdf.source_name", "core_mof")
    res1 = f1.search()
    assert check_field(res1, "mdf.elements", "Al") == -1


def test_forge_match_range():
    # Single-value use
    f1 = forge.Forge()
    f1.match_range("mdf.elements", "Al", "Al")
    res1, info1 = f1.search(info=True)
    assert check_field(res1, "mdf.elements", "Al") == 1
    f2 = forge.Forge()
    res2, info2 = f2.search("mdf.elements:Al", advanced=True, info=True)
    assert info1["total_query_matches"] == info2["total_query_matches"]
    # Non-matching use, test inclusive
    f3 = forge.Forge()
    f3.match_range("mdf.elements", "Al", "Al", inclusive=False)
    assert f3.search() == []
    # Actual range
    f4 = forge.Forge()
    f4.match_range("mdf.elements", "Al", "Cu")
    res4, info4 = f4.search(info=True)
    assert info1["total_query_matches"] < info4["total_query_matches"]
    assert (check_field(res4, "mdf.elements", "Al") >= 0 or
            check_field(res4, "mdf.elements", "Cu") >= 0)


def test_forge_exclude_range():
    # Single-value use
    f1 = forge.Forge()
    f1.exclude_range("mdf.elements", "Am", "*")
    f1.exclude_range("mdf.elements", "*", "Ak")
    res1, info1 = f1.search(info=True)
    assert (check_field(res1, "mdf.elements", "Al") == 0 or
            check_field(res1, "mdf.elements", "Al") == 2)
    f2 = forge.Forge()
    res2, info2 = f2.search("mdf.elements:Al", advanced=True, info=True)
    assert info1["total_query_matches"] <= info2["total_query_matches"]
    # Non-matching use, test inclusive
    f3 = forge.Forge()
    f3.exclude_range("mdf.elements", "Am", "*")
    f3.exclude_range("mdf.elements", "*", "Ak")
    f3.exclude_range("mdf.elements", "Al", "Al", inclusive=False)
    res3, info3 = f3.search(info=True)
    assert info1["total_query_matches"] == info3["total_query_matches"]


def test_forge_exclusive_match():
    f1 = forge.Forge()
    f1.exclusive_match("mdf.elements", "Al")
    res1 = f1.search()
    assert check_field(res1, "mdf.elements", "Al") == 0
    f2 = forge.Forge()
    f2.exclusive_match("mdf.elements", ["Al", "Cu"])
    res2 = f2.search()
    assert check_field(res2, "mdf.elements", "Al") == 1
    assert check_field(res2, "mdf.elements", "Cu") == 1
    assert check_field(res2, "mdf.elements", "Cp") == -1
    assert check_field(res2, "mdf.elements", "Fe") == -1


def test_forge_match_sources():
    f1 = forge.Forge()
    # One source
    f1.match_sources("nist_janaf")
    res1 = f1.search()
    assert res1 != []
    assert check_field(res1, "mdf.source_name", "nist_janaf") == 0
    # Multi-source
    f2 = forge.Forge()
    f2.match_sources(["nist_janaf", "hopv"])
    res2 = f2.search()
    # res1 is a subset of res2
    assert len(res2) > len(res1)
    assert all([r1 in res2 for r1 in res1])
    assert check_field(res2, "mdf.source_name", "nist_janaf") == 2
    # No source
    f3 = forge.Forge()
    assert f3.match_sources("") == f3


def test_forge_match_ids():
    # Get a couple IDs
    f0 = forge.Forge()
    res0 = f0.search("mdf.source_name:nist_janaf", advanced=True, limit=2)
    id1 = res0[0]["mdf"]["mdf_id"]
    id2 = res0[1]["mdf"]["mdf_id"]
    f1 = forge.Forge()
    # One ID
    f1.match_ids(id1)
    res1 = f1.search()
    assert res1 != []
    assert check_field(res1, "mdf.mdf_id", id1) == 0
    # Multi-ID
    f2 = forge.Forge()
    f2.match_ids([id1, id2])
    res2 = f2.search()
    # res1 is a subset of res2
    assert len(res2) > len(res1)
    assert all([r1 in res2 for r1 in res1])
    assert check_field(res2, "mdf.mdf_id", id2) == 2
    # No id
    f3 = forge.Forge()
    assert f3.match_ids("") == f3


def test_forge_match_elements():
    f1 = forge.Forge()
    # One element
    f1.match_elements("Al")
    res1 = f1.search()
    assert res1 != []
    check_val1 = check_field(res1, "mdf.elements", "Al")
    assert check_val1 == 0 or check_val1 == 1
    # Multi-element
    f2 = forge.Forge()
    f2.match_elements(["Al", "Cu"])
    res2 = f2.search()
    assert check_field(res2, "mdf.elements", "Al") == 1
    assert check_field(res2, "mdf.elements", "Cu") == 1
    # No elements
    f3 = forge.Forge()
    assert f3.match_elements("") == f3


def test_forge_match_titles():
    # One title
    f1 = forge.Forge()
    titles1 = '"OQMD - Na1Y2Zr1"'
    res1 = f1.match_titles(titles1).search()
    assert res1 != []
    assert check_field(res1, "mdf.title", "OQMD - Na1Y2Zr1") == 0

    # Multiple titles
    f2 = forge.Forge()
    titles2 = ['"AMCS - Tungsten"', '"Cytochrome QSAR"']
    res2 = f2.match_titles(titles2).search()
    assert res2 != []
    assert check_field(res2, "mdf.title", "Cytochrome QSAR - C13F2N6O") == 2

    # No titles
    f3 = forge.Forge()
    assert f3.match_titles("") == f3


def test_forge_match_tags():
    # Get one tag
    f0 = forge.Forge()
    res0 = f0.search("mdf.source_name:trinkle_elastic_fe_bcc", advanced=True, limit=1)
    tags1 = res0[0]["mdf"]["tags"][0]
    # One tag
    f1 = forge.Forge()
    res1 = f1.match_tags(tags1).search()
    assert check_field(res1, "mdf.tags", tags1) == 2

    f2 = forge.Forge()
    tags2 = "\"ab initio\""
    f2.match_tags(tags2)
    res2 = f2.search()
    # Elasticsearch splits ["ab-initio"] into ["ab", "initio"]
    assert check_field(res2, "mdf.tags", "ab-initio") == 2

    # Multiple tags
    f3 = forge.Forge()
    tags3 = ["\"density functional theory calculations\"", "\"X-ray\""]
    res3 = f3.match_tags(tags3, match_all=True).search()
    # "source_name": "ge_nanoparticles",
    # "tags": [ "amorphization","density functional theory calculations","Ge nanoparticles",
    #           "high pressure","phase transformation","Raman","X-ray absorption","zip" ]
    assert check_field(res3, "mdf.tags", "Raman") == 1
    assert check_field(res3, "mdf.tags", "X-ray absorption") == 1
    assert check_field(res3, "mdf.tags", "density functional theory calculations") == 1

    # No tag
    f4 = forge.Forge()
    assert f4.match_tags("") == f4


def test_forge_match_resource_types():
    f1 = forge.Forge()
    # Test one type
    f1.match_resource_types("record")
    res1 = f1.search(limit=10)
    assert check_field(res1, "mdf.resource_type", "record") == 0
    # Test two types
    f2 = forge.Forge()
    f2.match_resource_types(["collection", "dataset"])
    res2 = f2.search()
    assert check_field(res2, "mdf.resource_type", "record") == -1
    #TODO: Re-enable this assert after we get collections in MDF
#    assert check_field(res2, "mdf.resource_type", "dataset") == 2
    # Test zero types
    f3 = forge.Forge()
    assert f3.match_resource_types("") == f3


def test_forge_search(capsys):
    # Error on no query
    f1 = forge.Forge()
    assert f1.search() == []
    out, err = capsys.readouterr()
    assert "Error: No query" in out

    # Return info if requested
    f2 = forge.Forge()
    res2 = f2.search(q="Al", info=False)
    assert isinstance(res2, list)
    assert isinstance(res2[0], dict)
    f3 = forge.Forge()
    res3 = f3.search(q="Al", info=True)
    assert isinstance(res3, tuple)
    assert isinstance(res3[0], list)
    assert isinstance(res3[0][0], dict)
    assert isinstance(res3[1], dict)

    # Check limit
    f4 = forge.Forge()
    res4 = f4.search("oqmd", limit=3)
    assert len(res4) == 3

    # Check reset_query
    f5 = forge.Forge()
    f5.match_field("mdf.source_name", "hopv")
    res5 = f5.search(reset_query=False)
    res6 = f5.search()
    assert all([r in res6 for r in res5]) and all([r in res5 for r in res6])


def test_forge_search_by_elements():
    f1 = forge.Forge()
    f2 = forge.Forge()
    elements = ["Cu", "Al"]
    sources = ["oqmd", "nist_xps_db"]
    res1, info1 = f1.match_sources(sources).match_elements(elements).search(limit=10000, info=True)
    res2, info2 = f2.search_by_elements(elements, sources, limit=10000, info=True)
    assert all([r in res2 for r in res1]) and all([r in res1 for r in res2])
    assert check_field(res1, "mdf.elements", "Al") == 1
    assert check_field(res1, "mdf.source_name", "oqmd") == 2


def test_forge_search_by_titles():
    f1 = forge.Forge()
    titles1 = ["\"AMCS - Tungsten\""]
    res1 = f1.search_by_titles(titles1)
    assert check_field(res1, "mdf.title", "AMCS - Tungsten") == 0

    f2 = forge.Forge()
    titles2 = ["Tungsten"]
    res2 = f2.search_by_titles(titles2)
    assert check_field(res2, "mdf.title", "AMCS - Tungsten") == 2


def test_forge_search_by_tags():
    f1 = forge.Forge()
    tags1 = "DFT"
    res1 = f1.search_by_tags(tags1)
    assert check_field(res1, "mdf.tags", "DFT") == 2

    f2 = forge.Forge()
    tags2 = ["\"Density Functional Theory\"", "\"X-ray\""]
    res2 = f2.search_by_tags(tags2, match_all=True)
    f3 = forge.Forge()
    tags3 = ["\"Density Functional Theory\"", "\"X-ray\""]
    res3 = f3.search_by_tags(tags3, match_all=False)

    # res2 is a subset of res3
    assert len(res3) > len(res2)
    assert all([r in res3 for r in res2])


def test_forge_aggregate_source():
    # Test limit
    f1 = forge.Forge()
    res1 = f1.aggregate_source("amcs")
    assert isinstance(res1, list)
    assert len(res1) > 10000
    assert isinstance(res1[0], dict)


def test_forge_fetch_datasets_from_results():
    # Get some results
    # Record from OQMD
    res01 = forge.Forge().search("mdf.source_name:oqmd AND mdf.resource_type:record",
                                 advanced=True, limit=1)
    # Record from OQMD with info
    res02 = forge.Forge().search("mdf.source_name:oqmd AND mdf.resource_type:record",
                                 advanced=True, limit=1, info=True)
    # Records from JANAF
    res03 = forge.Forge().search("mdf.source_name:nist_janaf AND mdf.resource_type:record",
                                 advanced=True, limit=2)
    # Dataset for NIST XPS DB
    res04 = forge.Forge().search("mdf.source_name:nist_xps_db AND mdf.resource_type:dataset",
                                 advanced=True)

    # Get the correct dataset entries
    oqmd = forge.Forge().search("mdf.source_name:oqmd AND mdf.resource_type:dataset",
                                advanced=True)[0]
    nist_janaf = forge.Forge().search("mdf.source_name:nist_janaf AND mdf.resource_type:dataset",
                                      advanced=True)[0]

    # Fetch single dataset
    f1 = forge.Forge()
    res1 = f1.fetch_datasets_from_results(res01[0])
    assert res1[0] == oqmd
    # Fetch dataset with results + info
    f2 = forge.Forge()
    res2 = f2.fetch_datasets_from_results(res02)
    assert res2[0] == oqmd
    # Fetch multiple datasets
    f3 = forge.Forge()
    rtemp = res01+res03
    res3 = f3.fetch_datasets_from_results(rtemp)
    assert len(res3) == 2
    assert oqmd in res3
    assert nist_janaf in res3
    # Fetch dataset from dataset
    f4 = forge.Forge()
    res4 = f4.fetch_datasets_from_results(res04)
    assert res4 == res04
    # Fetch entries from current query
    f5 = forge.Forge()
    f5.match_sources("nist_xps_db")
    assert f5.fetch_datasets_from_results() == res04
    # Fetch nothing
    f6 = forge.Forge()
    unknown_entry = {"mdf": {"resource_type": "unknown"}}
    assert f6.fetch_datasets_from_results(unknown_entry) == []


def test_forge_aggregate():
    # Test that aggregate uses the current query properly
    # And returns results
    # And respects the reset_query arg
    f1 = forge.Forge()
    f1.match_field("mdf.source_name", "nist_xps_db")
    res1 = f1.aggregate(reset_query=False)
    assert len(res1) > 10000
    res2 = f1.aggregate()
    assert all([r in res2 for r in res1]) and all([r in res1 for r in res2])


def test_forge_reset_query():
    f = forge.Forge()
    # Term will return results
    f.match_field("elements", "Al")
    f.reset_query()
    # Specifying no query will return no results
    assert f.search() == []


def test_forge_current_query():
    f = forge.Forge()
    # Query.clean_query() is already tested, just need to check basic functionality
    f.match_field("field", "value")
    assert f.current_query() == "(field:value)"


def test_forge_http_download(capsys):
    f = forge.Forge()
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
    assert f.http_download(list(range(10001)))["success"] == False

    # "Missing" files
    f.http_download(example_result_missing)
    out, err = capsys.readouterr()
    assert not os.path.exists("./missing.txt")
    assert ("Error 404 when attempting to access "
            "'https://data.materialsdatafacility.org/test/missing.txt'") in out


@pytest.mark.xfail(reason="Test relies on get_local_ep() which can require user input.")
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


def test_forge_http_stream(capsys):
    f1 = forge.Forge()
    # Simple case
    res1 = f1.http_stream(example_result1)
    assert isinstance(res1, types.GeneratorType)
    assert next(res1) == "This is a test document for Forge testing. Please do not remove.\n"
    # With multiple files
    res2 = f1.http_stream((example_result2, {"info":{}}))
    assert isinstance(res2, types.GeneratorType)
    assert next(res2) == "This is a test document for Forge testing. Please do not remove.\n"
    assert next(res2) == "This is a second test document for Forge testing. Please do not remove.\n"
    # Too many results
    res3 = f1.http_stream(list(range(10001)))
    assert next(res3)["success"] == False
    out, err = capsys.readouterr()
    assert ("Too many results supplied. Use globus_download() for "
            "fetching more than 2000 entries.") in out
    with pytest.raises(StopIteration):
        next(res3)

    # "Missing" files
    f2 = forge.Forge()
    assert next(f2.http_stream(example_result_missing)) is None
    out, err = capsys.readouterr()
    assert not os.path.exists("./missing.txt")
    assert ("Error 404 when attempting to access "
            "'https://data.materialsdatafacility.org/test/missing.txt'") in out


def test_forge_http_return():
    f = forge.Forge()
    # Simple case
    res1 = f.http_return(example_result1)
    assert isinstance(res1, list)
    assert res1 == ["This is a test document for Forge testing. Please do not remove.\n"]
    # With multiple files
    res2 = f.http_return(example_result2)
    assert isinstance(res2, list)
    assert res2 == ["This is a test document for Forge testing. Please do not remove.\n",
                    "This is a second test document for Forge testing. Please do not remove.\n"]


def test_forge_chaining():
    f1 = forge.Forge()
    f1.match_field("source_name", "cip")
    f1.match_field("elements", "Al")
    res1 = f1.search()
    res2 = forge.Forge().match_field("source_name", "cip").match_field("elements", "Al").search()
    assert all([r in res2 for r in res1]) and all([r in res1 for r in res2])


def test_forge_show_fields():
    f1 = forge.Forge()
    res1 = f1.show_fields()
    assert "mdf" in res1.keys()
    res2 = f1.show_fields("mdf")
    assert "mdf.mdf_id" in res2.keys()

