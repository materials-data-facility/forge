from json import  dump
from bson import ObjectId
import globus_auth

def gen_test_data():
	gmeta = {
		"@datatype":"GIngest",
		"@version":"2016-11-09",
		"ingest_type":"GMetaEntry",
		"source_id":"testdb",
		"ingest_data":{
			"@datatype":"GMetaEntry",
			"@version":"2016-11-09",
			"subject":"REPLACEME",
			"visible_to":"REPLACEMETOO",
			"id":"usersuppliedid",
			"source_id":"testsource",
			"content":{
				}
			}

		}

	'''
	x = {
		"node_id" : BSON.
		"parent_id" : [BSON],
		"node_type" : string,
		"acl" : ["117e8833-68f5-4cb2-afb3-05b25db69be1", "c8745ef4-d274-11e5-bee8-3b6845397ac9"],
		"tree" : int(1/2/3/),
		"string_all" : "returneverything",
		"a" : int,
		"b" : string,
		"c": date string
		}
	'''
	c1 = {
		"node_id" : str(ObjectId()),
		"parent_id" : [],
		"node_type" : "collection",
		"acl" : ["public"],
		"tree" : 1,
		"string_all" : "returneverything",
		"a" : 5,
		"b" : "five",
		"c": "05/05/2005"
		}

	c2 = {
		"node_id" : str(ObjectId()),
		"parent_id" : [],
		"node_type" : "collection",
		"acl" : ["public"],
		"tree" : 3,
		"string_all" : "returneverything",
		"a" : 10,
		"b" : "ten",
		"c": "10/10/2010"
		}
	
	c3 = {
		"node_id" : str(ObjectId()),
		"parent_id" : [c1["node_id"]],
		"node_type" : "collection",
		"acl" : ["public"],
		"tree" : 1,
		"string_all" : "returneverything",
		"a" : 1000,
		"b" : "ten hundred",
		"c": "01/05/2015"
		}

	d1 = {
		"node_id" : str(ObjectId()),
		"parent_id" : [c1["node_id"]],
		"node_type" : "dataset",
		"acl" : ["public"],
		"tree" : 2,
		"string_all" : "returneverything",
		"a" : 500,
		"b" : "half a thousand",
		"c": "02/11/2011"
		}

	d2 = {
		"node_id" : str(ObjectId()),
		"parent_id" : [c1["node_id"], c2["node_id"]],
		"node_type" : "dataset",
		"acl" : ["public"],
		"tree" : 3,
		"string_all" : "returneverything",
		"a" : 42,
		"b" : "don't panic",
		"c": "11/11/2011"
		}

	d3 = {
		"node_id" : str(ObjectId()),
		"parent_id" : [c3["node_id"]],
		"node_type" : "dataset",
		"acl" : ["117e8833-68f5-4cb2-afb3-05b25db69be1", "c8745ef4-d274-11e5-bee8-3b6845397ac9"],
		"tree" : 1,
		"string_all" : "returneverything",
		"a" : 9999,
		"b" : "nine... nine... nine... nine...",
		"c": "09/09/1999"
		}

	r11 = {
		"node_id" : str(ObjectId()),
		"parent_id" : [d1["node_id"]],
		"node_type" : "record",
		"acl" : ["117e8833-68f5-4cb2-afb3-05b25db69be1", "c8745ef4-d274-11e5-bee8-3b6845397ac9"],
		"tree" : 2,
		"string_all" : "returneverything",
		"a" : 778,
		"b" : "PRIVATE STUFF",
		"c": "12/12/2014"
		}

	r12 = {
		"node_id" : str(ObjectId()),
		"parent_id" : [d1["node_id"]],
		"node_type" : "record",
		"acl" : ["public"],
		"tree" : 2,
		"string_all" : "returneverything",
		"a" : 112,
		"b" : "PUBLIC STUFF #1",
		"c": "01/01/2001"
		}

	r13 = {
		"node_id" : str(ObjectId()),
		"parent_id" : [d1["node_id"]],
		"node_type" : "record",
		"acl" : ["public"],
		"tree" : 2,
		"string_all" : "returneverything",
		"a" : 223,
		"b" : "PUBLIC STUFF #2",
		"c": "02/02/2002"
		}

	r21 = {
		"node_id" : str(ObjectId()),
		"parent_id" : [d2["node_id"]],
		"node_type" : "record",
		"acl" : ["public"],
		"tree" : 3,
		"string_all" : "returneverything",
		"a" : 333,
		"b" : "threes",
		"c": "10/11/2012"
		}

	r22 = {
		"node_id" : str(ObjectId()),
		"parent_id" : [d2["node_id"]],
		"node_type" : "record",
		"acl" : ["public"],
		"tree" : 3,
		"string_all" : "returneverything",
		"a" : 555,
		"b" : "fiver",
		"c": "08/10/2009"
		}

	r23 = {
		"node_id" : str(ObjectId()),
		"parent_id" : [d2["node_id"]],
		"node_type" : "record",
		"acl" : ["public"],
		"tree" : 3,
		"string_all" : "returneverything",
		"a" : 123456,
		"b" : "counting to six",
		"c": "10/11/2012"
		}

	r31 = {
		"node_id" : str(ObjectId()),
		"parent_id" : [d3["node_id"]],
		"node_type" : "record",
		"acl" : ["117e8833-68f5-4cb2-afb3-05b25db69be1", "c8745ef4-d274-11e5-bee8-3b6845397ac9"],
		"tree" : 1,
		"string_all" : "returneverything",
		"a" : 1010001,
		"b" : "trade secrets",
		"c": "05/11/2003"
		}

	r32 = {
		"node_id" : str(ObjectId()),
		"parent_id" : [d3["node_id"]],
		"node_type" : "record",
		"acl" : ["117e8833-68f5-4cb2-afb3-05b25db69be1", "c8745ef4-d274-11e5-bee8-3b6845397ac9"],
		"tree" : 1,
		"string_all" : "returneverything",
		"a" : 10010101101,
		"b" : "unpublished work",
		"c": "05/12/2004"
		}

	r33 = {
		"node_id" : str(ObjectId()),
		"parent_id" : [d3["node_id"]],
		"node_type" : "record",
		"acl" : ["117e8833-68f5-4cb2-afb3-05b25db69be1", "c8745ef4-d274-11e5-bee8-3b6845397ac9"],
		"tree" : 1,
		"string_all" : "returneverything",
		"a" : 10011010102,
		"b" : "general nonpublic info",
		"c": "01/01/1970"
		}
	l1 = [c1,c2,c3,d1,d2,d3,r11,r12,r13,r21,r22,r23,r31,r32,r33]
	with open("test_data.json", 'w') as out:
		for node in l1:
			dump(node, out)
			out.write('\n')

	client = globus_auth.login("https://datasearch.api.demo.globus.org/")
	for node in l1:
		node["@context"] = {"sample":"samp"}
		gmeta["ingest_data"]["subject"] = "bson://" + node["node_id"]
		gmeta["ingest_data"]["visible_to"] = node["acl"]
		gmeta["ingest_data"]["content"] = node
#		print(gmeta)
		print(client.ingest(gmeta))



if __name__ == "__main__":
	gen_test_data()




