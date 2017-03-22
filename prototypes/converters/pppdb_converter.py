from json import loads, dump, load
import mysql.connector
from sys import exit
from tqdm import tqdm
import paths
from utils import dc_validate
from bson import ObjectId

sql_que = "SELECT r.doi, r.type, r.temperature, r.tempunit, r.chinumber, r.chierror, r.chia, r.chiaerror, r.chib, r.chiberror, r.chic, r.chicerror, r.notes, r.indirect, r.reference, r.compound1, r.compound2, p.authors, p.date, r.id FROM reviewed_chis r JOIN papers p ON p.doi = r.doi"

#Converter for PPPDB
def convert_pppdb(out_stock, login_file, mdf_meta, sack_size=0, out_sack=None, verbose=False):
	with open(login_file) as login_in:
		login = load(login_in)
	try:
		conn = mysql.connector.connect(user=login["user"], password=login["password"], database=login["database"])
	except Exception as e:
		exit("Error connecting to database: " + repr(e))
	if not conn.is_connected():
		exit("Error: Connection failed. Please verify the username and password in " + login_file)
	query = sql_que
	try:
		cursor = conn.cursor()
		cursor.execute(query)
	except mysql.connector.OperationalError as e:
		exit("Error: Bad cursor: " + repr(e))

	with open(out_stock, 'w') as output:
		if sack_size and out_sack:
			sack = open(out_sack, 'w')
		else:
			sack = None
		for tup in tqdm(cursor, desc="Processing records", disable= not verbose):
			record = {
				"doi" : tup[0],
				"type" : tup[1],
				"temperature" : tup[2],
				"tempunit" : tup[3],
				"chinumber" : tup[4],
				"chierror" : tup[5],
				"chia" : tup[6],
				"chiaerror" : tup[7],
				"chib" : tup[8],
				"chiberror" : tup[9],
				"chic" : tup[10],
				"chicerror" : tup[11],
				"notes" : tup[12],
				"indirect" : tup[13],
				"reference" : tup[14],
				"compound1" : tup[15],
				"compound2" : tup[16],
				"authors" : tup[17],
				"date" : tup[18],
				"pppdb_id" : tup[19] #CHANGED FROM 'id'
				}
			feedstock_data = {}
			feedstock_data["dc.title"] = "PPPDB - Chi Parameter for " +  record["compound1"] + " and " + record["compound2"]
			feedstock_data["dc.creator"] = "PPPDB"
			author_list = str(record["authors"]).replace(", III", " (III)").replace(", and", ", ").replace("and", ",").split(",")
			feedstock_data["dc.contributor.author"] = [author.strip() for author in author_list]
			feedstock_data["dc.identifier"] = "http://pppdb.uchicago.edu&id="+str(record["pppdb_id"])
			feedstock_data["dc.subject"] = []
			feedstock_data["dc.description"] = str(record["notes"]) if record["notes"] else ""
			feedstock_data["dc.relatedidentifier"] = [str(record["doi"])] 
			feedstock_data["dc.year"] = int(str(record["date"])[-4:])
			feedstock_data["mdf-base.materials_composition"] = [record["compound1"], record["compound2"]]

			dc_valid = dc_validate(feedstock_data)
			if not dc_valid["valid"]:
				exit("Error in metadata: Invalid fields: " + str(dc_valid["invalid_fields"]))

			feedstock_data["mdf_id"] = str(ObjectId())
			feedstock_data["mdf_source_name"] = mdf_meta["mdf_source_name"]
			feedstock_data["mdf_source_id"] = mdf_meta["mdf_source_id"]
#			feedstock_data["globus_source"] = mdf_meta.get("globus_source", "")
			feedstock_data["mdf_datatype"] = mdf_meta["mdf_datatype"]
			feedstock_data["acl"] = mdf_meta["acl"]
			feedstock_data["globus_subject"] = feedstock_data["dc.identifier"]
			feedstock_data["mdf-publish.publication.collection"] = mdf_meta["collection"]
			feedstock_data["data"] = record

			dump(feedstock_data, output)
			output.write("\n")
			
			if sack and sack_size:
				dump(feedstock_data, sack)
				sack_size -= 1
		if sack:
			sack.close()


if __name__ == "__main__":
	mdf_metadata = {
		"mdf_source_name" : "pppdb",
		"mdf_source_id" : 15,
#		"globus_source" : "Polymer Property Predictor Database",
		"mdf_datatype" : "pppdb",
		"acl" : ["public"],
		"collection" : "Polymer Property Predictor Database"
		}
	convert_pppdb(out_stock=paths.raw_feed+"pppdb_all.json", login_file="sql_login.json", mdf_meta=mdf_metadata, sack_size=10, out_sack=paths.sack_feed+"pppdb_10.json", verbose=True)


