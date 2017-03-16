from json import loads, dump, load
import mysql.connector
from sys import exit
from tqdm import tqdm
import paths
from utils import dc_validate

sql_que = "SELECT r.doi, r.type, r.temperature, r.tempunit, r.chinumber, r.chierror, r.chia, r.chiaerror, r.chib, r.chiberror, r.chic, r.chicerror, r.notes, r.indirect, r.reference, r.compound1, r.compound2, p.authors, p.date, r.id FROM reviewed_chis r JOIN papers p ON p.doi = r.doi"

#Converter for PPPDB
def convert_pppdb(out_stock, login_file, sack_size=0, out_sack=None, verbose=False):
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
			record["dc.title"] = "PPPDB - Chi Parameter for " +  record["compound1"] + " and " + record["compound2"]
			record["dc.creator"] = "PPPDB"
			author_list = str(record["authors"]).replace(", III", " (III)").replace(", and", ", ").replace("and", ",").split(",")
			record["dc.contributor.author"] = [author.strip() for author in author_list]
			record["dc.identifier"] = "http://pppdb.uchicago.edu&id="+str(record["pppdb_id"])
			record["dc.subject"] = []
			record["dc.description"] = str(record["notes"]) if record["notes"] else ""
			record["dc.relatedidentifier"] = [str(record["doi"])] 
			record["dc.year"] = int(str(record["date"])[-4:])

			dc_valid = dc_validate(record)
			if not dc_valid["valid"]:
				exit("Error in metadata: Invaild fields: " + str(dc_valid["invalid_fields"]))

			dump(record, output)
			output.write("\n")
			
			if sack and sack_size:
				dump(record, sack)
				sack_size -= 1
		if sack:
			sack.close()


if __name__ == "__main__":
	convert_pppdb(out_stock=paths.raw_feed+"pppdb_all.json", login_file="sql_login.json", sack_size=10, out_sack=paths.sack_feed+"pppdb_10.json", verbose=True)


