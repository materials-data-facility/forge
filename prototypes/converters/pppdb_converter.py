import mysql.connector
from sys import exit
from json import load
from tqdm import tqdm
from validator import Validator


# This is the converter for the PPPDB
# Arguments:
#   login_file (string): A file containing login information for the MySQL database.
#   verbose (bool): Should the script print status messages to standard output? Default False.
def convert(login_file, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    dataset_metadata = {
        "globus_subject": "http://pppdb.uchicago.edu/",
        "acl": ["public"],
        "mdf_source_name": "pppdb",
        "mdf-publish.publication.collection": "PPPDB",

        "dc.title": "Polymer Property Predictor and Database",
        "dc.creator": "University of Chicago",
        "dc.identifier": "http://pppdb.uchicago.edu/",
#        "dc.contributor.author": ,
#        "dc.subject": ,
#        "dc.description": ,
#        "dc.relatedidentifier": ,
        "dc.year": 2017
        }


    # Make a Validator to help write the feedstock
    # You must pass the metadata to the constructor
    # Each Validator instance can only be used for a single dataset
    dataset_validator = Validator(dataset_metadata)


    # Get the data
    # Each record also needs its own metadata
    query = "SELECT r.doi, r.type, r.temperature, r.tempunit, r.chinumber, r.chierror, r.chia, r.chiaerror, r.chib, r.chiberror, r.chic, r.chicerror, r.notes, r.indirect, r.reference, r.compound1, r.compound2, p.authors, p.date, r.id FROM reviewed_chis r JOIN papers p ON p.doi = r.doi"

    with open(login_file) as login_in:
        login = load(login_in)
    try:
        conn = mysql.connector.connect(user=login["user"], password=login["password"], database=login["database"])
    except Exception as e:
        exit("Error connecting to database: " + repr(e))
    if not conn.is_connected():
        exit("Error: Connection failed. Please verify the username and password in " + login_file)
    try:
        cursor = conn.cursor()
        cursor.execute(query)
    except mysql.connector.OperationalError as e:
        exit("Error: Bad cursor: " + repr(e))

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

        record_metadata = {
            "globus_subject": "http://pppdb.uchicago.edu&id="+str(record["pppdb_id"]),
            "acl": ["public"],
            "mdf-publish.publication.collection": "Polymer Property Predictor and Database",
#            "mdf_data_class": ,
            "mdf-base.material_composition": record["compound1"] + " " + record["compound2"],

            "dc.title": "PPPDB - Chi Parameter for " +  record["compound1"] + " and " + record["compound2"],
#            "dc.creator": ,
            "dc.identifier": "http://pppdb.uchicago.edu&id="+str(record["pppdb_id"]),
            "dc.contributor.author": [author.strip() for author in str(record["authors"]).replace(", III", " (III)").replace(", and", ", ").replace("and", ",").split(",")],
#            "dc.subject": ,
            "dc.description": str(record["notes"]) if record["notes"] else "Chi Parameter for " +  record["compound1"] + " and " + record["compound2"],
            "dc.relatedidentifier": [str(record["doi"])],
            "dc.year": int(str(record["date"])[-4:]),

            "data": {
#                "raw": ,
                "files": {}
                }
            }
        record_metadata["data"].update(record)

        # Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)

        # Check if the Validator accepted the record, and print a message if it didn't
        # If the Validator returns "success" == True, the record was written successfully
        if result["success"] is not True:
            print("Error:", result["message"], ":", result.get("invalid_metadata", ""))

    if verbose:
        print("Finished converting")


# Optionally, you can have a default call here for testing
# The convert function may not be called in this way, so code here is primarily for testing
if __name__ == "__main__":
    import paths
    convert("sql_login.json", True)
