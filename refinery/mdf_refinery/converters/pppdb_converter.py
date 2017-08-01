import json
import sys
import os

import mysql.connector
from tqdm import tqdm
from chemspipy import ChemSpider

from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for the PPPDB
# Arguments:
#   input_path (string): The file or directory where the data resides.
#       NOTE: Do not hard-code the path to the data in the converter. The converter should be portable.
#   metadata (string or dict): The path to the JSON dataset metadata file, a dict or json.dumps string containing the dataset metadata, or None to specify the metadata here. Default None.
#   verbose (bool): Should the script print status messages to standard output? Default False.
#       NOTE: The converter should have NO output if verbose is False, unless there is an error.
def convert(input_path, metadata=None, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    if not metadata:
        dataset_metadata = {
        "mdf": {
            "title": "Polymer Property Predictor and Database",
            "acl": ["public"],
            "source_name": "pppdb",
            "citation": ["Publication pending"],
            "data_contact": {

                "given_name": "Roselyne",
                "family_name": "Tchoua",

                "email": "roselyne@cs.uchicago.edu",
                "institution": "The University of Chicago",

                # IDs
                },

            "author": [{

                "given_name": "Roselyne",
                "family_name": "Tchoua",

                "email": "roselyne@cs.uchicago.edu",
                "institution": "The University of Chicago",

                },
                {
                "given_name": "Deborah",
                "family_name": "Aldus",

                "institution": "The University of Chicago",
                },
                {
                "given_name": "Ian",
                "family_name": "Foster",

                "institution": "The University of Chicago",
                }
                ],

#            "license": ,

            "collection": "PPPDB",
#            "data_type": ,
            "tags": ["chi", "tg"],

            "description": "Polymer Property Predictor and Database",
            "year": 2017,

            "links": {

                "landing_page": "http://pppdb.uchicago.edu/",

#                "publication": ,
#                "data_doi": ,

#                "related_id": ,

                # data links: {

                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #}
                },

#            "mrr": ,

            "data_contributor": [{
                "given_name": "Jonathon",
                "family_name": "Gaff",
                "email": "jgaff@uchicago.edu",
                "institution": "The University of Chicago",
                "github": "jgaff"
                }]
            }
        }
    elif type(metadata) is str:
        try:
            dataset_metadata = json.loads(metadata)
        except Exception:
            try:
                with open(metadata, 'r') as metadata_file:
                    dataset_metadata = json.load(metadata_file)
            except Exception as e:
                sys.exit("Error: Unable to read metadata: " + repr(e))
    elif type(metadata) is dict:
        dataset_metadata = metadata
    else:
        sys.exit("Error: Invalid metadata parameter")


    dataset_validator = Validator(dataset_metadata)


    # Get the data
    query = "SELECT r.doi, r.type, r.temperature, r.tempunit, r.chinumber, r.chierror, r.chia, r.chiaerror, r.chib, r.chiberror, r.chic, r.chicerror, r.notes, r.indirect, r.reference, r.compound1, r.compound2, p.authors, p.date, r.id FROM reviewed_chis r JOIN papers p ON p.doi = r.doi"

    with open(os.path.join(input_path, "pppdb_logins.json")) as login_in:
        login = json.load(login_in)
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

    try:
        cs = ChemSpider(login["chemspi_token"])
    except Exception as e:
        exit("Error: Cannot authenticate to ChemSpider: " + repr(e))

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

        # Get chemical formulas for the compounds
        comp1 = cs.search(record["compound1"].replace("poly(", "").replace(")", ""))
        comp2 = cs.search(record["compound2"].replace("poly(", "").replace(")", ""))
        # If exactly one result is returned, get the formula and strip non-alphanumeric characters
        chem1 = (''.join([ch for ch in comp1[0].molecular_formula if ch.isalnum()])) if len(comp1) == 1 else ""
        chem2 = (''.join([ch for ch in comp2[0].molecular_formula if ch.isalnum()])) if len(comp2) == 1 else ""

        record_metadata = {
        "mdf": {
            "title": "PPPDB - Chi Parameter for " +  record["compound1"] + " and " + record["compound2"],
            "acl": ["public"],

            "tags": [record["compound1"] , record["compound2"]],
            "description": str(record["notes"]) if record["notes"] else "Chi Parameter for " +  record["compound1"] + " and " + record["compound2"],
            
            "composition": chem1 + " " + chem2,
            "raw": json.dumps(record),

            "links": {
                "landing_page": "http://pppdb.uchicago.edu?&id="+str(record["pppdb_id"]),

                "publication": [str(record["doi"])],
#                "dataset_doi": ,

#                "related_id": ,

                # data links: {
 
                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #},

#            "citation": record["reference"],
#            "data_contact": {

#                "given_name": ,
#                "family_name": ,

#                "email": ,
#                "institution":,

                # IDs
            },

#            "author": ,

#            "license": ,
#            "collection": ,
#            "data_format": ,
#            "data_type": ,
            "year": int(str(record["date"])[-4:]),

#            "mrr":

#            "processing": ,
#            "structure":,
            },
        "pppdb": {
#            "doi": record["doi"],
            "type" : record["type"],
            "temperature" : record["temperature"],
            "tempunit" : record["tempunit"],
            "chinumber" : record["chinumber"],
            "chierror" : record["chierror"],
#            "chia" : record["chia"],
#            "chiaerror" : record["chiaerror"],
#            "chib" : record["chib"],
#            "chiberror" : record["chiberror"],
#            "chic" : record["chic"],
#            "chicerror" : record["chicerror"],
#            "notes" : record["notes"],
#            "indirect" : record["indirect"],
            "reference" : record["reference"],
#            "compound1" : record["compound1"],
#            "compound2" : record["compound2"],
            "authors" : record["authors"],
            "date" : record["date"],
            "id": record["pppdb_id"]
        }
        }

        # Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)

        # Check if the Validator accepted the record, and print a message if it didn't
        # If the Validator returns "success" == True, the record was written successfully
        if result["success"] is not True:
            print("Error:", result["message"])


    if verbose:
        print("Finished converting")
