import json
import sys
from urllib.parse import quote

import psycopg2
from tqdm import tqdm

from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for the XAFS Spectra Library
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
            "title": "XAFS Spectra Library",
            "acl": ["public"],
            "source_name": "xafs_sl",
            "citation": ["http://cars.uchicago.edu/xaslib"],
            "data_contact": {

                "given_name": "Matthew",
                "family_name": "Newville",

                "email": "newville@cars.uchicago.edu",
                "institution": "The University of Chicago"
                },

            "author": {

                "given_name": "Matthew",
                "family_name": "Newville",

                "email": "newville@cars.uchicago.edu",
                "institution": "The University of Chicago"
                },

#            "license": ,

            "collection": "XAFS SL",
            "tags": ["XAFS", "Spectra"],

            "description": "This is a collection of X-ray Absorption Spectra. The data here are intended to be of good quality, and on well-characterized samples, but no guarantees are made about either of these intentions.",
#            "year": ,

            "links": {

                "landing_page": "http://cars.uchicago.edu/xaslib",

#                "publication": ,
#                "dataset_doi": ,

#                "related_id": ,

                # data links: {

                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #}
                },

#            "mrr": ,

            "data_contributor":[{
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
    query = "select s.id, s.name, s.rating_summary, s.collection_date, s.submission_date, s.d_spacing, s.comments, b.name, f.fullname, s1.name, s1.preparation, s1.formula, s2.name, ed.name, en.units, p.name from spectrum s, beamline b, facility f, sample s1, sample s2, edge ed, energy_units en, person p where b.id=s.beamline_id and f.id=b.facility_id and s1.id=s.sample_id and s2.id=s.reference_id and ed.id=s.edge_id and en.id=s.energy_units_id and p.id=s.person_id;"
    #Translation vars for convenience and code readability
    s_id = 0
    s_name = 1
    s_rating_summary = 2
    s_collection_date = 3
    s_submission_date = 4
    s_d_spacing = 5
    s_comments = 6
    b_name = 7
    f_fullname = 8
    s1_name = 9
    s1_preparation = 10
    s1_formula = 11
    s2_name = 12
    ed_name = 13
    en_units = 14
    p_name = 15
    #Make connection to Postgres
    with psycopg2.connect("dbname=xafs") as conn:
        with conn.cursor() as cursor:
            cursor.execute(query) #Run query
            #Process each row
            for record in tqdm(cursor, desc="Processing database", disable= not verbose):
                record_metadata = {
                "mdf": {
                    "title": record[s_name],
                    "acl": ["public"],

#                    "tags": ,
                    "description": record[s_comments],
                    
                    "composition": record[s1_formula],
#                    "raw": ,

                    "links": {
                        "landing_page": quote("http://cars.uchicago.edu/xaslib/spectrum/" + str(record[s_id]), safe="/:"),

#                        "publication": ,
#                        "dataset_doi": ,

#                        "related_id": ,

                        "xdi": {
                            #"globus_endpoint": ,
                            "http_host": "http://cars.uchicago.edu",
                            "path": "/" + quote(str(record[s_id]) + "/" + record[s_name] + ".xdi", safe="/:"),
                            },
                        },

#                    "citation": ,
#                    "data_contact": {

#                        "given_name": ,
#                        "family_name": ,

#                        "email": ,
#                        "institution":,

                        # IDs
#                        },

#                    "author": ,

#                    "license": ,
#                    "collection": ,
#                    "data_format": ,
#                    "data_type": ,
#                    "year": ,

#                    "mrr":

        #            "processing": ,
        #            "structure":,
                    },
                    "xafs_sl": {
                    "ratings" : record[s_rating_summary],
                    "absorption_edge" : record[ed_name],
                    "sample_name" : record[s1_name],
                    "sample_prep" : record[s1_preparation],
                    "reference_sample" : record[s2_name],
                    "beamline" : record[f_fullname] + " - " + record[b_name],
                    "energy_units" : record[en_units],
                    "d_spacing" : record[s_d_spacing],
                    "date_measured" : str(record[s_collection_date]),
#                    "date_uploaded" : str(record[s_submission_date]),
                    "user_comments" : record[s_comments]
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
