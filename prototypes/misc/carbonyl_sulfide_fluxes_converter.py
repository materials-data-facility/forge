import json
import sys
import os
from tqdm import tqdm
from mdf_refinery.parsers.tab_parser import parse_tab
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: Dataset for "Canopy uptake dominates nighttime carbonyl sulfide fluxes in a boreal forest"
# Arguments:
#   input_path (string): The file or directory where the data resides.
#       NOTE: Do not hard-code the path to the data in the converter (the filename can be hard-coded, though). The converter should be portable.
#   metadata (string or dict): The path to the JSON dataset metadata file, a dict or json.dumps string containing the dataset metadata, or None to specify the metadata here. Default None.
#   verbose (bool): Should the script print status messages to standard output? Default False.
#       NOTE: The converter should have NO output if verbose is False, unless there is an error.
def convert(input_path, metadata=None, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    # NOTE: For fields that represent people (e.g. mdf-data_contact), other IDs can be added (ex. "github": "jgaff").
    #    It is recommended that all people listed in mdf-data_contributor have a github username listed.
    #
    # If there are other useful fields not covered here, another block (dictionary at the same level as "mdf") can be created for those fields.
    # The block must be called the same thing as the source_name for the dataset.
    if not metadata:
        ## Metadata:dataset
        dataset_metadata = {
            "mdf": {

                "title": "Dataset for \"Canopy uptake dominates nighttime carbonyl sulfide fluxes in a boreal forest\"",
                "acl": ["public"],
                "source_name": "carbonyl_sulfide_fluxes",

                "data_contact": {

                    "given_name": "Huilin",
                    "family_name": "Chen",
                    "email": "Huilin.Chen@rug.nl",
                    "institution": "University of Groningen, University of Colorado"

                },

                "data_contributor": [{

                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78",

                }],

                "citation": ["Linda M.J. Kooijmans, Kadmiel Maseyk, Ulli Seibt, Wu Sun, Timo Vesala, Ivan Mammarella, … Huilin Chen. (2017). Dataset for \"Canopy uptake dominates nighttime carbonyl sulfide fluxes in a boreal forest\" [Data set]. Zenodo. http://doi.org/10.5281/zenodo.580303"],

                "author": [{

                    "given_name": "Linda M.J.",
                    "family_name": "Kooijmans",
                    "institution": "University of Groningen",

                },
                {

                    "given_name": "Kadmiel",
                    "family_name": "Maseyk",
                    "institution": "The Open University",

                },
                {

                    "given_name": "Ulli",
                    "family_name": "Seibt",
                    "institution": "University of California",

                },
                {

                    "given_name": "Wu",
                    "family_name": "Sun",
                    "institution": "University of California",

                },
                {

                    "given_name": "Timo",
                    "family_name": "Vesala",
                    "institution": "University of Helsinki",

                },
                {

                    "given_name": "Ivan",
                    "family_name": "Mammarella",
                    "institution": "University of Helsinki",

                },
                {

                    "given_name": "Pasi",
                    "family_name": "Kolari",
                    "institution": "University of Helsinki",

                },
                {

                    "given_name": "Juho",
                    "family_name": "Aalto",
                    "institution": "University of Helsinki",

                },
                {

                    "given_name": "Alessandro",
                    "family_name": "Franchin",
                    "institution": "University of Helsinki, University of Colorado",

                },
                {

                    "given_name": "Roberta",
                    "family_name": "Vecchi",
                    "institution": "University of Milan",

                },
                {

                    "given_name": "Gianluigi",
                    "family_name": "Valli",
                    "institution": "University of Milan",

                },
                {

                    "given_name": "Huilin",
                    "family_name": "Chen",
                    "email": "Huilin.Chen@rug.nl",
                    "institution": "University of Groningen, University of Colorado",

                }],

                "license": "https://creativecommons.org/licenses/by/4.0/",
                "collection": "Carbonyl Sulfide Fluxes",
                #"tags": [""],
                "description": "Nighttime averaged ecosystem fluxes of COS and CO2 obtained through the radon-tracer and eddy-covariance method as presented in \"Canopy uptake dominates nighttime carbonyl sulfide fluxes in a boreal forest\" submitted to Atmospheric Chemistry and Physics.",
                "year": 2017,

                "links": {

                    "landing_page": "https://doi.org/10.5281/zenodo.580303",
                    "publication": ["https://www.atmos-chem-phys-discuss.net/acp-2017-407/"],
                    #"data_doi": "",
                    #"related_id": "",

                    "txt": {

                        #"globus_endpoint": ,
                        "http_host": "https://zenodo.org",

                        "path": "/record/580303/files/Kooijmans_et_al_2017_ACPD_20170516.txt",

                    },

                },

            },

            #"mrr": {

            #},

            #"dc": {

            #},


        }
        ## End metadata
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



    # Make a Validator to help write the feedstock
    # You must pass the metadata to the constructor
    # Each Validator instance can only be used for a single dataset
    # If the metadata is incorrect, the constructor will throw an exception and the program will exit
    dataset_validator = Validator(dataset_metadata)


    # Get the data
    #    Each record should be exactly one dictionary
    #    You must write your records using the Validator one at a time
    #    It is recommended that you use a parser to help with this process if one is available for your datatype
    #    Each record also needs its own metadata
    with open(os.path.join(input_path, "Kooijmans_et_al_2017_ACPD_20170516.txt"), "r") as raw_in:
        data = raw_in.read()
    description = "".join(data.split("\n\n")[1:2])
    start = "##########################################\n"
    for line in tqdm(parse_tab(data.split(start)[-1], sep=","), desc="Processing Data", disable=not verbose):
        ## Metadata:record
        record_metadata = {
            "mdf": {

                "title": "Carbonyl Sulfide Fluxes doy: " + line["doy"],
                "acl": ["public"],
                #"composition": ,

                #"tags": ,
                "description": description,
                "raw": json.dumps(line),

                "links": {

                    #"landing_page": ,
                    #"publication": ,
                    #"data_doi": ,
                    #"related_id": ,

                    "txt": {

                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",

                        "path": "/collections/carbonyl_sulfide_fluxes/Kooijmans_et_al_2017_ACPD_20170516.txt",

                    },

                },

                #"citation": ,

                #"data_contact": {

                    #"given_name": ,
                    #"family_name": ,
                    #"email": ,
                    #"institution": ,

                #},

                #"author": [{

                    #"given_name": ,
                    #"family_name": ,
                    #"email": ,
                    #"institution": ,

                #}],

                #"year": ,

            },

            #"dc": {

            #},


        }
        ## End metadata

        # Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)

        # Check if the Validator accepted the record, and stop processing if it didn't
        # If the Validator returns "success" == True, the record was written successfully
        if not result["success"]:
            if not dataset_validator.cancel_validation()["success"]:
                print("Error cancelling validation. The partial feedstock may not be removed.")
            raise ValueError(result["message"] + "\n" + result.get("details", ""))


    # You're done!
    if verbose:
        print("Finished converting")
