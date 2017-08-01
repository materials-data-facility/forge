import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: Theoretical Investigations and Density Functional Theory Based Quantitative Structure–Activity Relationships Model for Novel Cytotoxic Platinum(IV) Complexes
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

                "title": "Theoretical Investigations and Density Functional Theory Based Quantitative Structure–Activity Relationships Model for Novel Cytotoxic Platinum(IV) Complexes",
                "acl": ["public"],
                "source_name": "cytotoxic_pt_complexes",

                "data_contact": {
                    
                    "given_name": "Markus",
                    "family_name": "Galanski",
                    "email": "markus.galanski@univie.ac.at",
                    "institution": "University of Vienna",

                },

                "data_contributor": [{
                    
                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78",

                }],

                "citation": ["Keppler, Bernhard K. (2013/01/10). Theoretical Investigations and Density Functional Theory Based Quantitative Structure–Activity Relationships Model for Novel Cytotoxic Platinum(IV) Complexes. Journal of Medicinal Chemistry, 56, 330-344. doi: 10.1021/jm3016427"],

                "author": [{

                    "given_name": "Hristo P.",
                    "family_name": "Varbanov",
                    "institution": "University of Vienna",

                },
                {

                    "given_name": "Michael A.",
                    "family_name": "Jakupec",
                    "institution": "University of Vienna",

                },
                {

                    "given_name": "Alexander",
                    "family_name": "Roller",
                    "institution": "University of Vienna",

                },
                {

                    "given_name": "Frank",
                    "family_name": "Jensen",
                    "email": "frj@chem.au.dk",
                    "institution": "University of Aarhus",

                },
                {

                    "given_name": "Markus",
                    "family_name": "Galanski",
                    "email": "markus.galanski@univie.ac.at",
                    "institution": "University of Vienna",

                },
                {

                    "given_name": "Bernhard K.",
                    "family_name": "Keppler",
                    "institution": "University of Vienna",

                }],

                "license": "https://creativecommons.org/licenses/by-nc/4.0/",
                "collection": "Cytotoxic Platinum Complexes",
                "tags": ["structure geometry", "series", "resistance", "Herein", "laboratory", "tetraki", "tris", "Relationship", "wb 97x", "mechanism", "cisplatin", "complex", "SW", "Cytotoxic", "calculation", "relationship", "Density Functional Theory", "DFT", "Reliable", "ComplexesOctahedral", "bi", "compound", "Quantitative", "Model", "QSAR investigations", "cytotoxicity", "candidate", "cell line CH 1", "descriptor", "optimization", "QSAR models", "toxicity", "Theoretical Investigations"],
                "description": "Octahedral platinum(IV) complexes are promising candidates in the fight against cancer. In order to rationalize the further development of this class of compounds, detailed studies on their mechanisms of action, toxicity, and resistance must be provided and structure–activity relationships must be drawn. Herein, we report on theoretical and QSAR investigations of a series of 53 novel bis-, tris-, and tetrakis(carboxylato)platinum(IV) complexes, synthesized and tested for cytotoxicity in our laboratories. ",
                "year": 2012,

                "links": {

                    "landing_page": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3557934/",
                    "publication": ["https://dx.doi.org/10.1021%2Fjm3016427"],
                    #"data_doi": "",
                    #"related_id": ,

                    "cif": {

                        #"globus_endpoint": ,
                        "http_host": "https://ndownloader.figshare.com",

                        "path": "/files/3593325",
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
    for data_file in tqdm(find_files(input_path, "temp_file.cif"), desc="Processing files", disable=not verbose):
        #Temp_file is the same as the real file, but with authors and adresses deleted so that ase can read composition
        #It should only be used for converting purposes
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "cif")
        ## Metadata:record
        record_metadata = {
            "mdf": {

                "title": "Cytotoxic Platinum Complexes - " + record["chemical_formula"],
                "acl": ["public"],
                "composition": record["chemical_formula"],

#                "tags": ,
#                "description": ,
                #"raw": json.dumps(record),

                "links": {

#                    "landing_page": ,
#                    "publication": ,
#                    "data_doi": ,
#                    "related_id": ,

                    "cif": {

                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",

                        "path": "/collections/cytotoxic_pt_complexes/" + "jm3016427_si_002.cif",
                        },
                    },

#                "citation": ,

#                "data_contact": {

#                    "given_name": ,
#                    "family_name": ,
#                    "email": ,
#                    "institution": ,

#                    },

#                "author": [{

#                    "given_name": ,
#                    "family_name": ,
#                    "email": ,
#                    "institution": ,

#                    }],

#                "year": ,

                },

           # "dc": {

           # },


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
