import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: BaCrO3-x JSSC 2015 (High-pressure BaCrO3 polytypes and the 5H–BaCrO2.8 phase)
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

                "title": "BaCrO3-x JSSC 2015 (High-pressure BaCrO3 polytypes and the 5H–BaCrO2.8 phase)",
                "acl": ["public"],
                "source_name": "high_pressure_ba_cro3",

                "data_contact": {
                    
                    "given_name": "Attfield J.",
                    "family_name": "Paul",
                    "email": "j.p.attfield@ed.ac.uk",
                    "institution": "University of Edinburgh School of Chemistry",

                },

                "data_contributor": [{
                    
                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78",

                }],

                "citation": ["Attfield, J. Paul. (2015). BaCrO3-x JSSC 2015, 2014-2015 [dataset]. University of Edinburgh School of Chemistry. http://dx.doi.org/10.7488/ds/305."],

                "author": [{

                    "given_name": "Attfield J.",
                    "family_name": "Paul",
                    "email": "j.p.attfield@ed.ac.uk",
                    "institution": "University of Edinburgh School of Chemistry",

                }],

                "license": "http://creativecommons.org/licenses/by/4.0/legalcode",
                "collection": "High Pressure Ba-CrO3",
                "tags": ["Reduced oxides", "Perovskites", "High pressure synthesis", "Vacancyordering", "Magnetic structure"],
                "description": "Polytypism of BaCrO3 perovskites has been investigated at 900–1100 °C and pressures up to 22 GPa. Hexagonal 5H, 4H, and 6H perovskites are observed with increasing pressure, and the cubic 3C perovskite (a=3.99503(1) Å) is observed in bulk form for the first time at 19–22 GPa. An oxygen-deficient material with limiting composition 5H–BaCrO2.8 is synthesised at 1200 °C under ambient pressure. This contains double tetrahedral Cr4+ layers and orders antiferromagnetically below 260 K with a (0 0 1/2) magnetic structure.",
                "year": 2015,

                "links": {

                    "landing_page": "http://www.research.ed.ac.uk/portal/en/datasets/bacro3x-jssc-2015-highpressure-bacro3-polytypes-and-the-5hbacro28-phase(17dcd792-2bb9-43d9-b244-a1d3a3ea7c15).html",
                    "publication": ["http://dx.doi.org/10.1016/j.jssc.2015.09.029"],
                    "data_doi": "http://dx.doi.org/10.7488/ds/305",
                    #"related_id": ,

                    "zip": {

                        #"globus_endpoint": ,
                        "http_host": "http://datashare.is.ed.ac.uk",

                        "path": "/bitstream/handle/10283/862/BaCrO3Data.zip?sequence=1&isAllowed=y",
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
    for data_file in tqdm(find_files(input_path, "cif"), desc="Processing files", disable=not verbose):
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "cif")
        ## Metadata:record
        record_metadata = {
            "mdf": {

                "title": "High Pressure Ba-CrO3 - " + record["chemical_formula"],
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

                        "path": "/collections/high_pressure_ba_cro3/" + data_file["no_root_path"] + "/" + data_file["filename"],
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
        if "X-ray" in data_file["path"]:
            if "oxidised" in data_file["filename"]:
                ext = ".XY"
            else:
                ext = ".xye"
            name = data_file["filename"].split(".")[0] + ext
            record_metadata["mdf"]["links"][ext[1:]] = {
                
                "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                "http_host": "https://data.materialsdatafacility.org",

                "path": "/collections/high_pressure_ba_cro3/" + data_file["no_root_path"] + "/" + name,
            }

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
