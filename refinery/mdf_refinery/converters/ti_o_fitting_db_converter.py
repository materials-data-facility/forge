import json
import sys
import os

from tqdm import tqdm

from mdf_refinery.validator import Validator
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_forge.toolbox import find_files

# VERSION 0.3.0

# This is the converter for the Ti-O Fitting Database.
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
            "title": "Fitting database entries for a modified embedded atom method potential for interstitial oxygen in titanium",
            "acl": ["public"],
            "source_name": "ti_o_fitting_db",
            "citation": ["Trinkle, Dallas R.; Zhang, Pinchao Fitting database entries for a modified embedded atom method potential for interstitial oxygen in titanium (2016-07-25) http://hdl.handle.net/11256/782"],
            "data_contact": {

                "given_name": "Dallas",
                "family_name": "Trinkle",

                "email": "dtrinkle@illinois.edu",
                "institution": "University of Illinois",

                },

            "author": [{

                "given_name": "Dallas",
                "family_name": "Trinkle",

                "email": "dtrinkle@illinois.edu",
                "institution": "University of Illinois",

                },
                {

                "given_name": "Pinchao",
                "family_name": "Zhang",

                "institution": "University of Illinois",

                }],

            "license": "http://creativecommons.org/licenses/by/3.0/us/",

            "collection": "Ti-O Fitting Database",
            "tags": ["dft", "atom potential"],

            "description": "Modeling oxygen interstitials in titanium requires a new empirical potential. We optimize potential parameters using a fitting database of first-principle oxygen interstitial energies and forces. A new database optimization algorithm based on Bayesian sampling is applied to obtain an optimal potential for a specific testing set of density functional data. A parallel genetic algorithm minimizes the sum of logistic function evaluations of the testing set predictions. We test the transferability of the potential model against oxygen interstitials in HCP titanium, transition barriers between oxygen interstitial sites, oxygen in the titanium prismatic stacking fault. The potential is applicable to oxygen interaction with the titanium screw dislocation, and predicts that the interactions between oxygen and the dislocation core is weak and short-ranged.",
            "year": 2016,

            "links": {

                "landing_page": "https://materialsdata.nist.gov/dspace/xmlui/handle/11256/782",

#                "publication": ,
                "data_doi": "http://hdl.handle.net/11256/782",

#                "related_id": ,

                # data links: {

                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #}
                },

#            "mrr": ,

            "data_contributor": {
                "given_name": "Jonathon",
                "family_name": "Gaff",
                "email": "jgaff@uchicago.edu",
                "institution": "The University of Chicago",
                "github": "jgaff"
                }
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
    i = 0
    for data_file in tqdm(find_files(input_path, "OUTCAR"), desc="Processing files", disable= not verbose):
        try:
            data = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "vasp-out")
        except Exception as e:
            print("Error on:", data_file["path"] + "/" + data_file["filename"], ":\n", repr(e))
            i +=1
            continue
        record_metadata = {
        "mdf": {
            "title": "Ti-O Fitting Database - " + data["chemical_formula"],
            "acl": ["public"],

#            "tags": ,
#            "description": ,
            
            "composition": data["chemical_formula"],
#            "raw": ,

            "links": {
#                "landing_page": ,

#                "publication": ,
#                "dataset_doi": ,

#                "related_id": ,

                "outcar": {
 
                    "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                    "http_host": "https://data.materialsdatafacility.org",

                    "path": "/collections/" + data_file["no_root_path"] + "/" + data_file["filename"],
                    },
                },

#            "citation": ,
#            "data_contact": {

#                "given_name": ,
#                "family_name": ,

#                "email": ,
#                "institution":,

                # IDs
#                },

#            "author": ,

#            "license": ,
#            "collection": ,
#            "data_format": ,
#            "data_type": ,
#            "year": ,

#            "mrr":

#            "processing": ,
#            "structure":,
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
        print("Bad:", i)

