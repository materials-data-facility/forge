import json
import sys
import os

from tqdm import tqdm

from ..validator.schema_validator import Validator
from ..utils.file_utils import find_files
from ..parsers.ase_parser import parse_ase

# VERSION 0.2.0

# This is the converter for the QM MD Trajectories of C7O2H10 dataset
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
            "mdf-title": "Quantum Machine - MD Trajectories of C7O2H10",
            "mdf-acl": ["public"],
            "mdf-source_name": "qm_mdt_c",
            "mdf-citation": ["S. Chmiela, A. Tkatchenko, H. E. Sauceda, I. Poltavsky, K. T. Schütt, K.-R. Müller Machine Learning of Accurate Energy-Conserving Molecular Force Fields, 2017.", "K. T. Schütt, F. Arbabzadah, S. Chmiela, K.-R. Müller, A. Tkatchenko Quantum-Chemical Insights from Deep Tensor Neural Networks, Nat. Commun. 8, 13890, 2017."],

            "mdf-data_contact": {

                "given_name": "Alexandre",
                "family_name": "Tkatchenko",

                "email": "alexandre.tkatchenko@uni.lu",
                "institution": "University of Luxembourg"

                # IDs
                },

            "mdf-author": [{

                "given_name": "Alexandre",
                "family_name": "Tkatchenko",

                "email": "alexandre.tkatchenko@uni.lu",
                "institution": "University of Luxembourg"

                # IDs
                },
                {

                "given_name": "Kristof",
                "family_name": "Schütt",

                "institution": "Technical University of Berlin"

                # IDs
                },
                {

                "given_name": "Farhad",
                "family_name": "Arbabzadah",

                "institution": "Technical University of Berlin"

                # IDs
                },
                {

                "given_name": "Stefan",
                "family_name": "Chmiela",

                "institution": "Technical University of Berlin"

                # IDs
                },
                {
                "given_name": "Klaus",
                "family_name": "Müller",

                "institution": "Technical University of Berlin"
                }],

#            "mdf-license": ,

            "mdf-collection": "Quantum Machine",
            "mdf-data_format": "xyz",
            "mdf-data_type": "Molecular dynamic simulation",
            "mdf-tags": ["molecular", "dynamics", "trajectories", "DFT", "density functional theory", "PBE", "exchange", "simulation"],

            "mdf-description": "This data set consists of molecular dynamics trajectories of 113 randomly selected C7O2H10 isomers calculated at a temperature of 500 K and resolution of 1fs using density functional theory with the PBE exchange-correlation potential.",
            "mdf-year": 2016,

            "mdf-links": {

                "mdf-landing_page": "http://quantum-machine.org/datasets/#C7O2H10",

                "mdf-publication": ["https://dx.doi.org/10.1038/ncomms13890"],
#                "mdf-dataset_doi": ,

#                "mdf-related_id": ,

                "tar_gz": {

                    #"globus_endpoint": ,
                    "http_host": "http://quantum-machine.org",

                    "path": "/data/c7o2h10_md.tar.gz",
                    }
                },

#            "mdf-mrr": ,

            "mdf-data_contributor": [{
                "given_name": "Jonathon",
                "family_name": "Gaff",
                "email": "jgaff@uchicago.edu",
                "institution": "The University of Chicago",
                "github": "jgaff"
                }]
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
    for file_data in tqdm(find_files(os.path.join(input_path, "c7o2h10_md"), "xyz"), desc="Processing QM_MDT_C", disable= not verbose):
        file_path = os.path.join(file_data["path"], file_data["filename"])
        record = parse_ase(file_path, "xyz")
        record_metadata = {
            "mdf-title": "MD Trajectories of C7O2H10 - " + record.get("chemical_formula", "") + " - " + file_data["filename"],
            "mdf-acl": ["public"],

#            "mdf-tags": ,
#            "mdf-description": ,
            
            "mdf-composition": record.get("chemical_formula", ""),
#            "mdf-raw": ,

            "mdf-links": {
                "mdf-landing_page": "https://data.materialsdatafacility.org/collections/test/md_trajectories_of_c7o2h10/c7o2h10_md/" + file_data["no_root_path"] + "/" if file_data["no_root_path"] else "" + file_data["filename"],

#                "mdf-publication": ,
#                "mdf-dataset_doi": ,

#                "mdf-related_id": ,

                "xyz": {
 
                    "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                    "http_host": "https://data.materialsdatafacility.org",

                    "path": "/collections/test/md_trajectories_of_c7o2h10/c7o2h10_md/" + file_data["filename"],
                    },
                "energy": {
                    "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                    "http_host": "https://data.materialsdatafacility.org",

                    "path": "/collections/test/md_trajectories_of_c7o2h10/c7o2h10_md/" + file_data["filename"].replace(".xyz", "") + ".energy.dat"
                    }
                },

#            "mdf-citation": ,
#            "mdf-data_contact": {

#                "given_name": ,
#                "family_name": ,

#                "email": ,
#                "institution":,

                # IDs
#                },

#            "mdf-author": ,

#            "mdf-license": ,
#            "mdf-collection": ,
#            "mdf-data_format": ,
#            "mdf-data_type": ,
#            "mdf-year": ,

#            "mdf-mrr":

#            "mdf-processing": ,
#            "mdf-structure":,
            "temperature" : {
                "value": 500,
                "unit": "kelvin"
                },
            "resolution" : {
                "value" : 1,
                "unit" : "femtosecond"
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
