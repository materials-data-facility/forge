import json
import sys
import os

from tqdm import tqdm

from mdf_refinery.validator import Validator
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase

# VERSION 0.3.0

# This is the converter for polymer data from Khazana.
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
                "title": "Khazana (Polymer)",
                "acl": ["public"],
                "source_name": "khazana_polymer",
                "citation": ["T. D. Huan, A. Mannodi-Kanakkithodi, C. Kim, V. Sharma, G. Pilania, R. Ramprasad\nA polymer dataset for accelerated property prediction and design Sci. Data, 3, 160012 (2016).", "A. Mannodi-Kanakkithodi, G. M. Treich, T. D. Huan, R. Ma, M. Tefferi, Y. Cao, G A. Sotzing, R. Ramprasad\nRational Co-Design of Polymer Dielectrics for Energy Storage Adv. Mater., 28, 6277 (2016).", "T. D. Huan, A. Mannodi-Kanakkithodi, R. Ramprasad\nAccelerated materials property predictions and design using motif-based fingerprints Phys. Rev. B, 92, 014106 (2015).", "A. Mannodi-Kanakkithodi, G. Pilania, T. D. Huan, T. Lookman, R. Ramprasad\nMachine learning strategy for accelerated design of polymer dielectrics Sci. Rep., 6, 20952 (2016)."],
                "data_contact": {

                    "given_name": "Rampi",
                    "family_name": "Ramprasad",

                    "email": "rampi.ramprasad@uconn.edu",
                    "institution": "University of Connecticut",

                    },

                "author": [{

                    "given_name": "Rampi",
                    "family_name": "Ramprasad",

                    "email": "rampi.ramprasad@uconn.edu",
                    "institution": "University of Connecticut",

                    },
                    {

                    "given_name": "Chiho",
                    "family_name": "Kim",

                    "institution": "University of Connecticut",

                    },
                    {

                    "given_name": "Huan",
                    "family_name": "Tran",

                    "institution": "University of Connecticut",

                    },
                    {

                    "given_name": "Arun",
                    "family_name": "Mannodi-Kanakkithodi",

                    "institution": "University of Connecticut",

                    }],

    #            "mdf-license": ,

                "collection": "Khazana",
                "tags": ["polymer"],

                "description": "Polymer Genome is a recommendation engine for the rapid design and discovery of polymer dielectrics, powered by quantum mechanical computations, experimental data and machine learning based models. Polymer Genome is designed to provide efficient pathways for estimating essential properties of existing/hypothetical polymers and recommending polymer candidates that meet certain property requirements.",
    #            "mdf-year": ,

                "links": {

                    "landing_page": "http://khazana.uconn.edu/polymer_genome/index.php",

                    "publication": ["https://dx.doi.org/10.1038/sdata.2016.12", "https://dx.doi.org/10.1002/adma.201600377", "https://doi.org/10.1103/PhysRevB.92.014106", "https://dx.doi.org/10.1038/srep20952"],
    #                "mdf-dataset_doi": ,

    #                "mdf-related_id": ,

                    # data links: {

                        #"globus_endpoint": ,
                        #"http_host": ,

                        #"path": ,
                        #}
                    },

    #            "mdf-mrr": ,

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
    for dir_data in tqdm(find_files(input_path, "\.cif$"), desc="Processing data files", disable= not verbose):
        file_data = parse_ase(file_path=os.path.join(dir_data["path"], dir_data["filename"]), data_format="cif", verbose=False)
        record_metadata = {
            "mdf": {
                "title": "Khazana Polymer - " + file_data["chemical_formula"],
                "acl": ["public"],

    #            "mdf-tags": ,
    #            "mdf-description": ,
                
                "composition": file_data["chemical_formula"],
    #            "mdf-raw": ,

                "links": {
                    "landing_page": "http://khazana.uconn.edu/module_search/material_detail.php?id=" + dir_data["filename"].replace(".cif", ""),

    #                "mdf-publication": ,
    #                "mdf-dataset_doi": ,

    #                "mdf-related_id": ,

                    # data links: {
     
                        #"globus_endpoint": ,
                        #"http_host": ,

                        #"path": ,
                        #},
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
