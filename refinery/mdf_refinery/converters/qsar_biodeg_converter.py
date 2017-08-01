import json
import sys
import os

from tqdm import tqdm

from mdf_refinery.validator import Validator
from mdf_refinery.parsers.tab_parser import parse_tab

# VERSION 0.2.0

# This is the converter for the QSAR biodegradation Data Set
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
            "mdf-title": "QSAR biodegradation Data Set",
            "mdf-acl": ["public"],
            "mdf-source_name": "qsar_biodeg",
            "mdf-citation": ["Mansouri, K., Ringsted, T., Ballabio, D., Todeschini, R., Consonni, V. (2013). Quantitative Structure - Activity Relationship models for ready biodegradability of chemicals. Journal of Chemical Information and Modeling, 53, 867-878", "Lichman, M. (2013). UCI Machine Learning Repository [http://archive.ics.uci.edu/ml]. Irvine, CA: University of California, School of Information and Computer Science."],
            "mdf-data_contact": {

                "given_name": "Davide",
                "family_name": "Ballabio",

                "email": "davide.ballabio@unimib.it",
                "institution": "Università degli Studi di Milano-Bicocca",

                },

            "mdf-author": [{

                "given_name": "Davide",
                "family_name": "Ballabio",

                "email": "davide.ballabio@unimib.it",
                "institution": "Università degli Studi di Milano-Bicocca",

                },
                {

                "given_name": "Kamel",
                "family_name": "Mansouri",

                "institution": "Università degli Studi di Milano-Bicocca",

                },
                {

                "given_name": "Tine",
                "family_name": "Ringsted",

                "institution": "Università degli Studi di Milano-Bicocca",

                },
                {

                "given_name": "Roberto",
                "family_name": "Todeschini",

                "institution": "Università degli Studi di Milano-Bicocca",

                },
                {

                "given_name": "Viviana",
                "family_name": "Consonni",

                "institution": "Università degli Studi di Milano-Bicocca",

                }],

#            "mdf-license": ,

            "mdf-collection": "QSAR Biodegradation Data Set",
            "mdf-data_format": "csv",
            "mdf-data_type": "Biodegradation",
            "mdf-tags": ["biodegredation", "Chemometrics"],

            "mdf-description": "Data set containing values for 41 attributes (molecular descriptors) used to classify 1055 chemicals into 2 classes (ready and not ready biodegradable).",
            "mdf-year": 2013,

            "mdf-links": {

                "mdf-landing_page": "https://archive.ics.uci.edu/ml/datasets/QSAR+biodegradation",

#                "mdf-publication": ,
#                "mdf-dataset_doi": ,

#                "mdf-related_id": ,

                # data links: {

                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #}
                },

 #           "mdf-mrr": ,

            "mdf-data_contributor": [{
                "given_name": "Evan",
                "family_name": "Pike",
                "email": "dep78@uchicago.edu",
                "institution": "The University of Chicago",
                "github": "dep78"
                },
                {
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
    i = 1
    headers = ["SpMax_L", "J_Dz(e)", "nHM", "F01[N-N]", "F04[C-N]", "NssssC", "nCb-", "C%", "nCp", "nO", "F03[C-N]", "SdssC", "HyWi_B(m)", "LOC", " SM6_L", "F03[C-O]", "Me", "Mi", "nN-N", "nArNO2", "nCRX3", "SpPosA_B(p)", "nCIR", "B01[C-Br]", "B03[C-Cl]", "N-073", "SpMax_A", "Psi_i_1d", "B04[C-Br]", "SdO", "TI2_L", "nCrt", "C-026", "F02[C-N]", "nHDon", "SpMax_B(m)", "Psi_i_A", "nN", "SM6_B(m)", " nArCOOR", "nX", "experimental class"]
    with open(os.path.join(input_path, "biodeg.csv"), 'r') as raw_in:
        for row_data in tqdm(parse_tab(raw_in.read(), sep=";", headers=headers), desc="Processing data", disable=not verbose):
            record = []
            for key, value in row_data.items():
                record.append(key+": "+value)
            record_metadata = {
                "mdf-title": "QSAR Biodegradation #" + str(i),
                "mdf-acl": ["public"],

    #            "mdf-tags": ,
    #            "mdf-description": ,
                
    #            "mdf-composition": ,
                "mdf-raw": json.dumps(record),

                "mdf-links": {
    #                "mdf-landing_page": ,

    #                "mdf-publication": ,
    #                "mdf-dataset_doi": ,

    #                "mdf-related_id": ,

                    "csv": {
     
                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",

                        "path": "/collections/qsar_biodeg/biodeg.csv",
                        },
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
            i += 1

            # Pass each individual record to the Validator
            result = dataset_validator.write_record(record_metadata)

            # Check if the Validator accepted the record, and print a message if it didn't
            # If the Validator returns "success" == True, the record was written successfully
            if result["success"] is not True:
                print("Error:", result["message"])


    if verbose:
        print("Finished converting")
