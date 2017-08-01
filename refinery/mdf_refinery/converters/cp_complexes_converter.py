import json
import sys
import os

from tqdm import tqdm

from mdf_refinery.validator import Validator
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase

# VERSION 0.3.0

# This is the converter for the Cp Complexes dataset.
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
            "title": "Synthesis, Characterization, and Some Properties of Cp*W(NO)(H)(η3‑allyl) Complexes",
            "acl": ["public"],
            "source_name": "cp_complexes",
            "citation": ["Baillie, Rhett A.; Holmes, Aaron S.; Lefèvre, Guillaume P.; Patrick, Brian O.; Shree, Monica V.; Wakeham, Russell J.; Legzdins, Peter; Rosenfeld, Devon C. (2015): Synthesis, Characterization, and Some Properties of Cp*W(NO)(H)(η3‑allyl) Complexes. ACS Publications. https://doi.org/10.1021/acs.inorgchem.5b00747.s002"],
            "data_contact": {

                "given_name": "Rhett",
                "family_name": "Baillie",

                "email": "rbaillie@berkeley.edu",
                "institution": "University of California, Berkeley"
                },

            "author": [{

                "given_name": "Rhett",
                "family_name": "Baillie",

                "email": "rbaillie@berkeley.edu",
                "institution": "University of California, Berkeley"
                },
                {

                "given_name": "Aaron",
                "family_name": "Holmes",

                "institution": "University of British Columbia - Vancouver"
                },
                {

                "given_name": "Guillaume",
                "family_name": "Lefèvre",

                "institution": "Atomic Energy and Alternative Energies Commission"
                },
                {

                "given_name": "Brian",
                "family_name": "Patrick",

                },
                {

                "given_name": "Monica",
                "family_name": "Shree",

                },
                {

                "given_name": "Russell",
                "family_name": "Wakeham",

                "institution": "Swansea University"
                },
                {

                "given_name": "Peter",
                "family_name": "Legzdins",

                },
                {

                "given_name": "Devon",
                "family_name": "Rosenfeld",

                }],

            "license": "https://creativecommons.org/licenses/by-nc/4.0/",

            "collection": "Cp*W(NO)(H)(η3‑allyl) Complexes",
            "tags": ["THF", "DFT", "18 e PMe 3 adducts", "complex", "coordination isomers", "magnesium allyl reagent"],

            "description": "Sequential treatment at low temperatures of Cp*W­(NO)­Cl2 in THF with 1 equiv of a binary magnesium allyl reagent, followed by an excess of LiBH4, affords three new Cp*W­(NO)­(H)­(η3-allyl) complexes, namely, Cp*W­(NO)­(H)­(η3-CH2CHCMe2) (1), Cp*W­(NO)­(H)­(η3-CH2CHCHPh) (2), and Cp*W­(NO)­(H)­(η3-CH2CHCHMe) (3).",
            "year": 2015,

            "links": {

                "landing_page": "https://figshare.com/articles/Synthesis_Characterization_and_Some_Properties_of_Cp_W_NO_H_sup_3_sup_allyl_Complexes/2158483",

                "publication": "https://doi.org/10.1021/acs.inorgchem.5b00747",
#                "data_doi": "https://doi.org/10.1021/acs.inorgchem.5b00747.s002", # Bad link

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
    for file_data in tqdm(find_files(input_path, ".cif"), desc="Processing data", disable= not verbose):
        record = parse_ase(os.path.join(file_data["path"], file_data["filename"]), data_format="cif")
        record_metadata = {
        "mdf": {
            "title": "Cp Complexes - " + record["chemical_formula"],
            "acl": ["public"],

#            "tags": ,
#            "description": ,
            
            "composition": record["chemical_formula"],
#            "raw": ,

            "links": {
#                "landing_page": ,

#                "publication": ,
#                "dataset_doi": ,

#                "related_id": ,

                # data links: {
 
                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #},
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
