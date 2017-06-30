import json
import sys
import os
from ..utils.file_utils import find_files
from ..parsers.ase_parser import parse_ase
from tqdm import tqdm
from ..validator.schema_validator import Validator

# VERSION 0.2.0

# This is the converter for: Unraveling the Planar-Globular Transition in Gold Nanoclusters through Evolutionary Search
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
    if not metadata:
        dataset_metadata = {
            "mdf-title": "Unraveling the Planar-Globular Transition in Gold Nanoclusters through Evolutionary Search",
            "mdf-acl": ['public'],
            "mdf-source_name": "au_nanoclusters_dft",
            "mdf-citation": ["Kinaci, Alper, Badri Narayanan, Fatih G. Sen, Michael J. Davis, Stephen K. Gray, Subramanian K. R. S. Sankaranarayanan, and Maria K. Y. Chan. \"Unraveling the Planar-Globular Transition in Gold Nanoclusters through Evolutionary Search.\" Nature News. Nature Publishing Group, 28 Nov. 2016. Web. 30 June 2017."],
            "mdf-data_contact": {

                "given_name": "Maria K. Y.",
                "family_name": "Chan",
                
                "email": "mchan@anl.gov",
                "instituition": "Argonne National Laboratory"

                },

            "mdf-author": [{
                
                "given_name": "Alper",
                "family_name": "Kinaci",
                
                "instituition": "Argonne National Laboratory"
                
                },
                {
                
                "given_name": "Badri",
                "family_name": "Narayanan",
                
                "instituition": "Argonne National Laboratory"
                
                },
                {
                
                "given_name": "Fatih G.",
                "family_name": "Sen",
                
                "instituition": "Argonne National Laboratory"
                
                },
                {
                
                "given_name": "Stephen K.",
                "family_name": "Gray",
                
                "instituition": "Argonne National Laboratory"
                
                },
                {
                
                "given_name": "Subramanian K. R. S.",
                "family_name": "Sankaranarayanan",
                
                "instituition": "Argonne National Laboratory"
                
                },
                {
                
                "given_name": "Maria K. Y.",
                "family_name": "Chan",
                
                "email": "mchan@anl.gov",
                "instituition": "Argonne National Laboratory"
                
                },
                {
                
                "given_name": "Michael J.",
                "family_name": "Davis",
                
                "instituition": "Argonne National Laboratory"
                
                }],

            "mdf-license": "http://creativecommons.org/licenses/by/4.0/",

            "mdf-collection": "Au Nanoclusters DFT",
            "mdf-data_format": ["xyz"],
            "mdf-data_type": ["DFT"],
            "mdf-tags": ["Atomistic models", "Evolution", "Nanoparticles", "Structure prediction"],

            "mdf-description": "Au nanoclusters are of technological relevance for catalysis, photonics, sensors, and of fundamental scientific interest owing to planar to globular structural transformation at an anomalously high number of atoms i.e. in the range 12–14. The nature and causes of this transition remain a mystery. In order to unravel this conundrum, high throughput density functional theory (DFT) calculations, coupled with a global structural optimization scheme based on a modified genetic algorithm (GA) are conducted.",
            "mdf-year": 2016,

            "mdf-links": {

                "mdf-landing_page": "https://www.nature.com/articles/srep34974#abstract",

               # "mdf-publication": ,
                #"mdf-dataset_doi": ,

               # "mdf-related_id": ,

                # data links: {
                
                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #}
                },

#            "mdf-mrr": ,

            "mdf-data_contributor": [{
                "given_name": "Evan",
                "family_name": "Pike",
                "email": "dep78@uchicago.edu",
                "institution": "The University of Chicago",
                "github": "dep78"
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
    for data_file in tqdm(find_files(input_path, "xyz"), desc="Processing files", disable=not verbose):
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "xyz")
        record_metadata = {
            "mdf-title": "Au Nanoclusters DFT - " + record["chemical_formula"],
            "mdf-acl": ['public'],

#            "mdf-tags": ,
#            "mdf-description": ,
            
            "mdf-composition": record["chemical_formula"],
#            "mdf-raw": ,

            "mdf-links": {
#                "mdf-landing_page": ,

#                "mdf-publication": ,
#                "mdf-dataset_doi": ,

#                "mdf-related_id": ,

                "xyz": {
                    "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                    "http_host": "https://data.materialsdatafacility.org",

                    "path": "/collections/au_nanoclusters_dft/" + data_file["filename"],
                    },
                },

#            "mdf-citation": ,
#            "mdf-data_contact": {

#                "given_name": ,
#                "family_name": ,

#                "email": ,
#                "institution":,

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

        # Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)

        # Check if the Validator accepted the record, and print a message if it didn't
        # If the Validator returns "success" == True, the record was written successfully
        if result["success"] is not True:
            print("Error:", result["message"])

    # You're done!
    if verbose:
        print("Finished converting")
