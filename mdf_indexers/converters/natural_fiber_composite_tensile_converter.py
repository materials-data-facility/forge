import json
import sys
import os
from ..utils.file_utils import find_files
from tqdm import tqdm
from ..validator.schema_validator import Validator

# VERSION 0.2.0

# This is the converter for Statistical behavior of the tensile properties of natural fibre composites
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
            "mdf-title": "Statistical behavior of the tensile properties of natural fibre composites",
            "mdf-acl": ['public'],
            "mdf-source_name": "natural_fiber_composite_tensile",
            "mdf-citation": ["Torres, Juan Pablo (2017), “Statistical behavior of the tensile properties of natural fibre composites”, Mendeley Data, v1 http://dx.doi.org/10.17632/v25pzywt5c.1"],
            "mdf-data_contact": {

                "given_name": "Juan Pablo",
                "family_name": "Torres",
                
                "email": "juan.torres@uq.edu.au",
                "instituition": "University of Queensland"

                },

            "mdf-author": [{
                
                "given_name": "Juan Pablo",
                "family_name": "Torres",
                
                "email": "juan.torres@uq.edu.au",
                "instituition": "University of Queensland"
                
                }],

            "mdf-license": "http://creativecommons.org/licenses/by/4.0",

            "mdf-collection": "Natural Fiber Tensile Properties",
            "mdf-data_format": ["csv"],
 #           "mdf-data_type": ,
            "mdf-tags": ["Laminates", "Mechanical Property", "Natural Fiber Composite"],

            "mdf-description": "This article features raw stress–strain tensile data for approximately 500 specimens corresponding to different natural fibre reinforced composite laminates. In addition, we provide here the calculated elastic modulus, strength and failure strain values for each specimen.",
            "mdf-year": 2017,

            "mdf-links": {

                "mdf-landing_page": "http://dx.doi.org/10.17632/v25pzywt5c.1",

                "mdf-publication": ["http://www.sciencedirect.com/science/article/pii/S1359835X17301136", "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5397101/"],
                "mdf-dataset_doi": "https://data.mendeley.com/datasets/v25pzywt5c/1/files/4d9a86e4-f523-4e27-b308-ca9d519da028",

                "mdf-related_id": "orcid.org/0000-0002-0301-4374",

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
    for data_file in tqdm(find_files(input_path, "csv"), desc="Processing files", disable=not verbose):
        with open(os.path.join(input_path, data_file["no_root_path"], data_file["filename"])) as raw_in:
            record = raw_in.readlines()
        #Get Composition from filename
        comp = data_file["filename"].split("_")[0]
        uri = "https://data.materialsdatafacility.org/collections/natural_fiber_composite_tensile/" + data_file["no_root_path"] + '/' + data_file["filename"]
        record_metadata = {
            "mdf-title": "Natural Fiber Tensile Properties - " + comp,
            "mdf-acl": ['public'],

#            "mdf-tags": ,
#            "mdf-description": ,
            
            "mdf-composition": comp,
#            "mdf-raw": ,

            "mdf-links": {
#                "mdf-landing_page": uri,

#                "mdf-publication": ,
#                "mdf-dataset_doi": ,

#                "mdf-related_id": ,

                "csv": {
                    "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                    "http_host": "https://data.materialsdatafacility.org",

                    "path": "/collections/natural_fiber_composite_tensile/" + data_file["no_root_path"] + "/" + data_file["filename"],
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
            "Modulus (Segment 0.001 mm/mm - 0.008 mm/mm) (MPa)": record[0].split(',')[1] + record[0].split(',')[2], #Combine 1 and 2 because Modulus is in the thousands
            "Poisson's ratio (Least squares fit)": record[1].split(',')[1],
            "Gauge length (mm)": record[2].split(',')[1],
            "Gauge length transverse (mm)": record[3].split(',')[1],
            "Thickness (mm)": record[4].split(',')[1],
            "Width (mm)": record[5].split(',')[1]
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
