import json
import sys
import os
from ..utils.file_utils import find_files
from ..parsers.ase_parser import parse_ase
from tqdm import tqdm
from ..validator.schema_validator import Validator

# VERSION 0.2.0

# This is the converter for the Dynamic behaviour of the silica-water-bio electrical double layer in the presence of a divalent electrolyte dataset
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
            "mdf-title": "Dynamic behaviour of the silica-water-bio electrical double layer in the presence of a divalent electrolyte",
            "mdf-acl": ['public'],
            "mdf-source_name": "silica_water_edl",
            "mdf-citation": ["Lowe, B.M., Maekawa, Y., Shibuta, Y., Sakata, T., Skylaris, C.-K. and Green, N.G. (2016) Dynamic Behaviour of the Silica-Water-Bio Electrical Double Layer in the Presence of a Divalent Electrolyte. Physical Chemistry Chemical Physics, https://doi.org/10.1039/C6CP04101A"],
            "mdf-data_contact": {

                "given_name": "Benjamin",
                "family_name": "Lowe",
                
                "email": "ben.lowe.uk@googlemail.com",
                "instituition": "University of Southampton"

                },

            "mdf-author": [{
                
                "given_name": "Benjamin",
                "family_name": "Lowe",
                
                "email": "ben.lowe.uk@googlemail.com",
                "instituition": "University of Southampton"
                
                },
                {
                    
                "given_name": "Toshiya",
                "family_name": "Sakata",
                
                "email": "sakata@biofet.t.u-tokyo.ac.jp",
                "institution": "The University of Tokyo"
                
                },
                {
                
                "given_name": "Nicolas",
                "family_name": "Green",
                
                "email": "ng2@ecs.soton.ac.uk",
                "instituition": "University of Southampton",
                
                },
                {
                
                "given_name": "Yuki",
                "family_name": "Maekawa",
                
                "instituition": "The University of Tokyo",
                
                },
                {
                
                "given_name": "Yasushi",
                "family_name": "Shibuta",
                
                "instituition": "The University of Tokyo",
                
                },
                {
                
                "given_name": "Chris",
                "family_name": "Skylaris",
                
                "instituition": "University of Southampton",
                
                }],

            "mdf-license": "http://creativecommons.org/licenses/by/4.0/",

            "mdf-collection": "Silica Water EDL",
            "mdf-data_format": ["xyz"],
            "mdf-data_type": ["MD"],
            "mdf-tags": ["BioFET", "BioFETs", "BioFED", "molecular dynamics", "MD"],

            "mdf-description": "In this paper, explicit-solvent atomistic calculations of this electric field are presented and the structure and dynamics of the interface are investigated in different ionic strengths using molecular dynamics simulations. Novel results from simulation of the addition of DNA molecules and divalent ions are also presented, the latter of particular importance in both physiological solutions and biosensing experiments",
            "mdf-year": 2016,

            "mdf-links": {

                "mdf-landing_page": "https://eprints.soton.ac.uk/401018/",

                "mdf-publication": ["http://eprints.soton.ac.uk/401017", "http://dx.doi.org/10.1039/C6CP04101A"],
                "mdf-dataset_doi": "https://eprints.soton.ac.uk/401018/",

#                "mdf-related_id": ,

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
    for data_file in tqdm(find_files(input_path, "record-0.xyz"), desc="Processing files", disable=not verbose):
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "xyz")
        uri = "https://data.materialsdatafacility.org/collections/" + "silica_water_edl/" + data_file["no_root_path"] + "/" + data_file["filename"]
        record_metadata = {
            "mdf-title": "Silica Water EDL - " + record["chemical_formula"],
            "mdf-acl": ['public'],

#            "mdf-tags": ,
#            "mdf-description": ,
            
            "mdf-composition": record["chemical_formula"],
#            "mdf-raw": ,

            "mdf-links": {
                "mdf-landing_page": uri,

#                "mdf-publication": ,
#                "mdf-dataset_doi": ,

#                "mdf-related_id": ,

                "data_links": {
                    "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                    #"http_host": ,

                    "path": "/collections/silica_water_edl/" + data_file["no_root_path"] + "/" + data_file["filename"],
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
            "number_of_frames": 3000
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
