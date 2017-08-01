import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: Pore Shape Modification of a Microporous Metal–Organic Framework Using High Pressure: Accessing a New Phase with Oversized Guest Molecules
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
            "mdf": {
                "title": "Pore Shape Modification of a Microporous Metal–Organic Framework Using High Pressure: Accessing a New Phase with Oversized Guest Molecules",
                "acl": ['public'],
                "source_name": "porous_mof",
                "citation": ["The University of Edinburgh School of Chemistry. (2016). Pore Shape Modification of a Microporous Metal-Organic Frame-work Using High Pressure: Accessing a New Phase with Oversized Guest Molecules, [dataset]. http://dx.doi.org/10.7488/ds/371."],
                "data_contact": {
    
                    "given_name": "Stephen A.",
                    "family_name": "Moggach",
                    
                    "email": "s.moggach@ed.ac.uk",
                    "instituition": "University of Edinburgh"
    
                    },
    
                "author": [{
                    
                    "given_name": "Stephen A.",
                    "family_name": "Moggach",
                    
                    "email": "s.moggach@ed.ac.uk",
                    "instituition": "University of Edinburgh"
                    
                    },
                    {
                    
                    "given_name": "Scott C.",
                    "family_name": "McKellar",
                    
                    "instituition": "University of Edinburgh"
                    
                    },
                    {
                    
                    "given_name": "Jorge",
                    "family_name": "Sotelo",
                    
                    "instituition": "University of Edinburgh"
                    
                    },
                    {
                    
                    "given_name": "Alex",
                    "family_name": "Greenaway",
                    
                    "instituition": "University of St Andrews"
                    
                    },
                    {
                    
                    "given_name": "John P. S.",
                    "family_name": "Mowat",
                    
                    "instituition": "University of St Andrews"
                    
                    },
                    {
                    
                    "given_name": "Odin",
                    "family_name": "Kvam",
                    
                    "instituition": "University of Edinburgh"
                    
                    },
                    {
                    
                    "given_name": "Carole A.",
                    "family_name": "Morrison",
                    
                    "instituition": "University of Edinburgh"
                    
                    },
                    {
                    
                    "given_name": "Paul A.",
                    "family_name": "Wright",
                    
                    "instituition": "University of St Andrews"
                    
                    }],
    
                "license": "http://creativecommons.org/licenses/by/4.0/legalcode",
    
                "collection": "Porous Metal-Organic-Framework",
                #"tags": ,
    
                "description": "Pressures up to 0.8 GPa have been used to squeeze a range of sterically “oversized” C5–C8 alkane guest molecules into the cavities of a small-pore Sc-based metal–organic framework. Guest inclusion causes a pronounced reorientation of the aromatic rings of one-third of the terephthalate linkers, which act as “torsion springs”, resulting in a fully reversible change in the local pore structure. The study demonstrates how pressure-induced guest uptake can be used to investigate framework flexibility relevant to “breathing” behavior and to understand the uptake of guest molecules in MOFs relevant to hydrocarbon separation.",
                "year": 2015,
    
                "links": {
    
                    "landing_page": "http://datashare.is.ed.ac.uk/handle/10283/942",
    
                    "publication": ["http://dx.doi.org/10.1021/acs.chemmater.5b02891"],
                    "data_doi": "http://dx.doi.org//10.7488/ds/371",
    
                    #"related_id": ,
    
                    "zip": {
                    
                        #"globus_endpoint": ,
                        "http_host": "http://datashare.is.ed.ac.uk",
    
                        "path": "/download/10283/942/Pore_Shape_Modification_of_a_Microporous_Metal-Organic_Frame-work_Using_High_Pressure:_Accessing_a_New_Phase_with_Oversized_Guest_Molecules.zip",
                        }
                    },
    
    #            "mrr": ,
    
                "data_contributor": [{
                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78"
                    }]
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
        record_metadata = {
            "mdf": {
                "title": "Metal-Organic-Frame-Work - " + record["chemical_formula"],
                "acl": ['public'],
    
    #            "tags": ,
    #            "description": ,
                
                "composition": record["chemical_formula"],
    #            "raw": ,
    
                "links": {
    #                "landing_page": ,
    
    #                "publication": ,
    #                "data_doi": ,
    
    #                "related_id": ,
    
                    "cif": {
                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",
    
                        "path": "/collections/porous_mof/" + data_file["no_root_path"] + "/" + data_file["filename"],
                        },
                    },
    
    #            "citation": ,
    #            "data_contact": {
    
    #                "given_name": ,
    #                "family_name": ,
    
    #                "email": ,
    #                "institution":,
    
    #                },
    
    #            "author": ,
    
    #            "license": ,
    #            "collection": ,
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

    # You're done!
    if verbose:
        print("Finished converting")
