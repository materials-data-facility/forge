import json
import sys
import os
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from tqdm import tqdm
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter the Crystallographic information of intermediate phases in binary Mg–X (X=Sn, Y, Sc, Ag) alloys dataset
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
                "title": "Crystallographic information of intermediate phases in binary Mg–X (X=Sn, Y, Sc, Ag) alloys",
                "acl": ['public'],
                "source_name": "mg_x_alloys_crystallography",
                "citation": ["Liu, Dongyan et al. “Crystallographic Information of Intermediate Phases in Binary Mg–X (X=Sn, Y, Sc, Ag) Alloys.” Data in Brief 4 (2015): 190–192. PMC. Web. 28 June 2017."],
                "data_contact": {
    
                    "given_name": "Xiangying",
                    "family_name": "Meng",
                    
                    "email": "x_y_meng@mail.neu.edu.cn",
                    "instituition": "Northeastern University, Shenyang, China"
    
                    },
    
                "author": [{
                    
                    "given_name": "Dongyan",
                    "family_name": "Liu",
                    
                    "instituition": "Northeastern University, Shenyang, China"
                    
                    },
                    {
                    
                    "given_name": "Xuefeng",
                    "family_name": "Dai",
                    
                    "instituition": "Northeastern University, Shenyang, China"
                    
                    },
                    {
                    
                    "given_name": "Xiaohong",
                    "family_name": "Wen",
                    
                    "instituition": "Northeastern University, Shenyang, China"
                    
                    },
                    {
                    
                    "given_name": "Gaowu",
                    "family_name": "Qin",
                    
                    "instituition": "Northeastern University, Shenyang, China"
                    
                    },
                    {
                    
                    "given_name": "Xiangying",
                    "family_name": "Meng",
                    
                    "email": "x_y_meng@mail.neu.edu.cn",
                    "instituition": "Northeastern University, Shenyang, China"
                    
                    }],
    
                "license": "http://creativecommons.org/licenses/by/4.0/",
    
                "collection": "Mg-X Alloy Crystallography",
                "tags": ["DFT", "ab-initio", "crystallography"],
    
                "description": "The compositions and structures of thermodynamically stable or metastable precipitations in binary Mg-X (X=Sn, Y, Sc, Ag) alloys are predicted using ab-initio evolutionary algorithm. The geometry optimizations of the predicted intermetallic compounds are carried out in the framework of density functional theory (DFT). A complete list of the optimized crystallographic information (in cif format) of the predicted intermetallic phases is presented here.",
                "year": 2015,
    
                "links": {
    
                    "landing_page": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4510453/",
    
                    "publication": ["http://www.sciencedirect.com/science/article/pii/S2352340915000797"],
                    #"data_doi": "",
    
           #         "related_id": ,
    
                    "zip": {
                    
                        #"globus_endpoint": ,
                        "http_host": "https://www.ncbi.nlm.nih.gov",
    
                        "path": "/pmc/articles/PMC4510453/bin/mmc1.zip",
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
                "title": "Mg-X Alloy Crystallography - " + record["chemical_formula"],
                "acl": ['public'],
    
    #            "tags": ,
    #            "description": ,
                
                "composition": record["chemical_formula"],
               # "raw": json.dumps(record),
    
                "links": {
    #                "landing_page": ,
    
    #                "publication": ,
    #                "data_doi": ,
    
    #                "related_id": ,
    
                    "cif": {
                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",
    
                        "path": "/collections/mg_x_alloys_crystallography/" + data_file["filename"],
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
