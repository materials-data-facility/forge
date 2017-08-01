import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.tab_parser import parse_tab
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: Data set for diffusion coefficients of alloying elements in dilute Mg alloys from first-principles
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
                "title": "Data set for diffusion coefficients of alloying elements in dilute Mg alloys from first-principles",
                "acl": ['public'],
                "source_name": "dilute_mg_alloys_dft",
                "citation": ["Zhou, Bi-Cheng et al. “Data Set for Diffusion Coefficients of Alloying Elements in Dilute Mg Alloys from First-Principles.” Data in Brief 5 (2015): 900–912. PMC. Web. 7 July 2017."],
                "data_contact": {
    
                    "given_name": "Bi-Cheng",
                    "family_name": "Zhou",
                    
                    "email": "bicheng.zhou@psu.edu",
                    "instituition": "The Pennsylvania State University"
    
                    },
    
                "author": [{
                    
                    "given_name": "Bi-Cheng",
                    "family_name": "Zhou",
                    
                    "email": "bicheng.zhou@psu.edu",
                    "instituition": "The Pennsylvania State University"
                    
                    },
                    {
                    
                    "given_name": "Shun-Li",
                    "family_name": "Shang",
                    
                    "instituition": "The Pennsylvania State University"
                    
                    },
                    {
                    
                    "given_name": "Yi",
                    "family_name": "Wang",
                    
                    "instituition": "The Pennsylvania State University"
                    
                    },
                    {
                    
                    "given_name": "Zi-Kui",
                    "family_name": "Liu",
                    
                    "instituition": "The Pennsylvania State University"
                    
                    }],
    
                "license": "http://creativecommons.org/licenses/by/4.0/",
    
                "collection": "Dilute Mg Alloys DFT",
                #"tags": ,
    
                "description": "Diffusion coefficients of alloying elements in Mg are critical for the development of new Mg alloys for lightweight applications. Here we present the data set of the temperature-dependent dilute tracer diffusion coefficients for 47 substitutional alloying elements in hexagonal closed packed (hcp) Mg calculated from first-principles calculations based on density functional theory (DFT) by combining transition state theory and an 8-frequency model.",
                "year": 2015,
    
                "links": {
    
                    "landing_page": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4669471/",
    
                    "publication": ["http://dx.doi.org/10.1016/j.dib.2015.10.024"],
                   # "data_doi": ,
    
                  #  "related_id": ,
    
                    "xslx": {
                    
                        #"globus_endpoint": ,
                        "http_host": "https://www.ncbi.nlm.nih.gov",
    
                        "path": "/pmc/articles/PMC4669471/bin/mmc1.xlsx",
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
    for data_file in tqdm(find_files(input_path, "^[^\.]"), desc="Processing files", disable=not verbose):
        with open(os.path.join(data_file["path"], data_file["filename"]), 'r', encoding="utf-8") as raw_in:
            all_data = raw_in.read().strip("\ufeff")
        if data_file["filename"] == "csv_info.txt":
            continue
        if "txt" in data_file["filename"]:
            sep="\t"
            file_type = "txt"
            path = data_file["no_root_path"] + "/"
        else:
            sep=","
            file_type = "csv"
            path = ""
        for record in parse_tab(all_data, sep):
            if "diffusion" in data_file["filename"]:
                comp = data_file["filename"].split("_")[0]
            else:
                comp = record["X"]
            record_metadata = {
                "mdf": {
                    "title": "Diliute Mg Alloys DFT - " + comp,
                    "acl": ['public'],
        
        #            "tags": ,
        #            "description": ,
                    
                    "composition": comp,
                    "raw": json.dumps(record),
        
                    "links": {
        #                "landing_page": ,
        
        #                "publication": ,
        #                "data_doi": ,
        
        #                "related_id": ,
        
                        file_type: {
                            "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                            "http_host": "https://data.materialsdatafacility.org",
        
                            "path": "/collections/dilute_mg_alloys_dft/" + path + data_file["filename"],
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
