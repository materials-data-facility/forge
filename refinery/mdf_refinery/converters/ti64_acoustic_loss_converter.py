import json
import sys
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: On the effect of hydrogen on the elastic moduli and acoustic loss behaviour of Ti-6Al-4V
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
                "title": "On the effect of hydrogen on the elastic moduli and acoustic loss behaviour of Ti-6Al-4V",
                "acl": ['public'],
                "source_name": "ti64_acoustic_loss",
                "citation": ["Driver, S. L., Jones, N. G., Stone, H. J., Rugg, D., & Carpenter, M. A. Research Data Supporting \"On the effect of hydrogen on the elastic moduli and acoustic loss behaviour of Ti-6Al-4V\" [Dataset]. https://doi.org/10.17863/CAM.90"],
                "data_contact": {
    
                    "given_name": "Sarah L.",
                    "family_name": "Driver",
                    
                    "email": "sld64@cam.ac.uk",
                    "instituition": "University of Cambridge"
    
                    },
    
                "author": [{
                    
                    "given_name": "Sarah L.",
                    "family_name": "Driver",
                    
                    "email": "sld64@cam.ac.uk",
                    "instituition": "University of Cambridge"
                    
                    },
                    {
                    
                    "given_name": "Nicholas G.",
                    "family_name": "Jones",
                    
                    "instituition": "University of Cambridge"
                    
                    },
                    {
                    
                    "given_name": "Howard J.",
                    "family_name": "Stone",
                    
                    "instituition": "University of Cambridge"
                    
                    },
                    {
                    
                    "given_name": "David",
                    "family_name": "Rugg",
                    
                    "instituition": "Rolls-Royce plc., Derby, UK"
                    
                    },
                    {
                    
                    "given_name": "Michael A.",
                    "family_name": "Carpenter",
                    
                    "instituition": "University of Cambridge"
                    
                    }],
    
                "license": "http://creativecommons.org/licenses/by/4.0/",
    
                "collection": "Ti-6Al-4V Acoustic Loss",
                "tags": ["Titanium alloys", "resonant ultrasound spectroscopy", "microstructure", "mobility", "hydrogen in metals", "internal friction"],
    
                "description": "Resonant Ultrasound Spectroscopy data of a sample of Ti-6Al-4V alloy. Response of the sample due to applied frequency is recorded at set temperatures.",
                "year": 2016,
    
                "links": {
    
                    "landing_page": "https://doi.org/10.17863/CAM.90",
    
                    "publication": ["https://doi.org/10.1080/14786435.2016.1198054"],
                   # "data_doi": "",
    
                    #"related_id": ,
    
                    "txt": {
                    
                        #"globus_endpoint": ,
                        "http_host": "https://www.repository.cam.ac.uk",
    
                        "path": "/bitstream/handle/1810/256150/ti64-rawdata.txt?sequence=1&isAllowed=y",
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

    for data_file in tqdm(find_files(input_path, "txt"), desc="Processing records", disable=not verbose):
        temperature = data_file["filename"].split("<")[1].split(">")[0]
        record_metadata = {
            "mdf": {
                "title": "Ti64 Acoustic Loss at " + temperature + "K",
                "acl": ['public'],
        
        #        "tags": ,
        #        "description": ,
                
                "composition": "Ti-6Al-4V",
        #        "raw": ,
        
                "links": {
        #            "landing_page": ,
        
        #            "publication": ,
        #            "data_doi": ,
        
        #            "related_id": ,
        
                    "txt": {
                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",
        
                        "path": "/collections/ti64_acoustic_loss/" + data_file["filename"],
                        },
                    },
        
        #        "citation": ,
        #        "data_contact": {
        
        #            "given_name": ,
        #            "family_name": ,
        
        #            "email": ,
        #            "institution":,
        
        #            },
        
        #        "author": ,
        
        #        "license": ,
        #        "collection": ,
        #        "year": ,
        
        #        "mrr":
        
        #        "processing": ,
        #        "structure":,
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
