import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for the MD-17 dataset: Quantum-chemical insights from deep tensor neural networks
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
                "title": "Quantum-chemical insights from deep tensor neural networks",
                "acl": ['public'],
                "source_name": "md_17",
                "citation": ["S. Chmiela, A. Tkatchenko, H. E. Sauceda, I. Poltavsky, K.Schütt, K.-R. Müller, arXiv:1611.04678 (2017)"],
                "data_contact": {
    
                    "given_name": "Alexandre",
                    "family_name": "Tkatchenko",
                    
                    "email": "tkatchen@fhi-berlin.mpg.de",
                    "instituition": "Fritz-Haber-Institut der Max-Planck-Gesellschaft, University of Luxembourg",
    
                    },
    
                "author": [{
                    
                    "given_name": "Kristof T.",
                    "family_name": "Schütt",
                    
                    "instituition": "Technische Universität Berlin"
                    
                    },
                    {
                        
                    "given_name": "Klaus R.",
                    "family_name": "Müller",
                    
                    "email": "klaus-robert.mueller@tu-berlin.de",
                    "institution": "Korea University"
                    
                    },
                    {
                    
                    "given_name": "Alexandre",
                    "family_name": "Tkatchenko",
                    
                    "email": "tkatchen@fhi-berlin.mpg.de",
                    "instituition": "Fritz-Haber-Institut der Max-Planck-Gesellschaft, University of Luxembourg",
                    
                    },
                    {
                    
                    "given_name": "Farhad",
                    "family_name": "Arbabzadah",
                    
                    "instituition": "Technische Universität Berlin",
                    
                    },
                    {
                    
                    "given_name": "Stefan",
                    "family_name": "Chmiela",
                    
                    "instituition": "Technische Universität Berlin",
                    
                    }],
    
                "license": "http://creativecommons.org/licenses/by/4.0/",
    
                "collection": "MD_17",
                "tags": ["Applied mathematics", "Computational chemistry", "Physical chemistry", "Scientific data"],
    
                "description": "Energies and forces from molecular dynamics trajectories of eight organic molecules. Ab initio molecular dynamics trajectories (133k to 993k frames) of benzene, uracil, naphthalene, aspirin, salicylic acid, malonaldehyde, ethanol, toluene at the DFT/PBE+vdW-TS level of theory at 500 K.",
                "year": 2017,
    
                "links": {
    
                    "landing_page": "http://qmml.org/datasets.html#md-17",
    
                    "publication": ["http://dx.doi.org/10.1038/ncomms13890", "https://arxiv.org/abs/1611.04678"],
                   # "data_doi": "",
    
    #                "related_id": ,
    
                    "tar": {
                    
                        #"globus_endpoint": ,
                        "http_host": "http://quantum-machine.org",
    
                        "path": "/data/md_datasets.tar",
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

    all_frames = {
        "aspirin": 111763,
        "benzene": 527984,
        "ethanol": 455093,
        "malonaldehyde": 893238,
        "naphthalene": 226256,
        "salicylic_acid": 220232,
        "toluene": 342791,
        "uracil": 133770
    }
    # Get the data
    #    Each record should be exactly one dictionary
    #    You must write your records using the Validator one at a time
    #    It is recommended that you use a parser to help with this process if one is available for your datatype
    #    Each record also needs its own metadata
    for data_file in tqdm(find_files(input_path, "000001.xyz"), desc="Processing files", disable=not verbose):
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "xyz")
        file_name = data_file["no_root_path"]
        num_frames = all_frames[file_name]
        record_metadata = {
            "mdf": {
                "title": "MD_17 - " + record["chemical_formula"],
                "acl": ['public'],
    
    #            "tags": ,
    #            "description": ,
                
                "composition": record["chemical_formula"],
    #            "raw": ,
    
                "links": {
                    #"landing_page": ,
    
    #                "publication": ,
    #                "data_doi": ,
    
    #                "related_id": ,
    
                    "xyz": {
                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",
    
                        "path": "/collections/md_17/" + data_file["no_root_path"] + '/' + data_file["filename"],
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
                },
            "md_17": {
                "num_frames": num_frames
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
