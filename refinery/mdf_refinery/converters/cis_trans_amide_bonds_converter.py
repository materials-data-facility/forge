import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: Modulation of Cis-Trans Amide Bond Rotamers in 5-Acyl-6,7-dihydrothieno[3,2-c]pyridines
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
                "title": "Modulation of Cis-Trans Amide Bond Rotamers in 5-Acyl-6,7-dihydrothieno[3,2-c]pyridines",
                "acl": ['public'],
                "source_name": "cis_trans_amide_bonds",
                "citation": ["Rzepa, HS, T. Lanyon-Hogg, M. Ritzefeld, N. Masumoto, A. I. Magee, and E. W. Tate. \"Modulation of Cis-Trans Amide Bond Rotamers in 5-Acyl-6,7-dihydrothieno[3,2-c]pyridines.\" Spiral: Modulation of Cis-Trans Amide Bond Rotamers in 5-Acyl-6,7-dihydrothieno[3,2-c]pyridines. N.p., 25 Sept. 2014. Web. 07 July 2017."],
                "data_contact": {
    
                    "given_name": "Henry S.",
                    "family_name": "Rzepa",
                    
                    "email": "rzepa@imperial.ac.uk",
                    "instituition": "Imperial College London"
    
                    },
    
                "author": [{
                    
                    "given_name": "Henry S.",
                    "family_name": "Rzepa",
                    
                    "email": "rzepa@imperial.ac.uk",
                    "instituition": "Imperial College London"
                    
                    },
                    {
                    
                    "given_name": "Thomas",
                    "family_name": "Lanyon-Hogg",
                    
                    },
                    {
                    
                    "given_name": "Markus",
                    "family_name": "Ritzefeld",
                    
                    },
                    {
                    
                    "given_name": "Naoko",
                    "family_name": "Masumoto",
                    
                    },
                    {
                    
                    "given_name": "Anthony I.",
                    "family_name": "Magee",
                    
                    },
                    {
                    
                    "given_name": "Edward W.",
                    "family_name": "Tate",
                    
                    }],
    
                "license": "https://creativecommons.org/licenses/by/4.0/",
    
                "collection": "Cis-Trans Amide Bonds",
                "tags": ["conformational", "conformational analysis", "NMR Spectroscopy", "Biochemistry", "Organic Chemistry"],
    
                "description": "2-Substituted N-acyl-piperidine is a widespread and important structural motif, found in nearly 500 currently available structures, and present in at least 30 pharmaceutically active compounds. Restricted rotation of the acyl substituent in such molecules can give rise to two distinct chemical environments. Here we demonstrate using NMR studies and modelling of the lowest energy structures of 5-acyl-6,7-dihydrothieno[3,2-c]pyridine derivatives that the amide cis-trans equilibrium is affected by intramolecular hydrogen bonding between the amide oxygen and adjacent aromatic protons.",
                "year": 2014,
    
                "links": {
    
                    "landing_page": "http://hdl.handle.net/10044/1/30292",
    
                    "publication": ["http://pubs.acs.org/doi/full/10.1021/acs.joc.5b00205"],
                    "data_doi": "http://dx.doi.org/10.6084/m9.figshare.1181739",
    
                    #"related_id": ,
    
                    "zip": {
                    
                        #"globus_endpoint": ,
                        "http_host": "https://ndownloader.figshare.com",
    
                        "path": "/files/6055506",
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
    for data_file in tqdm(find_files(input_path, "(xyz|log)$"), desc="Processing files", disable=not verbose):
        dtype = data_file["filename"].rsplit(".", 1)[1]
        if dtype == "log":
            ftype = "gaussian-out"
        else:
            ftype = dtype
        path = ""
        if data_file["no_root_path"]:
            path = data_file["no_root_path"] + "/"
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), ftype)
        record_metadata = {
            "mdf": {
                "title": "Cis-Trans Amide Bonds - " + record["chemical_formula"],
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
    
                    dtype: {
                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",
    
                        "path": "/collections/cis_trans_amide_bonds/" + path + data_file["filename"],
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
