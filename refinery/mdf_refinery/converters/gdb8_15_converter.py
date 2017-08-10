import json
import sys
import os
from tqdm import tqdm
from mdf_refinery.parsers.tab_parser import parse_tab
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the gdb8-15 dataset: Electronic Spectra from TDDFT and Machine Learning in Chemical Space
"""If feedstock path gets changed in an mdf version update,
   the path to the gdb9_14 feedstock will likely need changed"""
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
                "title": "Electronic spectra from TDDFT and machine learning in chemical space",
                "acl": ['public'],
                "source_name": "gdb8_15",
                "citation": ["Electronic spectra of 22k molecules Raghunathan Ramakrishnan, Mia Hartmann, Enrico Tapavicza, O. Anatole von Lilienfeld, J. Chem. Phys. submitted (2015)", "Structures of 22k molecules Raghunathan Ramakrishnan, Pavlo Dral, Matthias Rupp, O. Anatole von Lilienfeld Scientific Data 1, Article number: 140022 (2014). doi:10.1038/sdata.2014.22"],
                "data_contact": {
    
                    "given_name": "O. Anatole",
                    "family_name": "von Lilienfeld",
    
                    "email": "anatole.vonlilienfeld@unibas.ch",
                    "institution": "Argonne National Laboratory",
                    },
    
                "author": [{
                    
                    "given_name": "O. Anatole",
                    "family_name": "von Lilienfeld",
                    
                    "email": "anatole.vonlilienfeld@unibas.ch",
                    "instituition": "Argonne National Laboratory"
                    
                    },
                    {
                        
                    "given_name": "Raghunathan",
                    "family_name": "Ramakrishnan",
                    
                    "institution": "University of Basel"
                    
                    },
                    {
                    
                    "given_name": "Mia",
                    "family_name": "Hartmann",
                    
                    "instituition": "California State University",
                    
                    },
                    {
                    
                    "given_name": "Enrico",
                    "family_name": "Tapavicza",
                    
                    "email": "Enrico.Tapavicza@csulb.edu",
                    "instituition": "California State University",
                    
                    }],
    
    #            "license": "",
    
                "collection": "gdb8_15",
                "tags": ["Density functional theory", "Excitation energies", "Computer modeling", "Oscillators", "Molecular spectra"],
    
                "description": "Due to its favorable computational efficiency, time-dependent (TD) density functional theory (DFT) enables the prediction of electronic spectra in a high-throughput manner across chemical space. Its predictions, however, can be quite inaccurate. We resolve this issue with machine learning models trained on deviations of reference second-order approximate coupled-cluster (CC2) singles and doubles spectra from TDDFT counterparts, or even from DFT gap. We applied this approach to low-lying singlet-singlet vertical electronic spectra of over 20 000 synthetically feasible small organic molecules with up to eight CONF atoms.",
                "year": 2015,
    
                "links": {
    
                    "landing_page": "http://qmml.org/datasets.html#gdb8-15",
    
                    "publication": ["http://dx.doi.org/10.1063/1.4928757http://dx.doi.org/10.1063/1.4928757"],
                    #"data_doi": "",
    
    #                "related_id": ,
    
                    "zip": {
                    
                        #"globus_endpoint": ,
                        "http_host": "http://qmml.org",
    
                        "path": "/Datasets/gdb8-15.zip",
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
    headers = ["Index", "E1-CC2", "E2-CC2", "f1-CC2", "f2-CC2", "E1-PBE0", "E2-PBE0", "f1-PBE0", "f2-PBE0", "E1-PBE0", "E2-PBE0", "f1-PBE0", "f2-PBE0", "E1-CAM", "E2-CAM", "f1-CAM", "f2-CAM"]
    with open(os.path.join(input_path, "gdb8_22k_elec_spec.txt"), 'r') as raw_in:
        data = raw_in.read()
    #Start at line 29 for data
    starter = data.find("       1      0.43295186     0.43295958")
    #Remove the spaces before the index column
    decomp = data[starter:].split("\n")
    stripped_decomp = []
    for line in decomp:
        stripped_decomp.append(line.strip())

    #Open gdb9-14 feedstock to get chemical composition
    with open(os.path.expanduser("~/mdf/feedstock/gdb9_14_all.json"), 'r') as json_file:
        lines = json_file.readlines()
        full_json_data = [json.loads(line) for line in lines]
        #Composition needed doesn't begin until after record 6095
        json_data = full_json_data[6095:]
        
    for record in tqdm(parse_tab("\n".join(stripped_decomp), headers=headers, sep="     "), desc="Processing files", disable=not verbose):
        comp = json_data[int(record["Index"])]["mdf"]["composition"]
        record_metadata = {
            "mdf": {
                "title": "gdb8_15 - " + "record: " + record["Index"],
                "acl": ['public'],
    
    #            "tags": ,
    #            "description": ,
                
                "composition": comp,
                "raw": json.dumps(record),
    
                "links": {
                   # "landing_page": ,
    
    #                "publication": ,
    #                "data_doi": ,
    
    #                "related_id": ,
    
                    "txt": {
                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",
    
                        "path": "/collections/gdb8_15/gdb8_22k_elec_spec.txt",
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
