import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the GDB7_12(QM7) dataset: 7k small organic molecules, close to their ground states, with DFT atomization energies
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
                "title": "Fast and Accurate Modeling of Molecular Atomization Energies with Machine Learning",
                "acl": ['public'],
                "source_name": "gdb7_12",
                "citation": ["Matthias Rupp, Alexandre Tkatchenko, Klaus-Robert Müller, O. Anatole von Lilienfeld: Fast and Accurate Modeling of Molecular Atomization Energies with Machine Learning, Physical Review Letters 108(5): 058301, 2012. DOI: 10.1103/PhysRevLett.108.058301", "Gr\'egoire Montavon, Katja Hansen, Siamac Fazli, Matthias Rupp, Franziska Biegler, Andreas Ziehe, Alexandre Tkatchenko, O. Anatole von Lilienfeld, Klaus-Robert M\"uller: Learning Invariant Representations of Molecules for Atomization Energy Prediction, Advances in Neural Information Processing Systems 25 (NIPS 2012), Lake Tahoe, Nevada, USA, December 3-6, 2012."],
                "data_contact": {
    
                    "given_name": "O. Anatole",
                    "family_name": "von Lilienfeld",
    
                    "email": "anatole@alcf.anl.gov",
                    "institution": "Argonne National Laboratory",
                    },
    
                "author": [{
                    
                    "given_name": "O. Anatole",
                    "family_name": "von Lilienfeld",
                    
                    "email": "anatole@alcf.anl.gov",
                    "instituition": "Argonne National Laboratory"
                    
                    },
                    {
                        
                    "given_name": "Alexandre",
                    "family_name": "Tkatchenko",
                    
                    "institution": "University of California Los Angeles, Fritz-Haber-Institut der Max-Planck-Gesellschaft"
                    
                    },
                    {
                    
                    "given_name": "Matthias",
                    "family_name": "Rupp",
                    
                    "instituition": "Technical University of Berlin, University of California Los Angeles",
                    
                    },
                    {
                    
                    "given_name": "Klaus-Robert",
                    "family_name": "Müller",
                    
                    "email": "klaus-robert.mueller@tu-berlin.de",
                    "instituition": "Technical University of Berlin, University of California Los Angeles",
                    
                    }],
    
    #            "license": ,
    
                "collection": "gdb7_12",
    #            "tags": ,
    
                "description": "7k small organic molecules, close to their ground states, with DFT atomization energies. 7,165 small organic molecules composed of H, C, N, O, S, saturated with H, and up to 7 non-H atoms. Molecules relaxed with an empirical potential. Atomization energies calculated using DFT with hybrid PBE0 functional.",
                "year": 2012,
    
                "links": {
    
                    "landing_page": "http://qmml.org/datasets.html#gdb7-12",
    
                    "publication": ["https://doi.org/10.1103/PhysRevLett.108.058301"],
                    #"data_doi": "",
    
    #                "related_id": ,
    
                    "zip": {
                    
                        #"globus_endpoint": ,
                        "http_host": "http://qmml.org",
    
                        "path": "/Datasets/gdb7-12.zip",
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
    for data_file in tqdm(find_files(input_path, "xyz"), desc="Processing files", disable=not verbose):
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "xyz")
        record_metadata = {
            "mdf": {
                "title": "gdb7_12 " + data_file["filename"],
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
    
                        "path": "/collections/gdb7_12/" + data_file["filename"],
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
