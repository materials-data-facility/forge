import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.validator import Validator
from mdf_refinery.parsers.ase_parser import parse_ase

# VERSION 0.3.0

# This is the converter for the gdb9-14 dataset: The Quantum chemistry structures and properties of 134 kilo molecules
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
                "title": "Quantum Chemistry Structures and Properties of 134 kilo Molecules",
                "acl": ['public'],
                "source_name": "gdb9_14",
                "citation": ["Raghunathan Ramakrishnan, Pavlo Dral, Matthias Rupp, O. Anatole von Lilienfeld: Quantum Chemistry Structures and Properties of 134 kilo Molecules, Scientific Data 1: 140022, 2014."],
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
                        
                    "given_name": "Raghunathan",
                    "family_name": "Ramakrishnan",
                    
                    "institution": "University of Basel"
                    
                    },
                    {
                    
                    "given_name": "Pavlo O.",
                    "family_name": "Dral",
                    
                    "instituition": "Max-Planck-Institut für Kohlenforschung, University of Erlangen-Nuremberg",
                    
                    },
                    {
                    
                    "given_name": "Matthias",
                    "family_name": "Rupp",
                    
                    "instituition": "University of Basel",
                    
                    }],
    
                "license": "https://creativecommons.org/licenses/by-nc-sa/4.0/",
    
                "collection": "gdb9_14",
                "tags": ["Computational chemistry", "Density functional theory", "Quantum chemistry"],
    
                "description": "133,885 small organic molecules with up to 9 C, O, N, F atoms, saturated with H. Geometries, harmonic frequencies, dipole moments, polarizabilities, energies, enthalpies, and free energies of atomization at the DFT/B3LYP/6-31G(2df,p) level of theory. For a subset of 6,095 constitutional isomers of C7H10O2, energies, enthalpies, and free energies of atomization are provided at the G4MP2 level of theory.",
                "year": 2014,
    
                "links": {
    
                    "landing_page": "http://qmml.org/datasets.html#gdb9-14",
    
                    "publication": ["http://dx.doi.org/10.1038/sdata.2014.22"],
                   # "data_doi": "",
    
    #                "related_id": ,
    
                    "zip": {
                    
                        #"globus_endpoint": ,
                        "http_host": "http://qmml.org",
    
                        "path": "/Datasets/gdb9-14.zip",
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
    errors=0
    for data_file in tqdm(find_files(input_path, "xyz"), desc="Processing files", disable=not verbose):
        index = ""
        try:
            record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "xyz")
        except Exception as e:          #Unable to convert string to float on some files.
            errors+=1                   #String is in scientific form e.g. 6.2198*^-6
        comp = record["chemical_formula"]
        if data_file["no_root_path"] == "dsgdb9nsd.xyz":
            start = data_file["filename"].find('_')
            #index is between the underscore and ".xyz"
            index = int(data_file["filename"][start+1:-4])
            
        record_metadata = {
            "mdf": {
                "title": "gdb9_14 - " + comp,
                "acl": ['public'],
    
    #            "tags": ,
    #            "description": ,
                
                "composition": comp,
    #            "raw": ,
    
                "links": {
                   # "landing_page": ,
    
    #                "publication": ,
    #                "data_doi": ,
    
    #                "related_id": ,
    
                    "xyz": {
                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",
    
                        "path": "/collections/gdb9_14/" + data_file["no_root_path"] + "/" + data_file["filename"],
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
    #            "year": ,
    
    #            "mrr":
    
    #            "processing": ,
    #            "structure":,
                },
            "gdb9_14": {
                "index": index,
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
        print("TOTAL ERRORS: " + str(errors))
        print("Finished converting")
