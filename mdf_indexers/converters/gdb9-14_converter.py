import json
import sys
import os
from ..utils.file_utils import find_files
from ..parsers.pymatgen_parser import parse_pymatgen
from tqdm import tqdm
from ..validator.schema_validator import Validator

# VERSION 0.2.0

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
            "mdf-title": "Quantum Chemistry Structures and Properties of 134 kilo Molecules",
            "mdf-acl": ['public'],
            "mdf-source_name": "gdb9-14",
            "mdf-citation": ["Raghunathan Ramakrishnan, Pavlo Dral, Matthias Rupp, O. Anatole von Lilienfeld: Quantum Chemistry Structures and Properties of 134 kilo Molecules, Scientific Data 1: 140022, 2014."],
            "mdf-data_contact": {

                "given_name": "O. Anatole",
                "family_name": "von Lilienfeld",

                "email": "anatole@alcf.anl.gov",
                "institution": "Argonne National Laboratory",
                },

            "mdf-author": [{
                
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

            "mdf-license": "https://creativecommons.org/licenses/by-nc-sa/4.0/",

            "mdf-collection": "gdb9-14",
            "mdf-data_format": ["xyz"],
            "mdf-data_type": ["DFT"],
            "mdf-tags": ["Computational chemistry", "Density functional theory", "Quantum chemistry"],

            "mdf-description": "133,885 small organic molecules with up to 9 C, O, N, F atoms, saturated with H. Geometries, harmonic frequencies, dipole moments, polarizabilities, energies, enthalpies, and free energies of atomization at the DFT/B3LYP/6-31G(2df,p) level of theory. For a subset of 6,095 constitutional isomers of C7H10O2, energies, enthalpies, and free energies of atomization are provided at the G4MP2 level of theory.",
            "mdf-year": 2014,

            "mdf-links": {

                "mdf-landing_page": "http://qmml.org/datasets.html#gdb9-14",

                "mdf-publication": ["http://dx.doi.org/10.1038/sdata.2014.22"],
               # "mdf-dataset_doi": "",

#                "mdf-related_id": ,

                "zip": {
                
                    #"globus_endpoint": ,
                    "http_host": "http://qmml.org",

                    "path": "/Datasets/gdb9-14.zip",
                    }
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
    for data_file in tqdm(find_files(input_path, "xyz"), desc="Processing files", disable=not verbose):
        record = parse_pymatgen(os.path.join(data_file["path"], data_file["filename"]))
        if record["structure"]:
            comp = record["structure"]["material_composition"]
        elif record["molecule"]:
            comp = record["molecule"]["material_composition"]
        uri = "https://data.materialsdatafacility.org/collections/" + "gdb9-14/" + data_file["no_root_path"] + "/" + data_file["filename"]
        index = ""
        if data_file["no_root_path"] == "dsgdb9nsd.xyz":
            start = data_file["filename"].find('_')
            #index is between the underscore and ".xyz" in the filename hence the -4 end
            index = int(data_file["filename"][start+1:-4])
            
        record_metadata = {
            "mdf-title": "gdb9-14 - " + data_file["filename"],
            "mdf-acl": ['public'],

#            "mdf-tags": ,
#            "mdf-description": ,
            
            "mdf-composition": comp,
#            "mdf-raw": ,

            "mdf-links": {
                "mdf-landing_page": uri,

#                "mdf-publication": ,
#                "mdf-dataset_doi": ,

#                "mdf-related_id": ,

                "data_links": {
                    "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                    #"http_host": ,

                    "path": "/collections/gdb9-14/" + data_file["no_root_path"] + "/" + data_file["filename"],
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
            "index": index
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
