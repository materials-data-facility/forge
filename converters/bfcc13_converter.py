import json
import sys
import os
from parsers.ase_parser import parse_ase
from parsers.utils import find_files
from tqdm import tqdm
from validator import Validator

# VERSION 0.1.0

# This is the converter for the BFCC-13 dataset: Cluster expansion made easy with Bayesian compressive sensing
# Arguments:
#   input_path (string): The file or directory where the data resides. This should not be hard-coded in the function, for portability.
#   metadata (string or dict): The path to the JSON dataset metadata file, a dict or json.dumps string containing the dataset metadata, or None to specify the metadata here. Default None.
#   verbose (bool): Should the script print status messages to standard output? Default False.
#       NOTE: The converter should have NO output if verbose is False, unless there is an error.
def convert(input_path, metadata=None, verbose=False):
    if verbose:
        print("Begin converting")

    if not metadata:
        dataset_metadata = {
            "globus_subject": "http://qmml.org/datasets.html#bfcc-13",
            "acl": ["public"],
            "mdf_source_name": "bfcc13",
            "mdf-publish.publication.collection": "bfcc13",
            "mdf_data_class": "vasp",

            "cite_as": ["Lance J. Nelson, Vidvuds Ozoliņš, C. Shane Reese, Fei Zhou, Gus L.W. Hart: Cluster expansion made easy with Bayesian compressive sensing, Physical Review B 88(15): 155105, 2013."],
#            "license": ,
            "mdf_version": "0.1.0",

            "dc.title": "Cluster expansion made easy with Bayesian compressive sensing",
            "dc.creator": "Brigham Young University, University of California Los Angeles, Lawrence Livermore National Laboratory",
            "dc.identifier": "http://qmml.org/datasets.html",
            "dc.contributor.author": ["Lance J. Nelson", "Vidvuds Ozoliņš", "C. Shane Reese", "Fei Zhou", "Gus L.W. Hart"],
#            "dc.subject": ,
            "dc.description": "4k DFT calculations for solid AgPd, CuPt and AgPt FCC superstructures. DFT/PBE energy, forces and stresses for cell sizes 1-16 across all compositions including primitive cells.",
            "dc.relatedidentifier": ["https://journals.aps.org/prb/abstract/10.1103/PhysRevB.88.155105"],
            "dc.year": 2013
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
    #dataset_validator = Validator(dataset_metadata, strict=False)
    # You can also force the Validator to treat warnings as errors with strict=True
    dataset_validator = Validator(dataset_metadata, strict=True)


    # Get the data
    #    Each record should be exactly one dictionary
    #    It is recommended that you convert your records one at a time, but it is possible to put them all into one big list (see below)
    #    It is also recommended that you use a parser to help with this process if one is available for your datatype
    # Each record also needs its own metadata
    for data_file in tqdm(find_files(input_path, "OUTCAR"), desc="Processing files", disable=not verbose):
        data = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "vasp")
        if data:
            uri = "https://data.materialsdatafacility.org/collections/" + "bfcc-13/bfcc-13/" + data_file["no_root_path"] + "/" + data_file["filename"]
            record_metadata = {
                "globus_subject": uri,
                "acl": ["public"],
    #            "mdf-publish.publication.collection": ,
    #            "mdf_data_class": ,
                "mdf-base.material_composition": data["frames"][0]["chemical_formula"],
    
    #            "cite_as": ,
    #            "license": ,
    
                "dc.title": "bfcc13 - " + data["frames"][0]["chemical_formula"],
    #            "dc.creator": ,
                "dc.identifier": uri,
    #            "dc.contributor.author": ,
    #            "dc.subject": ,
    #            "dc.description": ,
    #            "dc.relatedidentifier": ,
    #            "dc.year": ,
    
                "data": {
    #                "raw": ,
                    "files": {"outcar": uri},
                        }
                }
    
            # Pass each individual record to the Validator
            result = dataset_validator.write_record(record_metadata)
    
            # Check if the Validator accepted the record, and print a message if it didn't
            # If the Validator returns "success" == True, the record was written successfully
            if result["success"] is not True:
                print("Error:", result["message"], ":", result.get("invalid_metadata", ""))
            # The Validator may return warnings if strict=False, which should be noted
            if result.get("warnings", None):
                print("Warnings:", result["warnings"])

    # Alternatively, if the only way you can process your data is in one large list, you can pass the list to the Validator
    # You still must add the required metadata to your records
    # It is recommended to use the previous method if possible
    # result = dataset_validator.write_dataset(your_records_with_metadata)
    #if result["success"] is not True:
        #print("Error:", result["message"])

    # You're done!
    if verbose:
        print("Finished converting")


# Optionally, you can have a default call here for testing
# The convert function may not be called in this way, so code here is primarily for testing
if __name__ == "__main__":
    import paths
    convert(paths.datasets + "bfcc-13", verbose=True)
