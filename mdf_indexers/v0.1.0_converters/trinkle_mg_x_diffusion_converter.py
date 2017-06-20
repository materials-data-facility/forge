import os
import json
import sys
from tqdm import tqdm
from parsers.utils import find_files
from parsers.ase_parser import parse_ase
from validator import Validator


# This is the converter for the Trinkle Mg-X-Diffusion Dataset
# Arguments:
#   input_path (string): The file or directory where the data resides. This should not be hard-coded in the function, for portability.
#   metadata (string or dict): The path to the JSON dataset metadata file, a dict containing the dataset metadata, or None to specify the metadata here. Default None.
#   verbose (bool): Should the script print status messages to standard output? Default False.
def convert(input_path, metadata=None, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    if not metadata:
        dataset_metadata = {
            "globus_subject": "https://data.materialsdatafacility.org/published/#trinkle_mg_x_diffusion",
            "acl": ["public"],
            "mdf_source_name": "trinkle_mg_x_diffusion",
            "mdf-publish.publication.collection": "Mg-X Diffusion Dataset",
            "mdf_data_class": "vasp",

            "cite_as": ["Citation for dataset Mg-X-Diffusion with author(s): Dallas Trinkle, Ravi Agarwal"],
#            "license": "",

            "dc.title": "Mg-X-Diffusion",
            "dc.creator": "University of Illinois at Urbana-Champaign",
            "dc.identifier": "https://data.materialsdatafacility.org/published/#trinkle_mg_x_diffusion",
            "dc.contributor.author": ["Trinkle, Dallas", "Agarwal, Ravi"],
            #"dc.subject": [],
            #"dc.description": "",
#            "dc.relatedidentifier": [],
            "dc.year": 2017
            }
    elif type(metadata) is str:
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
    dataset_validator = Validator(dataset_metadata)

    # Get the data
    # Each record also needs its own metadata
    for data_file in tqdm(find_files(input_path, "OUTCAR"), desc="Processing files", disable=not verbose):
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "vasp")
        uri = "https://data.materialsdatafacility.org/collections/" + "mg-x/" + data_file["no_root_path"] + "/" + data_file["filename"]
        record_metadata = {
            "globus_subject": uri,
            "acl": ["public"],
#            "mdf-publish.publication.collection": ,
#            "mdf-base.material_composition": record["frames"][0]["chemical_formula"],

#            "cite_as": ,
#            "license": ,

            "dc.title": "Mg-X Diffusions - ",
#            "dc.creator": ,
            "dc.identifier": uri,
#            "dc.contributor.author": ,
#            "dc.subject": ,
#            "dc.description": ,
#            "dc.relatedidentifier": ,
#            "dc.year": ,

            "data": {
#                "raw": ,
                "files": {"outcar": uri}
                }
            }
        try:
            record_metadata["mdf-base.material_composition"] = record["frames"][0]["chemical_formula"]
            record_metadata["dc.title"] += " " + record["frames"][0]["chemical_formula"]
        except:
            #parse_ase unable to read composition of record 1386: https://data.materialsdatafacility.org/collections/mg-x/Elements/Eu/Mg-X_Eu/OUTCAR
            #Placing in the correct material composition
            record_metadata["mdf-base.material_composition"] = "EuMg149"
            record_metadata["dc.title"] += "EuMg149"
            
        # Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)

        # Check if the Validator accepted the record, and print a message if it didn't
        # If the Validator returns "success" == True, the record was written successfully
        if result["success"] is not True:
            print("Error:", result["message"], ":", result.get("invalid_metadata", ""))

    if verbose:
        print("Finished converting")


# Optionally, you can have a default call here for testing
# The convert function may not be called in this way, so code here is primarily for testing
if __name__ == "__main__":
    import paths
    convert(paths.datasets+"trinkle_mg_x_diffusion", verbose=True)
