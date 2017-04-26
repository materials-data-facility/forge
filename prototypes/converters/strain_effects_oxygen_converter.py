import os
import json
import sys
from tqdm import tqdm
from parsers.utils import find_files
from parsers.ase_parser import parse_ase
from validator import Validator


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
            "globus_subject": "http://hdl.handle.net/11256/701",
            "acl": ["public"],
            "mdf_source_name": "strain_effects_oxygen",
            "mdf-publish.publication.collection": "Strain Effects on Oxygen Migration",

            "cite_as": ["Mayeshiba, T. & Morgan, D. Strain effects on oxygen migration in perovskites. Physical chemistry chemical physics : PCCP 17, 2715-2721, doi:10.1039/c4cp05554c (2015).", "Mayeshiba, T. & Morgan, D. Correction: Strain effects on oxygen migration in perovskites. Physical chemistry chemical physics : PCCP, doi:10.1039/c6cp90050j (2016)."],
#            "license": ,

            "dc.title": "Strain effects on oxygen migration in perovskites: La[Sc, Ti, V, Cr, Mn, Fe, Co, Ni, Ga]O3",
            "dc.creator": "University of Wisconsin-Madison",
            "dc.identifier": "http://hdl.handle.net/11256/701",
            "dc.contributor.author": ["Mayeshiba, Tam", "Morgan, Dane"],
#            "dc.subject": ,
#            "dc.description": ,
            "dc.relatedidentifier": ["https://dx.doi.org/10.1039/c4cp05554c", "https://dx.doi.org/10.1039/c6cp90050j"],
            "dc.year": 2016
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
    for data_file in tqdm(find_files(input_path, "^OUTCAR$"), desc="Processing files", disable= not verbose):
        data = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "vasp")
        uri = "https://data.materialsdatafacility.org/collections/" + data_file["no_root_path"] + "/" + data_file["filename"]
        try:
            record_metadata = {
                "globus_subject": uri,
                "acl": ["public"],
    #            "mdf-publish.publication.collection": ,
                "mdf_data_class": "vasp",
                "mdf-base.material_composition": data["frames"][0]["chemical_formula"],

    #            "cite_as": ,
    #            "license": ,

                "dc.title": "Oxygen Migration - " + data["frames"][0]["chemical_formula"],
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

            # Pass each individual record to the Validator
            result = dataset_validator.write_record(record_metadata)

            # Check if the Validator accepted the record, and print a message if it didn't
            # If the Validator returns "success" == True, the record was written successfully
            if result["success"] is not True:
                print("Error:", result["message"], ":", result.get("invalid_metadata", ""))
        except Exception:
            if verbose:
                print("Error on:", os.path.join(data_file["path"], data_file["filename"]))

    if verbose:
        print("Finished converting")


# Optionally, you can have a default call here for testing
# The convert function may not be called in this way, so code here is primarily for testing
if __name__ == "__main__":
    import paths
    convert(paths.datasets+"strain_effects_oxygen", None, True)
