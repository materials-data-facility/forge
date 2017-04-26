import os
import json
import sys
from tqdm import tqdm
from parsers.utils import find_files
from parsers.ase_parser import parse_ase
from validator import Validator


# This is the converter for
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
            "globus_subject": "http://hdl.handle.net/11256/272",
            "acl": ["public"],
            "mdf_source_name": "oxygen_interstitials_deformation",
            "mdf-publish.publication.collection": "Oxygen Interstitials and Deformation Twins in alpha-Titanium",

            "cite_as": ["Interaction Between Oxygen Interstitials and Deformation Twins in alpha-Titanium, Acta Materialia v. 105 (2016), pp. 44 - 51 http://dx.doi.org/10.1016/j.actamat.2015.12.019"],
            "license": "http://creativecommons.org/licenses/by/3.0/us/",

            "dc.title": "Interaction Between Oxygen Interstitials and Deformation Twins in alpha-Titanium",
            "dc.creator": "University of Maryland",
            "dc.identifier": "http://hdl.handle.net/11256/272",
            "dc.contributor.author": ["Joost, William J.", "Ankem, Sreeramamurthy", "Kuklja, Maija M."],
#            "dc.subject": ,
#            "dc.description": ,
            "dc.relatedidentifier": ["http://dx.doi.org/10.1016/j.actamat.2015.12.019"],
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
    for data_file in tqdm(find_files(input_path, "OUTCAR"), desc="Processing files", disable= not verbose):
        data = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "vasp")
        uri = "https://data.materialsdatafacility.org/collections/" + data_file["no_root_path"] + "/" + data_file["filename"]
        record_metadata = {
            "globus_subject": uri,
            "acl": ["public"],
            "mdf-publish.publication.collection": "Oxygen Interstitials and Deformation Twins in alpha-Titanium",
            "mdf_data_class": "vasp",
            "mdf-base.material_composition": data["frames"][0]["chemical_formula"],

#            "cite_as": ,
#            "license": ,

            "dc.title": "Oxygen Interstitials and Deformation Twins - " + data["frames"][0]["chemical_formula"],
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

    if verbose:
        print("Finished converting")


# Optionally, you can have a default call here for testing
# The convert function may not be called in this way, so code here is primarily for testing
if __name__ == "__main__":
    import paths
    convert(paths.datasets+"oxygen_interstitals_deformation", None, True)
