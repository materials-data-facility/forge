import os
import json
import sys
from tqdm import tqdm
from parsers.utils import find_files
from parsers.ase_parser import parse_ase
from validator import Validator


# This is the converter for the Ta Melting dataset.
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
            "globus_subject": "http://hdl.handle.net/11256/88",
            "acl": ["public"],
            "mdf_source_name": "ta_melting",
            "mdf-publish.publication.collection": "Ta Melting",
            "mdf_data_class": "vasp",

            "cite_as": ["Qi-Jun Hong and Axel van de Walle, Solid-liquid coexistence in small systems: A statistical method to calculate melting temperatures, Journal of chemical physics, 139, 094114 (2013). http://dx.doi.org/10.1063/1.4819792"],
            "license": "http://creativecommons.org/licenses/by/3.0/us/",

            "dc.title": "Ta Melting Point Calculation by Small-cell Coexistence Method",
            "dc.creator": "Brown University, Caltech",
            "dc.identifier": "http://hdl.handle.net/11256/88",
            "dc.contributor.author": ["Qi-Jun Hong", "Axel van de Walle"],
#            "dc.subject": ,
            "dc.description": "We calculate the melting temperature of Tantalum, by employing the small-size coexistence solid-liquid coexistence method.",
            "dc.relatedidentifier": ["http://dx.doi.org/10.1063/1.4819792"],
            "dc.year": 2013
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
        try:
            record_metadata = {
                "globus_subject": uri,
                "acl": ["public"],
                "mdf-publish.publication.collection": "Ta Melting",
                "mdf-base.material_composition": data["frames"][0]["chemical_formula"],

#                "cite_as": ,
#                "license": ,

                "dc.title": "Ta Melting - " + data["frames"][0]["chemical_formula"],
#                "dc.creator": ,
                "dc.identifier": uri,
#                "dc.contributor.author": ,
#                "dc.subject": ,
#                "dc.description": ,
#                "dc.relatedidentifier": ,
#                "dc.year": ,

                "data": {
#                    "raw": ,
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
            print("Error on:", data_file["path"])

    if verbose:
        print("Finished converting")


# Optionally, you can have a default call here for testing
# The convert function may not be called in this way, so code here is primarily for testing
if __name__ == "__main__":
    import paths
    convert(paths.datasets+"ta_melting", None, True)
