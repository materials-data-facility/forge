import os
import json
import sys
from tqdm import tqdm
from parsers.utils import find_files
from parsers.ase_parser import parse_ase
from validator import Validator


# This is the converter for the Doak Strain Energies dataset
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
            "globus_subject": "http://hdl.handle.net/11256/85",
            "acl": ["public"],
            "mdf_source_name": "doak_strain_energies",
            "mdf-publish.publication.collection": "Doak Strain Energies",
            "mdf_data_class": "vasp",

            "cite_as": ["Doak JW, Wolverton C (2012) Coherent and incoherent phase stabilities of thermoelectric rocksalt IV-VI semiconductor alloys. Phys. Rev. B 86: 144202 http://dx.doi.org/10.1103/PhysRevB.86.144202"],
            "license": "http://creativecommons.org/licenses/by-sa/3.0/us/",

            "dc.title": "GeTe-PbTe PbS-PbTe PbSe-PbS PbTe-PbSe PbTe-SnTe SnTe-GeTe mixing and coherency strain energies",
            "dc.creator": "Northwestern University",
            "dc.identifier": "http://hdl.handle.net/11256/85",
            "dc.contributor.author": ["Doak, JW", "Wolverton, C"],
#            "dc.subject": ,
#            "dc.description": ,
            "dc.relatedidentifier": ["http://dx.doi.org/10.1103/PhysRevB.86.144202"],
            "dc.year": 2012
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
                "mdf-publish.publication.collection": "Doak Strain Energies",
                "mdf-base.material_composition": data["frames"][0]["chemical_formula"],

    #            "cite_as": ,
    #            "license": ,

                "dc.title": "Strain Energy - " + data["frames"][0]["chemical_formula"],
    #            "dc.creator": ,
                "dc.identifier": uri,
    #            "dc.contributor.author": ,
    #            "dc.subject": ,
    #            "dc.description": ,
    #            "dc.relatedidentifier": ,
    #            "dc.year": ,

                "data": {
#                    "raw": ,
                    "files": {"outcar" : uri}
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
    convert(paths.datasets+"doak_strain_energies", None, True)
