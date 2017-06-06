import json
import sys
import os
from tqdm import tqdm
from parsers.tab_parser import parse_tab
from parsers.utils import find_files
from validator import Validator

# VERSION 0.1.0

# This is the converter for the Ni-Co-Al-Ti-Cr quinary alloys dataset
# Arguments:
#   input_path (string): The file or directory where the data resides. This should not be hard-coded in the function, for portability.
#   metadata (string or dict): The path to the JSON dataset metadata file, a dict or json.dumps string containing the dataset metadata, or None to specify the metadata here. Default None.
#   verbose (bool): Should the script print status messages to standard output? Default False.
#       NOTE: The converter should have NO output if verbose is False, unless there is an error.
def convert(input_path, metadata=None, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    if not metadata:
        dataset_metadata = {
            "globus_subject": "https://doi.org/10.17863/CAM.705",
            "acl": ["public"],
            "mdf_source_name": "quinary_alloys",
            "mdf-publish.publication.collection": "Ni-Co-Al-Ti-Cr Quinary Alloys",
#            "mdf_data_class": ,

            "cite_as": ['Christofidou, K. A., Jones, N. G., Pickering, E. J., Flacau, R., Hardy, M. C., & Stone, H. J. Research Data Supporting "The microstructure and hardness of Ni-Co-Al-Ti-Cr quinary alloys" [Dataset]. https://doi.org/10.17863/CAM.705'],
            "license": "http://creativecommons.org/licenses/by/4.0/",
            "mdf_version": "0.1.0",

            "dc.title": 'Research Data Supporting "The microstructure and hardness of Ni-Co-Al-Ti-Cr quinary alloys"',
            "dc.creator": "University of Cambridge",
            "dc.identifier": "https://doi.org/10.17863/CAM.705",
            "dc.contributor.author": ["Christofidou, K. A.", "Jones, N. G.", "Pickering, E. J.", "Flacau, R.", "Hardy, M. C.", "Stone, H. J."],
            "dc.subject": ["DSC", "SEM", "TEM", "neutron diffraction", "thermodynamics", "hardness"],
            "dc.description": "DSC files, neutron diffraction data, hardness measurements, SEM and TEM images and thermodynamic simulations are provided for all alloy compositions studied and presented in this manuscript. The naming convention is provided in the manuscript along with the composition of each alloy.",
            "dc.relatedidentifier": ["https://doi.org/10.1016/j.jallcom.2016.07.159"],
            "dc.year": 2016
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
#    dataset_validator = Validator(dataset_metadata, strict=False)
    # You can also force the Validator to treat warnings as errors with strict=True
    dataset_validator = Validator(dataset_metadata, strict=True)


    # Get the data
    # Each record also needs its own metadata
    with open(os.path.join(input_path, "alloy_data.csv"), 'r') as adata:
        raw_data = adata.read()
    for record in tqdm(parse_tab(raw_data), desc="Processing records", disable= not verbose):
        links = {}
        mdf_base = "https://data.materialsdatafacility.org/collections/quinary_alloys/"
        for ln in find_files(input_path, record["Alloy"]):
            key = "_".join(ln["no_root_path"].split("/")).replace(" ", "_")
            links[key] = mdf_base + os.path.join(ln["no_root_path"], ln["filename"])
        record_metadata = {
            "globus_subject": mdf_base + "alloy_data.csv#" + record["Alloy"],
            "acl": ["public"],
#            "mdf-publish.publication.collection": ,
#            "mdf_data_class": ,
            "mdf-base.material_composition": "NiCoAlTiCr",

#            "cite_as": ,
#            "license": ,

            "dc.title": "Ni-Co-Al-Ti-Cr Quinary Alloys " + record["Alloy"],
#            "dc.creator": ,
            "dc.identifier": mdf_base + "alloy_data.csv",
#            "dc.contributor.author": ,
#            "dc.subject": ,
#            "dc.description": ,
#            "dc.relatedidentifier": ,
#            "dc.year": ,

            "data": {
                "raw": json.dumps(record),
                "files": links,
                "atomic_composition_percent": {
                    "Ni": record["Ni"],
                    "Co": record["Co"],
                    "Al": record["Al"],
                    "Ti": record["Ti"],
                    "Cr": record["Cr"]
                    }
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

    if verbose:
        print("Finished converting")


# Optionally, you can have a default call here for testing
# The convert function may not be called in this way, so code here is primarily for testing
if __name__ == "__main__":
    import paths
    convert(paths.datasets+"quinary_alloys", verbose=True)
