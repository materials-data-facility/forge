import os
from tqdm import tqdm
import json
from parsers.ase_parser import parse_ase
from parsers.utils import find_files
from validator import Validator


# This is the converter for the CoRE-MOF dataset. 
# Arguments:
#   input_path (string): The file or directory where the data resides. This should not be hard-coded in the function, for portability.
#   doi_file_path (string): A CoRE-MOF-specific data file with additional useful data (DOIs) not contained in the data files.
#   verbose (bool): Should the script print status messages to standard output? Default False.
def convert(input_path, doi_file_path, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    dataset_metadata = {
        "globus_subject": "http://gregchung.github.io/CoRE-MOFs/",
        "acl": ["public"],
        "mdf_source_name": "core_mof",
        "mdf-publish.publication.collection": "CoRE-MOF",

        "cite_as": ['D. Nazarian, J. Camp, D.S. Sholl, "A Comprehensive Set of High-Quality Point Charges for Simulations of Metal-Organic Frameworks," Chemistry of Materials, 2016, 28 (3), pp 785–793'],
        "dc.title": "Computation-Ready Experimental Metal-Organic Frameworks Database",
        "dc.creator": "Pusan National University",
        "dc.identifier": "http://gregchung.github.io/CoRE-MOFs/",
        "dc.contributor.author": ["Y.G. Chung", "J. Camp", "M. Haranczyk", "B.J. Sikora", "W. Bury", "V. Krungleviciute", "T. Yildirim", "O.K. Farha", "D.S. Sholl", "R.Q. Snurr"],
#        "dc.subject": ,
        "dc.description": "High-throughput computational screening of metal-organic frameworks rely on the availability of disorder-free atomic coordinate files which can be used as input to simulation software packages.",
        "dc.relatedidentifier": ["https://dx.doi.org/10.1021/cm502594j"],
        "dc.year": 2014
        }


    # Make a Validator to help write the feedstock
    # You must pass the metadata to the constructor
    # Each Validator instance can only be used for a single dataset
    dataset_validator = Validator(dataset_metadata)

    # Get the data
    # Each record also needs its own metadata
    # Get DOIs
    doi_dict = {}
    with open(doi_file_path) as dois:
        for line in dois:
            values = line.split(",")
            if values[1] != "-":
                doi_dict[values[0]] = values[1]

    for cif in tqdm(find_files(input_path, file_pattern=".cif", verbose=verbose), desc="Processing CIFs", disable= not verbose):
        with open(os.path.join(cif["path"], cif["filename"])) as cif_in:
            # Discard non-CIF, duplicate metadata in first line
            cif_in.readline()
            file_data = parse_ase(file_path=cif_in, data_format="cif", verbose=False)

        record_metadata = {
            "globus_subject": "https://raw.githubusercontent.com/gregchung/gregchung.github.io/master/CoRE-MOFs/core-mof-v1.0-ddec/" + cif["filename"],
            "acl": ["public"],
            "mdf-publish.publication.collection": "CoRE-MOF",
            "mdf_data_class": "cif",
            "mdf-base.material_composition": file_data["chemical_formula"],

            "dc.title": "CoRE-MOF - " + file_data["chemical_formula"] + " (" + cif["filename"].split("_")[0] + ")",
#            "dc.creator": ,
            "dc.identifier": "https://github.com/gregchung/gregchung.github.io/blob/master/CoRE-MOFs/core-mof-v1.0-ddec/" + cif["filename"],
#            "dc.contributor.author": ,
#            "dc.subject": ,
#            "dc.description": ,
#            "dc.relatedidentifier": [doi_dict[key] for key in doi_dict.keys() if cif["filename"].startswith(key)],
#            "dc.year": ,

            "data": {
#                "raw": ,
                "files": {"cif": "https://raw.githubusercontent.com/gregchung/gregchung.github.io/master/CoRE-MOFs/core-mof-v1.0-ddec/" + cif["filename"]}
                }
            }
        relatedidentifier = [doi_dict[key] for key in doi_dict.keys() if cif["filename"].startswith(key)]
        if relatedidentifier:
            record_metadata["dc.relatedidentifier"] = relatedidentifier

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
    convert(paths.datasets+"core_mof/core-mof-v1.0-ddec", paths.datasets+"core_mof/structure-doi-CoRE-MOFsV2.0.csv", True)
