import os
from tqdm import tqdm
from validator import Validator
from parsers.utils import find_files
from parsers.ase_parser import parse_ase


# This is the converter for Khazana VASP DFT data.
# Arguments:
#   input_path (string): The file or directory where the data resides. This should not be hard-coded in the function, for portability.
#   verbose (bool): Should the script print status messages to standard output? Default False.
def convert(input_path, verbose=False):

    # Collect the metadata
    dataset_metadata = {
        "globus_subject": "http://khazana.uconn.edu/module_search/search.php?m=2",
        "acl": ["public"],
        "mdf_source_name": "khazana_vasp",
        "mdf-publish.publication.collection": "Khazana",
        "mdf_data_class": "vasp",

        "cite_as": ["http://khazana.uconn.edu/module_search/search.php?m=2"],
        "dc.title": "Khazana (VASP)",
        "dc.creator": "University of Connecticut",
        "dc.identifier": "http://khazana.uconn.edu",
#        "dc.contributor.author": ,
        "dc.subject": ["DFT", "VASP"]
#        "dc.description": ,
#        "dc.relatedidentifier": ,
#        "dc.year": 
        }


    # Make a Validator to help write the feedstock
    # You must pass the metadata to the constructor
    # Each Validator instance can only be used for a single dataset
    dataset_validator = Validator(dataset_metadata)


    # Get the data
    #    Each record should be exactly one dictionary
    #    It is recommended that you convert your records one at a time, but it is possible to put them all into one big list (see below)
    #    It is also recommended that you use a parser to help with this process if one is available for your datatype

    # Each record also needs its own metadata
    for dir_data in tqdm(find_files(root=input_path, file_pattern="^OUTCAR"), desc="Processing data files", disable= not verbose):
        file_data = parse_ase(file_path=os.path.join(dir_data["path"], dir_data["filename"]), data_format="vasp", verbose=False)

        uri = "https://data.materialsdatafacility.org/collections/khazana/OUTCARS/" + dir_data["filename"]
        record_metadata = {
            "globus_subject": uri,
            "acl": ["public"],
            "mdf-publish.publication.collection": "Khazana",
            "mdf-base.material_composition": file_data["frames"][0]["chemical_formula"],

            "dc.title": "Khazana VASP - " + file_data["frames"][0]["chemical_formula"],
#            "dc.creator": ,
            "dc.identifier": uri,
#            "dc.contributor.author": ,
#            "dc.subject": ,
#            "dc.description": ,
#            "dc.relatedidentifier": ,
#            "dc.year": ,

            "data": {
#                "raw": str(file_data),
                "files": {"outcar": uri}
                }
            }

        # Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)

        # Check if the Validator accepted the record, and print a message if it didn't
        # If the Validator returns "success" == True, the record was written successfully
        if result["success"] is not True:
            print("Error:", result["message"], ":", result.get("invalid_metadata"))

    if verbose:
        print("Finished converting")


# Optionally, you can have a default call here for testing
# The convert function may not be called in this way, so code here is primarily for testing
if __name__ == "__main__":
    import paths
    print("Begin conversion")
    convert(paths.datasets+"khazana/OUTCARS", True)
