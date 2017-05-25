import json
import sys
from tqdm import tqdm
from parsers.tab_parser import parse_tab
from validator import Validator


# This is the converter for the NIST X-Ray Transition Energies Database 
# Arguments:
#   input_path (string): The file or directory where the data resides. This should not be hard-coded in the function, for portability.
#   metadata (string or dict): The path to the JSON dataset metadata file, a dict containing the dataset metadata, or None to specify the metadata here. Default None.
#   verbose (bool): Should the script print status messages to standard output? Default False.
#       NOTE: The converter should have NO output if verbose is False, unless there is an error.
def convert(input_path, metadata=None, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    if not metadata:
        dataset_metadata = {
            "globus_subject": "https://www.nist.gov/pml/x-ray-transition-energies-database",
            "acl": ["public"],
            "mdf_source_name": "nist_xray_tran_en_db",
            "mdf-publish.publication.collection": "NIST X-Ray Transition Energies",
#            "mdf_data_class": ,

            "cite_as": ["http://physics.nist.gov/PhysRefData/XrayTrans/Html/refs.html"],
#            "license": ,

            "dc.title": "X-Ray Transition Energies Database",
            "dc.creator": "NIST",
            "dc.identifier": "https://www.nist.gov/pml/x-ray-transition-energies-database",
#            "dc.contributor.author": ,
            "dc.subject": ["Radiation", "Spectroscopy", "Reference data"],
            "dc.description": "This x-ray transition table provides the energies for K transitions connecting the K shell (n = 1) to the shells with principal quantum numbers n = 2 to 4 and L transitions connecting the L1, L2, and L3 shells (n = 2) to the shells with principal quantum numbers n = 3 and 4. The elements covered include Z = 10, neon to Z = 100, fermium. There are two unique features of this database: (1) all experimental values are on a scale consistent with the International System of measurement (the SI) and the numerical values are determined using constants from the Recommended Values of the Fundamental Physical Constants: 1998 [115] and (2) accurate theoretical estimates are included for all transitions. The user will find that for many of the transitions, the experimental and theoretical values are very consistent. It is our hope that the theoretical values will provide a useful estimate for missing or poorly measured experimental values.",
            "dc.relatedidentifier": ["http://physics.nist.gov/PhysRefData/XrayTrans/Html/search.html"],
            "dc.year": 2003
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
#    dataset_validator = Validator(dataset_metadata, strict=False)
    # You can also force the Validator to treat warnings as errors with strict=True
    dataset_validator = Validator(dataset_metadata, strict=True)


    # Get the data
    # Each record also needs its own metadata
    headers = ['element', 'A', 'transition', 'theory_(eV)', 'theory_uncertainty_(eV)', 'direct_(eV)', 'direct_uncertainty_(eV)', 'combined_(eV)', 'combined_uncertainty_(eV)', 'vapor_(eV)', 'vapor_uncertainty_(eV)', 'blend', 'reference']
    with open(input_path) as in_file:
        raw_data = in_file.read()
    count = 0
    for record in tqdm(parse_tab(raw_data, sep="\t", headers=headers), desc="Processing data", disable= not verbose):
        link = "http://physics.nist.gov/PhysRefData/XrayTrans/Html/search.html" + "#" + str(count)
        record_metadata = {
            "globus_subject": link,
            "acl": ["public"],
#            "mdf-publish.publication.collection": ,
#            "mdf_data_class": ,
            "mdf-base.material_composition": record["element"],

#            "cite_as": ,
#            "license": ,

            "dc.title": "X-Ray Transition - " + record["element"],
#            "dc.creator": ,
            "dc.identifier": link,
#            "dc.contributor.author": ,
#            "dc.subject": ,
#            "dc.description": ,
#            "dc.relatedidentifier": ,
#            "dc.year": ,

            "data": {
                "raw": json.dumps(record)
#                "files": 
                }
            }
        count += 1

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
    convert(paths.datasets+"nist_xray_tran_en_db/xray_tran_en_db.txt", verbose=True)
