import os
from json import load
from tqdm import tqdm
from parsers.utils import find_files
from validator import Validator


# This is the converter for the NIST-JANAF Thermochemical tables. 
# Arguments:
#   input_path (string): The file or directory where the data resides. This should not be hard-coded in the function, for portability.
#   verbose (bool): Should the script print status messages to standard output? Default False.
def convert(input_path, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    dataset_metadata = {
        "globus_subject": "http://kinetics.nist.gov/janaf/",
        "acl": ["public"],
        "mdf_source_name": "nist_janaf",
        "mdf-publish.publication.collection": "NIST-JANAF",

        "cite_as": ["M. W. Chase, Jr., JANAF Thermochemical Tables Third Edition, J. Phys. Chem. Ref. Data, Vol. 14, Suppl. 1, 1985."],
        "dc.title": "NIST-JANAF Thermochemical Tables",
        "dc.creator": "NIST",
        "dc.identifier": "http://kinetics.nist.gov/janaf/",
        "dc.contributor.author": ["M.W. Chase, Jr.", "C.A. Davies", "J.R. Downey, Jr.", "D.J. Frurip", "R.A. McDonald", "A.N. Syverud"],
#        "dc.subject": ,
        "dc.description": "DISCLAIMER: NIST uses its best efforts to deliver a high quality copy of the Database and to verify that the data contained therein have been selected on the basis of sound scientific judgement. However, NIST makes no warranties to that effect, and NIST shall not be liable for any damage that may result from errors or omissions in the Database.",
#        "dc.relatedidentifier": ,
        "dc.year": 1985
        }


    # Make a Validator to help write the feedstock
    # You must pass the metadata to the constructor
    # Each Validator instance can only be used for a single dataset
    dataset_validator = Validator(dataset_metadata)


    # Get the data
    # Each record also needs its own metadata
    for entry in tqdm(find_files(input_path, ".*[0-9]\.json$"), desc="Processing data", disable= not verbose):
        with open(os.path.join(entry["path"], entry["filename"])) as in_file:
            data = load(in_file)

        record_metadata = {
            "globus_subject": "http://kinetics.nist.gov/janaf/" + entry["filename"],
            "acl": ["public"],
            "mdf-publish.publication.collection": "NIST-JANAF",
#            "mdf_data_class": ,
            "mdf-base.material_composition": data['identifiers']['molecular formula'],

#            "cite_as": ,
            "dc.title": "NIST-JANAF - " + data['identifiers']['chemical formula'] + " " + data['identifiers']['state'],
#            "dc.creator": ,
#            "dc.identifier": ,
#            "dc.contributor.author": ,
#            "dc.subject": ,
#            "dc.description": ,
#            "dc.relatedidentifier": ,
#            "dc.year": ,

            "data": {
#                "raw": ,
#                "files": 
                'state': "".join([data["state definitions"][st] + ", " for st in data['identifiers']['state'].split(",")])
#                'cas': data['identifiers']['cas registry number']
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
    convert(paths.datasets + "janaf/srd13_janaf", True)
