from os.path import join
from tqdm import tqdm
import json
from validator import Validator


# This is the converter for NIST's Classical Interatomic Potentials dataset
# Arguments:
#   input_path (string): The file or directory where the data resides. This should not be hard-coded in the function, for portability.
#   verbose (bool): Should the script print status messages to standard output? Default False.
def convert(input_path, verbose=False):
    if verbose:
        print("Begin converting")
    # Collect the metadata
    dataset_metadata = {
        "globus_subject": "http://datadryad.org/resource/doi:10.5061/dryad.dd56c",
        "acl": ["public"],
        "mdf_source_name": "cip",
        "mdf-publish.publication.collection": "Classical Interatomic Potentials",

        "dc.title": "Evaluation and comparison of classical interatomic potentials through a user-friendly interactive web-interface",
        "dc.creator": "National Institute of Standards and Technology",
        "dc.identifier": "http://dx.doi.org/10.5061/dryad.dd56c",
        "dc.contributor.author": ["Choudhary K", "Congo FYP", "Liang T", "Becker C", "Hennig RG", "Tavazza F"],
        "dc.subject": ["interatomic potentials", "force-fields", "total energy", "energy", "elastic matrix", "structure", "elastic modulus", "JARVIS"],
#        "dc.description": ,
        "dc.relatedidentifier": ["http://dx.doi.org/10.1038/sdata.2016.125"],
        "dc.year": 2017
        }


    # Make a Validator to help write the feedstock
    # You must pass the metadata to the constructor
    # Each Validator instance can only be used for a single dataset
    dataset_validator = Validator(dataset_metadata)


    # Get the data
    # Each record also needs its own metadata
    with open(input_path) as in_file:
        for record in tqdm(json.load(in_file), desc="Converting data", disable= not verbose):
            record_metadata = {
                "globus_subject": record["case-number"],
                "acl": ["public"],
                "mdf-publish.publication.collection": "Classical Interatomic Potentials",
#                "mdf_data_class": ,
                "mdf-base.material_composition": record["composition"],

                "dc.title": "NIST Classical Interatomic Potential - " + record["forcefield"] + ", " + record["composition"],
#                "dc.creator": ,
                "dc.identifier": record["case-number"],
#                "dc.contributor.author": ,
#                "dc.subject": ,
#                "dc.description": ,
#                "dc.relatedidentifier": ,
#                "dc.year": ,

                "data": {
                    "raw": json.dumps(record),
                    "files": {}
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
    convert(paths.datasets + "10.5061_dryad.dd56c/classical_interatomic_potentials.json", True)
