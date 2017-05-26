import json
import sys
import os
from tqdm import tqdm
from validator import Validator


# This is the converter for NIST's Atomic Weights and Isotopic Compositions with Relative Atomic Masses dataset. 
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
            "globus_subject": "https://www.nist.gov/pml/atomic-weights-and-isotopic-compositions-relative-atomic-masses",
            "acl": ["public"],
            "mdf_source_name": "nist_atom_weight_iso_comp",
            "mdf-publish.publication.collection": "NIST Atomic Weights and Isotopic Compositions",
#            "mdf_data_class": ,

            "cite_as": ["NIST Standard Reference Database 144"],
#            "license": ,

            "dc.title": "Atomic Weights and Isotopic Compositions with Relative Atomic Masses",
            "dc.creator": "NIST",
            "dc.identifier": "https://www.nist.gov/pml/atomic-weights-and-isotopic-compositions-relative-atomic-masses",
#            "dc.contributor.author": ,
            "dc.subject": ["Atomic", "molecular", "quantum", "Nuclear physics", "Reference data"],
            "dc.description": "The atomic weights are available for elements 1 through 118 and isotopic compositions or abundances are given when appropriate.",
            "dc.relatedidentifier": ["http://www.ciaaw.org/atomic-weights.htm", "http://www.iupac.org/publications/pac/83/2/0397/", "http://amdc.impcas.ac.cn/evaluation/data2012/ame.html"],
            "dc.year": 1999
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
    with open(os.path.join(input_path, "notes.json")) as notes:
        note_lookup = json.load(notes)
    with open(os.path.join(input_path, "atom_weight_iso_comp.txt")) as raw_file:
        raw = raw_file.read()
    raw = raw.replace("&nbsp;", "")
    record_list = raw.split("\n\n")
    for raw_record in tqdm(record_list, desc="Processing records", disable= not verbose):
        record = {}
        for line in raw_record.split("\n"):
            data_list = line.split("=")
            if len(data_list) > 1 and data_list[1].strip():
                record[data_list[0].strip().lower().replace(" ", "_")] = data_list[1].strip()
        note_list = [note_lookup[n] for n in record.get("notes", "").split(",") if record.get("notes", "")]
        link = "https://www.nist.gov/pml/atomic-weights-and-isotopic-compositions-relative-atomic-masses#" + record["atomic_symbol"] + record["mass_number"]
        record_metadata = {
            "globus_subject": link,
            "acl": ["public"],
#            "mdf-publish.publication.collection": ,
#            "mdf_data_class": ,
            "mdf-base.material_composition": record["atomic_symbol"],

#            "cite_as": ,
#            "license": ,

            "dc.title": "NIST Atomic Weights - " + record["atomic_symbol"] + record["mass_number"],
#            "dc.creator": ,
            "dc.identifier": link,
#            "dc.contributor.author": ,
#            "dc.subject": ,
#            "dc.description": ",".join(note_list),
#            "dc.relatedidentifier": ,
#            "dc.year": ,

            "data": {
                "raw": json.dumps(record)
#                "files": 
                }
            }
        if note_list:
            record_metadata["dc.description"] = ",".join(note_list)

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
    convert(paths.datasets+"nist_atom_weight_iso_comp", verbose=True)
