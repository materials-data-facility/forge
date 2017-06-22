import json
import sys
import os
from tqdm import tqdm
from parsers.utils import find_files
from parsers.ase_parser import parse_ase
from validator import Validator

# VERSION 0.1.0

# This is the template for new converters. It is not a complete converter. Incomplete parts are labelled with "TODO"
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
            "globus_subject": "http://qmml.org/datasets.html#md-17",
            "acl": ["public"],
            "mdf_source_name": "md-17",
            "mdf-publish.publication.collection": "MD-17",
            "mdf_data_class": "xyz",

            "cite_as": ["S. Chmiela, A. Tkatchenko, H. E. Sauceda, I. Poltavsky, K.Schütt, K.-R. Müller, arXiv:1611.04678 (2017)"],
            "license": "http://creativecommons.org/licenses/by/4.0/",
            "mdf_version": "0.1.0",

            "dc.title": "Quantum-chemical insights from deep tensor neural networks",
            "dc.creator": "Technische Universität Berlin, Korea University, Fritz-Haber-Institut der Max-Planck-Gesellschaft, University of Luxembourg",
            "dc.identifier": "http://qmml.org/datasets.html",
            "dc.contributor.author": ["S. Chmiela", "A. Tkatchenko", "H. E. Sauceda", "I. Poltavsky", "K.Schütt", "K.-R. Müller"],
            "dc.subject": ["Applied mathematics", "Computational chemistry", "Physical chemistry", "Scientific data"],
            "dc.description": "Energies and forces from molecular dynamics trajectories of eight organic molecules. Ab initio molecular dynamics trajectories (133k to 993k frames) of benzene, uracil, naphthalene, aspirin, salicylic acid, malonaldehyde, ethanol, toluene at the DFT/PBE+vdW-TS level of theory at 500 K.",
            "dc.relatedidentifier": ["http://dx.doi.org/10.1038/ncomms13890", "https://arxiv.org/abs/1611.04678"],
            "dc.year": 2017
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
    #dataset_validator = Validator(dataset_metadata, strict=False)
    # You can also force the Validator to treat warnings as errors with strict=True
    dataset_validator = Validator(dataset_metadata, strict=True)
    all_frames = {
        "aspirin": 111763,
        "benzene": 527984,
        "ethanol": 455093,
        "malonaldehyde": 893238,
        "naphthalene": 226256,
        "salicylic_acid": 220232,
        "toluene": 342791,
        "uracil": 133770
    }

    # Get the data
    # Each record also needs its own metadata
    for data_file in tqdm(find_files(input_path, "000001.xyz"), desc="Processing files", disable=not verbose):
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "xyz")
        uri = "https://data.materialsdatafacility.org/collections/" + "md-17/" + data_file["no_root_path"] + '/' + data_file["filename"]
        file_name = data_file["no_root_path"]
        num_frames = all_frames[file_name]
        record_metadata = {
            "globus_subject": uri,
            "acl": ["public"],
#            "mdf-publish.publication.collection": ,
#            "mdf_data_class": ,
            "mdf-base.material_composition": record["chemical_formula"],

#            "cite_as": ,
#            "license": ,

            "dc.title": "MD-17 - " + record["chemical_formula"],
#            "dc.creator": ,
            "dc.identifier": uri,
#            "dc.contributor.author": ,
#            "dc.subject": ,
#            "dc.description": ,
#            "dc.relatedidentifier": ,
#            "dc.year": ,

            "data": {
#                "raw": ,
#                "files": ,
                "number_of_frames": num_frames,
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

    # Alternatively, if the only way you can process your data is in one large list, you can pass the list to the Validator
    # You still must add the required metadata to your records
    # It is recommended to use the previous method if possible
    # result = dataset_validator.write_dataset(your_records_with_metadata)
    #if result["success"] is not True:
        #print("Error:", result["message"])

    # You're done!
    if verbose:
        print("Finished converting")


# Optionally, you can have a default call here for testing
# The convert function may not be called in this way, so code here is primarily for testing
if __name__ == "__main__":
    import paths
    convert(paths.datasets + "md-17", verbose=True)
