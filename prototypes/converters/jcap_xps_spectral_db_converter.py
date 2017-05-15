import json
import sys
import os
from tqdm import tqdm
from parsers.utils import find_files
from validator import Validator
from parsers.tab_parser import parse_tab


# This is the converter for the JCAP XPS Spectral Database 
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
            "globus_subject": "http://solarfuelshub.org/xps-spectral-database",
            "acl": ["public"],
            "mdf_source_name": "jcap_xps_spectral_db",
            "mdf-publish.publication.collection": "JCAP XPS Spectral DB",
#            "mdf_data_class": ,

            "cite_as": ["http://solarfuelshub.org/xps-spectral-database"],
#            "license": ,

            "dc.title": "JCAP XPS Spectral Database",
            "dc.creator": "JCAP",
            "dc.identifier": "http://solarfuelshub.org/xps-spectral-database",
#            "dc.contributor.author": ,
#            "dc.subject": ,
            "dc.description": "The JCAP High Throughput Experimentation research team uses combinatorial methods to quickly identify promising light absorbers and catalysts for solar-fuel devices. Pure-phase materials — including metal oxides, nitrides, sulfides, oxinitrides, and other single- and mixed-metal materials — are prepared using multiple deposition techniques (e.g., physical vapor deposition, inkjet printing, and micro-fabrication) on various substrates. High-resolution X-ray photoelectron spectroscopy (XPS) spectra for materials that have been characterized to date are made available here as part of JCAP's Materials Characterization Standards (MatChS) database.",
#            "dc.relatedidentifier": ,
#            "dc.year": 
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
    for data_file in tqdm(find_files(input_path, ".json"), desc="Processing files", disable= not verbose):
        with open(os.path.join(data_file["path"], data_file["filename"])) as in_file:
            data = json.load(in_file)
        link = data.pop("link")
        record_metadata = {
            "globus_subject": link,
            "acl": ["public"],
#            "mdf-publish.publication.collection": ,
#            "mdf_data_class": ,
            "mdf-base.material_composition": data["material"],

#            "cite_as": ,
#            "license": ,

            "dc.title": "JCAP Spectra - " + data["xps_region"],
#            "dc.creator": ,
            "dc.identifier": link,
#            "dc.contributor.author": ,
#            "dc.subject": ,
#            "dc.description": ,
#            "dc.relatedidentifier": ,
            "dc.year": data.pop("year"),

            "data": {
#                "raw": json.dumps(list(parse_tab(data.pop("data")))),
                "files": {"csv": link}
                }
            }
        data.pop("data")
        record_metadata["data"].update(data)

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
    convert(paths.datasets+"jcap_xps_spectral_db", verbose=True)
