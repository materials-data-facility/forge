import json
import sys
import os

from tqdm import tqdm

from ..validator.schema_validator import Validator
from ..utils.file_utils import find_files

# VERSION 0.2.0

# This is the converter for the JCAP XPS Spectral Database
# Arguments:
#   input_path (string): The file or directory where the data resides.
#       NOTE: Do not hard-code the path to the data in the converter. The converter should be portable.
#   metadata (string or dict): The path to the JSON dataset metadata file, a dict or json.dumps string containing the dataset metadata, or None to specify the metadata here. Default None.
#   verbose (bool): Should the script print status messages to standard output? Default False.
#       NOTE: The converter should have NO output if verbose is False, unless there is an error.
def convert(input_path, metadata=None, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    if not metadata:
        dataset_metadata = {
            "mdf-title": "JCAP XPS Spectral Database",
            "mdf-acl": ["public"],
            "mdf-source_name": "jcap_xps_spectral_db",
            "mdf-citation": ["http://solarfuelshub.org/xps-spectral-database"],
            "mdf-data_contact": {

                "given_name": "Harry",
                "family_name": "Atwater",

                "email": "info@solarfuelshub.org",
                "institution": "Joint Center for Artificial Photosynthesis",

                },

#            "mdf-author": ,

#            "mdf-license": ,

            "mdf-collection": "JCAP XPS Spectral DB",
            "mdf-data_format": "json",
            "mdf-data_type": "XPS",
            "mdf-tags": ["xps", "spectra"],

            "mdf-description": "The JCAP High Throughput Experimentation research team uses combinatorial methods to quickly identify promising light absorbers and catalysts for solar-fuel devices. Pure-phase materials — including metal oxides, nitrides, sulfides, oxinitrides, and other single- and mixed-metal materials — are prepared using multiple deposition techniques (e.g., physical vapor deposition, inkjet printing, and micro-fabrication) on various substrates. High-resolution X-ray photoelectron spectroscopy (XPS) spectra for materials that have been characterized to date are made available here as part of JCAP's Materials Characterization Standards (MatChS) database.",
#            "mdf-year": ,

            "mdf-links": {

                "mdf-landing_page": "http://solarfuelshub.org/xps-spectral-database",

#                "mdf-publication": ,
#                "mdf-dataset_doi": ,

#                "mdf-related_id": ,

                # data links: {

                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #}
                },

#            "mdf-mrr": ,

            "mdf-data_contributor": {
                "given_name": "Jonathon",
                "family_name": "Gaff",
                "email": "jgaff@uchicago.edu",
                "institution": "The University of Chicago",
                "github": "jgaff"
                }
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


    dataset_validator = Validator(dataset_metadata)


    # Get the data
    for data_file in tqdm(find_files(input_path, ".json"), desc="Processing files", disable= not verbose):
        with open(os.path.join(data_file["path"], data_file["filename"])) as in_file:
            data = json.load(in_file)
        record_metadata = {
            "mdf-title": "JCAP Spectra - " + data["xps_region"],
            "mdf-acl": ["public"],

#            "mdf-tags": ,
#            "mdf-description": ,
            
            "mdf-composition": data.pop("material"),
#            "mdf-raw": ,

            "mdf-links": {
                "mdf-landing_page": data.pop("link"),

#                "mdf-publication": ,
#                "mdf-dataset_doi": ,

#                "mdf-related_id": ,

                # data links: {
 
                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #},
                },

#            "mdf-citation": ,
#            "mdf-data_contact": {

#                "given_name": ,
#                "family_name": ,

#                "email": ,
#                "institution":,

                # IDs
#                },

#            "mdf-author": ,

#            "mdf-license": ,
#            "mdf-collection": ,
#            "mdf-data_format": ,
#            "mdf-data_type": ,
            "mdf-year": data.pop("year"),

#            "mdf-mrr":

#            "mdf-processing": ,
#            "mdf-structure":,
            }
        data.pop("data")
        record_metadata.update(data)

        # Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)

        # Check if the Validator accepted the record, and print a message if it didn't
        # If the Validator returns "success" == True, the record was written successfully
        if result["success"] is not True:
            print("Error:", result["message"])


    if verbose:
        print("Finished converting")
