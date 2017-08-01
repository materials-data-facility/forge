import json
import sys
import os

from tqdm import tqdm

from mdf_refinery.validator import Validator
from mdf_refinery.parsers.tab_parser import parse_tab

# VERSION 0.3.0

# This is the converter for NIST SRD 161
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
        "mdf": {
            "title": "NIST Spectrum of Th-Ar Hollow Cathode Lamps",
            "acl": ["public"],
            "source_name": "nist_th_ar_lamp_spectrum",
            "citation": ["NIST SRD 161"],
            "data_contact": {

                "given_name": "Gillian",
                "family_name": "Nave",

                "email": "gillian.nave@nist.gov",
                "institution": "National Institute of Standards and Technology",

                },

            "author": [{

                "given_name": "Gillian",
                "family_name": "Nave",

                "email": "gillian.nave@nist.gov",
                "institution": "National Institute of Standards and Technology",

                },
                {

                "given_name": "Craig",
                "family_name": "Sansonetti",

                "institution": "National Institute of Standards and Technology",

                },
                {

                "given_name": "Florian",
                "family_name": "Kerber",

                "institution": "European Southern Observatory",

                }],

#            "license": ,

            "collection": "NIST Spectrum of Th-Ar Hollow Cathode Lamps",
            "tags": ["Spectroscopy", "Reference data"],

            "description": "This atlas presents observations of the infra-red (IR) spectrum of a low current Th-Ar hollow cathode lamp with the 2-m Fourier transform spectrometer (FTS) at the National Institute of Standards and Technology. These observations establish more than 2400 lines that are suitable for use as wavelength standards in the range 691 nm to 5804 nm.",
            "year": 2009,

            "links": {

                "landing_page": "https://www.nist.gov/pml/spectrum-th-ar-hollow-cathode-lamps",

#                "publication": ,
#                "dataset_doi": ,

#                "related_id": ,

                # data links: {

                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #}
                },

#            "mrr": ,

            "data_contributor": {
                "given_name": "Jonathon",
                "family_name": "Gaff",
                "email": "jgaff@uchicago.edu",
                "institution": "The University of Chicago",
                "github": "jgaff"
                }
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
    headers = ["wavenumber", "wavenumber_uncertainty_le-3", "snr", "fwhm_le-3", "intensity", "species", "lower_level", "lower_j", "upper_level", "upper_j", "vacuum_wavelength", "vacuum_wavelength_uncertainty_le-3"]
    with open(os.path.join(input_path, "nist_th_ar_lamp_spectrum.txt")) as in_file:
        raw = in_file.read()
    while "  " in raw:
        raw = raw.replace("  ", " ")
    for record in tqdm(parse_tab(raw, headers=headers, sep=" "), desc="Processing records", disable= not verbose):
        record_metadata = {
        "mdf": {
            "title": "Hollow Cathode Lamp Spectrum - " + record["wavenumber"],
            "acl": ["public"],

#            "tags": ,
#            "description": ,
            
            "composition": record["species"],
            "raw": json.dumps(record),

            "links": {
                "landing_page": "http://physics.nist.gov/PhysRefData/ThArLampAtlas/node9.html",

#                "publication": ,
#                "dataset_doi": ,

#                "related_id": ,

                # data links: {
 
                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #},
                },

#            "citation": ,
#            "data_contact": {

#                "given_name": ,
#                "family_name": ,

#                "email": ,
#                "institution":,

                # IDs
#                },

#            "author": ,

#            "license": ,
#            "collection": ,
#            "data_format": ,
#            "data_type": ,
#            "year": ,

#            "mrr":

#            "processing": ,
#            "structure":,
            }
        }

        # Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)

        # Check if the Validator accepted the record, and print a message if it didn't
        # If the Validator returns "success" == True, the record was written successfully
        if result["success"] is not True:
            print("Error:", result["message"])


    if verbose:
        print("Finished converting")
