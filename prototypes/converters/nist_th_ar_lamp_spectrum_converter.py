import json
import sys
from tqdm import tqdm
from parsers.tab_parser import parse_tab
from validator import Validator


# This is the converter for NIST's Th-Ar spectrum dataset
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
            "globus_subject": "https://www.nist.gov/pml/spectrum-th-ar-hollow-cathode-lamps",
            "acl": ["public"],
            "mdf_source_name": "nist_th_ar_lamp_spectrum",
            "mdf-publish.publication.collection": "NIST Spectrum of Th-Ar Hollow Cathode Lamps",
#            "mdf_data_class": ,

            "cite_as": ["NIST SRD 161"],
#            "license": ,

            "dc.title": "NIST Spectrum of Th-Ar Hollow Cathode Lamps",
            "dc.creator": "NIST",
            "dc.identifier": "https://www.nist.gov/pml/spectrum-th-ar-hollow-cathode-lamps",
            "dc.contributor.author": ["Gillian Nave", "Craig J. Sansonetti1", "Florian Kerber"],
            "dc.subject": ["Spectroscopy", "Reference data"],
            "dc.description": "This atlas presents observations of the infra-red (IR) spectrum of a low current Th-Ar hollow cathode lamp with the 2-m Fourier transform spectrometer (FTS) at the National Institute of Standards and Technology. These observations establish more than 2400 lines that are suitable for use as wavelength standards in the range 691 nm to 5804 nm.",
#            "dc.relatedidentifier": ,
            "dc.year": 2009
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
#   dataset_validator = Validator(dataset_metadata, strict=False)
    # You can also force the Validator to treat warnings as errors with strict=True
    dataset_validator = Validator(dataset_metadata, strict=True)


    # Get the data
    # Each record also needs its own metadata
    headers = ["wavenumber", "wavenumber_uncertainty_le-3", "snr", "fwhm_le-3", "intensity", "species", "lower_level", "lower_j", "upper_level", "upper_j", "vacuum_wavelength", "vacuum_wavelength_uncertainty_le-3"]
    with open(input_path) as in_file:
        raw = in_file.read()
    while "  " in raw:
        raw = raw.replace("  ", " ")
    for record in tqdm(parse_tab(raw, headers=headers, sep=" "), desc="Processing records", disable= not verbose):
        link = "http://physics.nist.gov/PhysRefData/ThArLampAtlas/node9.html#" + record["wavenumber"]
        record_metadata = {
            "globus_subject": link,
            "acl": ["public"],
#            "mdf-publish.publication.collection": ,
#            "mdf_data_class": ,
            "mdf-base.material_composition": record["species"],

#            "cite_as": ,
#            "license": ,

            "dc.title": "Hollow Cathode Lamp Spectrum - " + record["wavenumber"],
#            "dc.creator": ,
            "dc.identifier": link,
#            "dc.contributor.author": ,
#            "dc.subject": ,
#            "dc.description": ,
#            "dc.relatedidentifier": ,
#            "dc.year": ,

            "data": {
                "raw": json.dumps(record)
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

    if verbose:
        print("Finished converting")


# Optionally, you can have a default call here for testing
# The convert function may not be called in this way, so code here is primarily for testing
if __name__ == "__main__":
    import paths
    convert(paths.datasets+"nist_th_ar_lamp_spectrum/nist_th_ar_lamp_spectrum.txt", verbose=True)
