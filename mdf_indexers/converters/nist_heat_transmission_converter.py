import json
import sys
from tqdm import tqdm
from validator import Validator


# This is the converter for NIST's SRD on heat transmission in building materials
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
            "globus_subject": "https://srdata.nist.gov/insulation/home/index",
            "acl": ["public"],
            "mdf_source_name": "nist_heat_transmission",
            "mdf-publish.publication.collection": "NIST Heat Transmission Materials",
#            "mdf_data_class": ,

            "cite_as": ["Robert R. Zarr, Josue A. Chavez, Angela Y. Lee, Geraldine Dalton, and Shari L. Young, NIST Heat Transmission Properties of Insulating and Building Materials, NIST Standard Reference Database Number 81, National Institute of Standards and Technology, Gaithersburg MD, 20899, http://srdata.nist.gov/Insulation/."],
#            "license": ,

            "dc.title": "NIST Heat Transmission Properties of Insulating and Building Materials",
            "dc.creator": "NIST",
            "dc.identifier": "http://srdata.nist.gov/Insulation/",
            "dc.contributor.author": ["Robert R. Zarr", "Josue A. Chavez", "Angela Y. Lee", "Geraldine Dalton", "Shari L. Young"],
#            "dc.subject": ,
            "dc.description": "The NIST Database on Heat Conductivity of Building Materials provides a valuable reference for building designers, material manufacturers, and researchers in the thermal design of building components and equipment. NIST has accumulated a valuable and comprehensive collection of thermal conductivity data from measurements performed with a 200-mm square guarded-hot-plate apparatus (from 1933 to 1983). The guarded-hot-plate test method is arguably the most accurate and popular method for determination of thermal transmission properties of flat, homogeneous specimens under steady state conditions.",
#            "dc.relatedidentifier": ,
            "dc.year": 2015
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
    with open(input_path) as in_file:
        dataset = json.load(in_file)
    for record in tqdm(dataset, desc="Processing data", disable= not verbose):
        link = "https://srdata.nist.gov/insulation/Search/detail/" + str(record["ID"])
        record_metadata = {
            "globus_subject": link,
            "acl": ["public"],
#            "mdf-publish.publication.collection": ,
#            "mdf_data_class": ,
#            "mdf-base.material_composition": ,

#            "cite_as": ,
#            "license": ,

            "dc.title": "Heat Transmission Properties - " + (record.get("Material") or record.get("tradename") or record.get("manufacturer", "") + str(record["ID"])),
#            "dc.creator": ,
            "dc.identifier": link,
#            "dc.contributor.author": ,
#            "dc.subject": , 
#            "dc.description": record.get("tradename", "") + " " + record.get("manufacturer", ""),
#            "dc.relatedidentifier": ,
#            "dc.year": ,

            "data": {
                "raw": json.dumps(record)
#                "files": 
                }
            }
        desc = ""
        if record.get("tradename"):
            desc += record.get("tradename")
        if record.get("manufacturer"):
            if desc:
                desc += " by "
            desc += record.get("manufacturer")
        if desc:
            record_metadata["dc.description"] = desc

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
    convert(paths.datasets+"nist_heat_transmission/nist_heat_transmission.json", verbose=True)
