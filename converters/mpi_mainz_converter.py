import json
import sys
from tqdm import tqdm
from parsers.utils import find_files
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
    # TODO: Make sure the metadata is present in some form.
    # Fields can be:
    #    REQ (Required, must be present)
    #    RCM (Recommended, should be present if possible)
    #    OPT (Optional, can be present if useful)
    if not metadata:
        dataset_metadata = {
            "globus_subject": "https://doi.org/10.5281/zenodo.6951",                      # REQ string: Unique value (should be URI if possible)
            "acl": ["public"],                                 # REQ list of strings: UUID(s) of users/groups allowed to access data, or ["public"]
            "mdf_source_name": "mpi_mainz",                     # REQ string: Unique name for dataset
            "mdf-publish.publication.collection": "The MPI-Mainz UV/VIS Spectral Atlas of Gaseous Molecules",  # RCM string: Collection the dataset belongs to
            "mdf_data_class": "UV/VIS",                      # RCM string: Type of data in all records in the dataset (do not provide for multi-type datasets)

            "cite_as": ["Keller-Rudek, H. M.-P. I. for C. M. G., Moortgat, G. K. M.-P. I. for C. M. G., Sander, R. M.-P. I. for C. M. G., & Sörensen, R. M.-P. I. for C. M. G. (2013). The MPI-Mainz UV/VIS Spectral Atlas of Gaseous Molecules [Data set]. Zenodo. http://doi.org/10.5281/zenodo.6951,"],                             # REQ list of strings: Complete citation(s) for this dataset.
            "license": "https://creativecommons.org/licenses/by/4.0/",                             # RCM string: License to use the dataset (preferrably a link to the actual license).
            "mdf_version": "0.1.0",                         # REQ string: The metadata version in use (see VERSION above).

            "dc.title": "The MPI-Mainz UV/VIS Spectral Atlas of Gaseous Molecules",                            # REQ string: Title of dataset
            "dc.creator": "Max-Planck Institute for Chemistry, Mainz, Germany",                          # REQ string: Owner of dataset
            "dc.identifier": "https://doi.org/10.5281/zenodo.6951",                       # REQ string: Link to dataset (dataset DOI if available)
            "dc.contributor.author": ["Keller-Rudek", "Moortgat, Geert K.", "Sander", "Sörensen"],              # RCM list of strings: Author(s) of dataset
            "dc.subject": ["cross sections", "quantum yields"],                          # RCM list of strings: Keywords about dataset
            "dc.description": "This archive contains a frozen snapshot of all cross section and quantum yield data files from the MPI-Mainz UV/VIS Spectral Atlas of Gaseous Molecules.",                      # RCM string: Description of dataset contents
#            "dc.relatedidentifier": ,                # RCM list of strings: Link(s) to related materials (such as an article)
            "dc.year": 2013                              # RCM integer: Year of dataset creation
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
    dataset_validator = Validator(dataset_metadata, strict=False)
    # You can also force the Validator to treat warnings as errors with strict=True
    #dataset_validator = Validator(dataset_metadata, strict=True)


    # Get the data
    # TODO: Write the code to convert your dataset's records into JSON-serializable Python dictionaries
    #    Each record should be exactly one dictionary
    #    It is recommended that you convert your records one at a time, but it is possible to put them all into one big list (see below)
    #    It is also recommended that you use a parser to help with this process if one is available for your datatype

    # Each record also needs its own metadata
    for file_data in tqdm(find_files(input_path, ".txt"), desc="Processing files", disable=not verbose):
        in1 = file_data["filename"].find("_")
        comp = file_data["filename"][:in1]
        # TODO: Fill in these dictionary fields for each record
        # Fields can be:
        #    REQ (Required, must be present)
        #    RCM (Recommended, should be present if possible)
        #    OPT (Optional, can be present if useful)
        record_metadata = {
            "globus_subject": "https://doi.org/10.5281/zenodo.6951#" + file_data["filename"],                      # REQ string: Unique value (should be URI to record if possible)
            "acl": ["public"],                                 # REQ list of strings: UUID(s) of users/groups allowed to access data, or ["public"]
#            "mdf-publish.publication.collection": ,  # OPT string: Collection the record belongs to (if different from dataset)
#            "mdf_data_class": ,                      # OPT string: Type of data in record (if not set in dataset metadata)
            "mdf-base.material_composition": comp,       # RCM string: Chemical composition of material in record

#            "cite_as": ,                             # OPT list of strings: Complete citation(s) for this record (if different from dataset)
#            "license": ,                             # OPT string: License to use the record (if different from dataset) (preferrably a link to the actual license).

            "dc.title": "mpi_mainz - " + file_data["filename"],                            # REQ string: Title of record
#            "dc.creator": ,                          # OPT string: Owner of record (if different from dataset)
#            "dc.identifier": ,                       # RCM string: Link to record (record webpage, if available)
#            "dc.contributor.author": ,               # OPT list of strings: Author(s) of record (if different from dataset)
#            "dc.subject": ,                          # OPT list of strings: Keywords about record
#            "dc.description": ,                      # OPT string: Description of record
#            "dc.relatedidentifier": ,                # OPT list of strings: Link(s) to related materials (if different from dataset)
#            "dc.year": ,                             # OPT integer: Year of record creation (if different from dataset)

#            "data": {                                # RCM dictionary: Other record data (described below)
#                "raw": ,                             # RCM string: Original data record text, if feasible
#                "files": ,                           # RCM dictionary: {file_type : uri_to_file} pairs, data files (Example: {"cif" : "https://example.org/cifs/data_file.cif"})

                # other                              # RCM any JSON-valid type: Any other data fields you would like to include go in the "data" dictionary. Keys will be prepended with 'mdf_source_name:'
#                }
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

    # TODO: Save your converter as [mdf_source_name]_converter.py
    # You're done!
    if verbose:
        print("Finished converting")


# Optionally, you can have a default call here for testing
# The convert function may not be called in this way, so code here is primarily for testing
if __name__ == "__main__":
    import paths
    convert(paths.datasets+"uv-vis-spectral-atlas-2013-06-24", verbose=True)
