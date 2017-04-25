import json
import sys
import os
from tqdm import tqdm
from parsers.utils import find_files
from parsers.ase_parser import parse_ase
from validator import Validator


# This is the converter for the Ti-O EAM Model dataset.
# Arguments:
#   input_path (string): The file or directory where the data resides. This should not be hard-coded in the function, for portability.
#   metadata (string or dict): The path to the JSON dataset metadata file, a dict containing the dataset metadata, or None to specify the metadata here. Default None.
#   verbose (bool): Should the script print status messages to standard output? Default False.
def convert(input_path, metadata=None, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    if not metadata:
        dataset_metadata = {
#            "globus_subject": ,                      # REQ string: Unique value (should be URI if possible)
#            "acl": ,                                 # REQ list of strings: UUID(s) of users/groups allowed to access data, or ["public"]
#            "mdf_source_name": ,                     # REQ string: Unique name for dataset
#            "mdf-publish.publication.collection": ,  # RCM string: Collection the dataset belongs to

#            "cite_as": ,                             # REQ list of strings: Complete citation(s) for this dataset.
#            "license": ,                             # RCM string: License to use the dataset (preferrably a link to the actual license).

#            "dc.title": ,                            # REQ string: Title of dataset
#            "dc.creator": ,                          # REQ string: Owner of dataset
#            "dc.identifier": ,                       # REQ string: Link to dataset (dataset DOI if available)
#            "dc.contributor.author": ,               # RCM list of strings: Author(s) of dataset
#            "dc.subject": ,                          # RCM list of strings: Keywords about dataset
#            "dc.description": ,                      # RCM string: Description of dataset contents
#            "dc.relatedidentifier": ,                # RCM list of strings: Link(s) to related materials (such as an article)
#            "dc.year":                               # RCM integer: Year of dataset creation
            }
    elif type(metadata) is str:
        try:
            with open(metadata, 'r') as metadata_file:
                dataset_metadata = json.load(metadata_file)
        except Exception as e:
            sys.exit("Error: Unable to read metadata: " + repr(e))
    elif type(metadata) is dict:
        dataset_metadata = metadata



    # Make a Validator to help write the feedstock
    # You must pass the metadata to the constructor
    # Each Validator instance can only be used for a single dataset
    dataset_validator = Validator(dataset_metadata)


    # Get the data
    # Each record also needs its own metadata
    for file_data in tqdm(find_files(input_path, "OUTCAR"), desc="Processing files", disable= not verbose):
        try:
            record = parse_ase(os.path.join(file_data["path"], file_data["filename"]), "vasp")
            if not record:
                raise ValueError("No data returned")
        except Exception as e:
            continue
        record_metadata = {
            "globus_subject": "https://data.materialsdatafacility.org/collections/" + file_data["no_root_path"] + "/" + file_data["filename"],                      # REQ string: Unique value (should be URI to record if possible)
            "acl": ["public"],                                 # REQ list of strings: UUID(s) of users/groups allowed to access data, or ["public"]
            "mdf-publish.publication.collection": "Ti-O MEAM Model",  # RCM string: Collection the record belongs to
            "mdf_data_class": "vasp",                      # RCM string: Type of data in record
            "mdf-base.material_composition": record["frames"][0]["chemical_formula"],       # RCM string: Chemical composition of material in record

#            "cite_as": ,                             # OPT list of strings: Complete citation(s) for this record (if different from dataset)
#            "license": ,                             # OPT string: License to use the record (if different from dataset) (preferrably a link to the actual license).

            "dc.title": "Ti-O MEAM Model - " + record["frames"][0]["chemical_formula"],                            # REQ string: Title of record
#            "dc.creator": ,                          # OPT string: Owner of record (if different from dataset)
            "dc.identifier": "https://data.materialsdatafacility.org/collections/" + file_data["no_root_path"] + "/" + file_data["filename"],                       # RCM string: Link to record (record webpage, if available)
#            "dc.contributor.author": ,               # OPT list of strings: Author(s) of record (if different from dataset)
#            "dc.subject": ,                          # OPT list of strings: Keywords about record
#            "dc.description": ,                      # OPT string: Description of record
#            "dc.relatedidentifier": ,                # OPT list of strings: Link(s) to related materials (if different from dataset)
#            "dc.year": ,                             # OPT integer: Year of record creation (if different from dataset)

            "data": {                                # REQ dictionary: Other record data (described below)
#                "raw": ,                             # RCM string: Original data record text, if feasible
                "files": {"outcar": "https://data.materialsdatafacility.org/collections/" + file_data["no_root_path"] + "/" + file_data["filename"]}
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
    meta = {
        "globus_subject": "https://materialsdata.nist.gov/dspace/xmlui/handle/11115/244",
        "acl": ["public"],
        "mdf_source_name": "ti_o_meam_model",
        "mdf-publish.publication.collection": "Ti-O MEAM Model",
        "cite_as": ['W.J. Joost, S. Ankem, M.M. Kuklja "A modified embedded atom method potential for the titanium-oxygen system" Modelling and Simulation in Materials Science and Engineering Vol. 23, pp. 015006 (2015) doi:10.1088/0965-0393/23/1/015006'],
        "dc.title": "A Modified Embedded Atom Method Potential for the Titanium-Oxygen System",
        "dc.creator": "University of Maryland",
        "dc.identifier": "https://materialsdata.nist.gov/dspace/xmlui/handle/11115/244",
        "dc.contributor.author": ["W.J. Joost", "S. Ankem", "M.M. Kuklja"],
        "dc.relatedidentifier": ["https:/dx.doi.org/10.1088/0965-0393/23/1/015006"],
        "dc.year": 2014
        }
    convert(paths.datasets+"ti_o_meam_model", meta, True)

