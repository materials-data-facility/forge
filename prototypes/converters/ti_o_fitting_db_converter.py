import os
from tqdm import tqdm
from parsers.utils import find_files
from parsers.ase_parser import parse_ase
from validator import Validator


# This is the converter for the Ti-O Fitting Database. 
# Arguments:
#   input_path (string): The file or directory where the data resides. This should not be hard-coded in the function, for portability.
#   verbose (bool): Should the script print status messages to standard output? Default False.
def convert(input_path, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    dataset_metadata = {
        "globus_subject": "http://hdl.handle.net/11256/782",                      # REQ string: Unique value (should be URI if possible)
        "acl": ["public"],                                 # REQ list of strings: UUID(s) of users/groups allowed to access data, or ["public"]
        "mdf_source_name": "ti_o_fitting_db",                     # REQ string: Unique name for dataset
        "mdf-publish.publication.collection": "Ti-O Fitting Database",  # RCM string: Collection the dataset belongs to

        "cite_as": ["Trinkle, Dallas R.; Zhang, Pinchao Fitting database entries for a modified embedded atom method potential for interstitial oxygen in titanium (2016-07-25) http://hdl.handle.net/11256/782"],
        "license": "http://creativecommons.org/licenses/by/3.0/us/",                             # RCM string: License to use the dataset (preferrably a link to the actual license).

        "dc.title": "Fitting database entries for a modified embedded atom method potential for interstitial oxygen in titanium",                            # REQ string: Title of dataset
        "dc.creator": "University of Illinois, Urbana-Champaign",                          # REQ string: Owner of dataset
        "dc.identifier": "http://hdl.handle.net/11256/782",                       # REQ string: Link to dataset (dataset DOI if available)
        "dc.contributor.author": ["Trinkle, Dallas R", "Zhang, Pinchao"],               # RCM list of strings: Author(s) of dataset
#        "dc.subject": ,                          # RCM list of strings: Keywords about dataset
#        "dc.description": ,                      # RCM string: Description of dataset contents
#        "dc.relatedidentifier": ,                # RCM list of strings: Link(s) to related materials (such as an article)
        "dc.year": 2016                              # RCM integer: Year of dataset creation
        }


    # Make a Validator to help write the feedstock
    # You must pass the metadata to the constructor
    # Each Validator instance can only be used for a single dataset
    dataset_validator = Validator(dataset_metadata)


    # Get the data
    # Each record also needs its own metadata
    for data_file in tqdm(find_files(input_path, "OUTCAR"), desc="Processing files", disable= not verbose):
        data = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "vasp")
        uri = "https://data.materialsdatafacility.org/collections/" + data_file["no_root_path"] + "/" + data_file["filename"]

        try:
            record_metadata = {
                "globus_subject": uri,                      # REQ string: Unique value (should be URI to record if possible)
                "acl": ["public"],                                 # REQ list of strings: UUID(s) of users/groups allowed to access data, or ["public"]
                "mdf-publish.publication.collection": "Ti-O Fitting Database",  # RCM string: Collection the record belongs to
                "mdf_data_class": "vasp",                      # RCM string: Type of data in record
                "mdf-base.material_composition": data["frames"][0]["chemical_formula"],       # RCM string: Chemical composition of material in record

    #            "cite_as": ,                             # OPT list of strings: Complete citation(s) for this record (if different from dataset)
    #            "license": ,                             # OPT string: License to use the record (if different from dataset) (preferrably a link to the actual license).

                "dc.title": "Ti-O Fitting Database - " + data["frames"][0]["chemical_formula"],                            # REQ string: Title of record
    #            "dc.creator": ,                          # OPT string: Owner of record (if different from dataset)
                 "dc.identifier": uri,                       # RCM string: Link to record (record webpage, if available)
    #            "dc.contributor.author": ,               # OPT list of strings: Author(s) of record (if different from dataset)
    #            "dc.subject": ,                          # OPT list of strings: Keywords about record
    #            "dc.description": ,                      # OPT string: Description of record
    #            "dc.relatedidentifier": ,                # OPT list of strings: Link(s) to related materials (if different from dataset)
    #            "dc.year": ,                             # OPT integer: Year of record creation (if different from dataset)

                "data": {                                # REQ dictionary: Other record data (described below)
    #                "raw": ,                             # RCM string: Original data record text, if feasible
                    "files": {"outcar": uri}                            # RCM dictionary: {file_type : uri_to_file} pairs, data files (Example: {"cif" : "https://example.org/cifs/data_file.cif"})
                    }
                }

            # Pass each individual record to the Validator
            result = dataset_validator.write_record(record_metadata)

            # Check if the Validator accepted the record, and print a message if it didn't
            # If the Validator returns "success" == True, the record was written successfully
            if result["success"] is not True:
                print("Error:", result["message"], ":", result.get("invalid_metadata", ""))
        except Exception:
            print(data_file["path"] + "/" + data_file["filename"])


    # TODO: Save your converter as [mdf_source_name]_converter.py
    # You're done!
    if verbose:
        print("Finished converting")


# Optionally, you can have a default call here for testing
# The convert function may not be called in this way, so code here is primarily for testing
if __name__ == "__main__":
    import paths
    convert(paths.datasets+"ti_o_fitting_db", True)
