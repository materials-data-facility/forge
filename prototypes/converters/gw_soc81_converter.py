import json
import sys
from tqdm import tqdm
from parsers.tab_parser import parse_tab
from validator import Validator


# This is the converter for the GW100 dataset
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
            "globus_subject": "http://www.west-code.org/database/gwsoc81/index.php",                      # REQ string: Unique value (should be URI if possible)
            "acl": ["public"],                                 # REQ list of strings: UUID(s) of users/groups allowed to access data, or ["public"]
            "mdf_source_name": "gw_soc_81",                     # REQ string: Unique name for dataset
            "mdf-publish.publication.collection": "GW-SOC81",  # RCM string: Collection the dataset belongs to
#            "mdf_data_class": ,                      # RCM string: Type of data in all records in the dataset (do not provide for multi-type datasets)

            "cite_as": ["P. Scherpelz, M. Govoni, I. Hamada, and G. Galli, Implementation and Validation of Fully-Relativistic GW Calculations: Spin-Orbit Coupling in Molecules, Nanocrystals and Solids, J. Chem. Theory Comput. 12, 3523 (2016).", "P.J. Linstrom and W.G. Mallard, Eds., NIST Chemistry WebBook, NIST Standard Reference Database Number 69, National Institute of Standards and Technology, Gaithersburg MD, 20899, http://webbook.nist.gov."],                             # REQ list of strings: Complete citation(s) for this dataset.
#            "license": ,                             # RCM string: License to use the dataset (preferrably a link to the actual license).

            "dc.title": "Benchmark of G0W0 on 81 Molecules with Spin-Orbit Coupling",                            # REQ string: Title of dataset
            "dc.creator": "The University of Chicago, Argonne National Laboratory",                          # REQ string: Owner of dataset
            "dc.identifier": "http://www.west-code.org/database/gwsoc81/index.php",                       # REQ string: Link to dataset (dataset DOI if available)
#            "dc.contributor.author": ,               # RCM list of strings: Author(s) of dataset
#            "dc.subject": ,                          # RCM list of strings: Keywords about dataset
 #           "dc.description": ,                      # RCM string: Description of dataset contents
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
    with open(input_path) as in_file:
        data = in_file.read()
    for record in tqdm(parse_tab(data), desc="Processing records", disable= not verbose):
        link = "http://www.west-code.org/database/gwsoc81/pag/" + record["cas"] + ".php"
        record_metadata = {
            "globus_subject": link,                      # REQ string: Unique value (should be URI to record if possible)
            "acl": ["public"],                                 # REQ list of strings: UUID(s) of users/groups allowed to access data, or ["public"]
#            "mdf-publish.publication.collection": ,  # OPT string: Collection the record belongs to (if different from dataset)
#            "mdf_data_class": ,                      # OPT string: Type of data in record (if not set in dataset metadata)
            "mdf-base.material_composition": record["formula"],       # RCM string: Chemical composition of material in record

#            "cite_as": ,                             # OPT list of strings: Complete citation(s) for this record (if different from dataset)
#            "license": ,                             # OPT string: License to use the record (if different from dataset) (preferrably a link to the actual license).

            "dc.title": "GW-SOC81 - " + record["name"],                            # REQ string: Title of record
#            "dc.creator": ,                          # OPT string: Owner of record (if different from dataset)
            "dc.identifier": link,                       # RCM string: Link to record (record webpage, if available)
#            "dc.contributor.author": ,               # OPT list of strings: Author(s) of record (if different from dataset)
#            "dc.subject": ,                          # OPT list of strings: Keywords about record
#            "dc.description": ,                      # OPT string: Description of record
#            "dc.relatedidentifier": ,                # OPT list of strings: Link(s) to related materials (if different from dataset)
#            "dc.year": ,                             # OPT integer: Year of record creation (if different from dataset)

            "data": {                                # RCM dictionary: Other record data (described below)
#                "raw": ,                             # RCM string: Original data record text, if feasible
#                "files": ,                           # RCM dictionary: {file_type : uri_to_file} pairs, data files (Example: {"cif" : "https://example.org/cifs/data_file.cif"})
                "name": record["name"],
                "cas_number": record["cas"]
                # other                              # RCM any JSON-valid type: Any other data fields you would like to include go in the "data" dictionary. Keys will be prepended with 'mdf_source_name:'
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
    convert(paths.datasets+"west_code/gw_soc81.csv", verbose=True)
