from validator_copy import Validator
import example_parser


# Arguments:
#   input_path (string): The file or directory where the data resides. This should not be hard-coded in the function, for portability.
#   verbose (bool): Should the script print status messages to standard output? Default False.
def convert(input_path, verbose=False):

    # Collect the metadata
    # Fields can be:
    #    REQ (Required, must be present)
    #    RCM (Recommended, should be present if possible)
    #    OPT (Optional, can be present if useful)
    dataset_metadata = {
        "globus_subject": "https://materialsdatafacility.org/",                   # REQ string: Unique value (should be URI if possible)
        "acl": ["public"],                                                        # REQ list of strings: UUID(s) of users/groups allowed to access data, or ["public"]
        "mdf_source_name": "example_dataset",                                     # REQ string: Unique name for dataset
        "mdf-publish.publication.collection": "examples",                         # RCM string: Collection the dataset belongs to

        "dc.title": "MDF Example Dataset",                                        # REQ string: Title of dataset
        "dc.creator": "MDF",                                                      # REQ string: Creator of dataset
        "dc.identifier": "http://dx.doi.org/10.12345",                            # REQ string: Link to dataset (dataset DOI if available)
        "dc.contributor.author": ["Jonathon Gaff", "Ben Blaiszik"],               # RCM list of strings: Author(s) of dataset
        "dc.subject": ["example", "test", "converter"],                           # RCM list of strings: Keywords about dataset
        "dc.description": "This is an example dataset for an example converter",  # RCM string: Description of dataset contents
        "dc.relatedidentifier": ["https://www.globus.org"],                       # RCM list of strings: Link(s) to related materials (such as an article)
        "dc.year": 2017                                                           # RCM integer: Year of dataset creation
        }

    # Make a Validator to help write the feedstock
    # You must pass the metadata to the constructor
    # Each Validator instance can only be used for a single dataset
    dataset_validator = Validator(dataset_metadata)

    # Get the data
    #    Each record should be exactly one dictionary
    #    It is recommended that you convert your records one at a time, but it is possible to put them all into one big list (see below)
    #    It is also recommended that you use a parser to help with this process if one is available for your datatype
    raw_data = read_data()

    # Each record also needs its own metadata
    for raw_record in raw_data:
        # Using a parser when possible is recommended
        record = example_parser.parse_example_single(raw_record)

        # Fields can be:
        #    REQ (Required, must be present)
        #    RCM (Recommended, should be present if possible)
        #    OPT (Optional, can be present if useful)
        record_metadata = {
            "globus_subject": "https://materialsdatafacility.org/example/" + record["id"],  # REQ string: Unique value (should be URI to record if possible)
            "acl": ["public"],                                                              # REQ list of strings: UUID(s) of users/groups allowed to access data, or ["public"]
            "mdf-publish.publication.collection": "examples",                               # RCM string: Collection the record belongs to
            "mdf_data_class": "text",                                                       # RCM string: Type of data in record
            "mdf-base.material_composition": record["chemical_composition"],                # RCM string: Chemical composition of material in record

            "dc.title": "MDF Example - " + record["chemical_composition"],                  # REQ string: Title of record
            #"dc.creator": ,                                                                # OPT string: Owner of record (if different from dataset)
            "dc.identifier": "https://materialsdatafacility.org/example/" + record["id"],   # RCM string: Link to record (record webpage, if available)
            #"dc.contributor.author": ,                                                     # OPT list of strings: Author(s) of record (if different from dataset)
            #"dc.subject": ,                                                                # OPT list of strings: Keywords about record
            "dc.description": "This is an example record",                                  # OPT string: Description of record
            #"dc.relatedidentifier": ,                                                      # OPT list of strings: Link(s) to related materials (if different from dataset)
            #"dc.year": ,                                                                   # OPT integer: Year of record creation (if different from dataset)

            "data": {                                                                       # REQ dictionary: Other record data (described below)
                "raw": str(raw_record),                                                     # RCM string: Original data record text, if feasible
                "files": {"text": "https://materialsdatafacility.org/robots.txt"},          # REQ dictionary: {file_type : uri_to_file} pairs, may be empty (Example: {"cif" : "https://example.org/cifs/data_file.cif"})

                # other                                                                     # RCM any JSON-valid type: Any other data fields you would like to include go in the "data" dictionary. Keys will be prepended with mdf_source_name:
                "useful_data": [record["useful_data_1"], record["useful_data_2"]],
                "other_useful_data": record["useful_data_3"]
                }
            }
        # Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)

        # Check if the Validator accepted the record, and print a message if it didn't
        # If the Validator returns "success" == True, the record was written successfully
        if result["success"] is not True:
            print("Error:", result["message"], ":", result.get("invalid_data", ""))

    # Alternatively, if the only way you can process your data is in one large list, you can pass the list to the Validator
    # You still must add the required metadata to your records
    # It is recommended to use the previous method if possible
    # result = dataset_validator.write_dataset(your_records_with_metadata)
    #if result["success"] is not True:
        #print("Error:", result["message"])

    # TODO: Save your converter as [dataset_name]_converter.py
    # You're done!
    if verbose:
        print("Finished converting")


def read_data():
    """Dummy function as an example"""
    return range(10)


# Optionally, you can have a default call here for testing
# The convert function may not be called in this way, so code here is primarily for testing
if __name__ == "__main__":
    convert(input_path="")
