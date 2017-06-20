import json
import os
import sys
from parsers.utils import find_files
from parsers.ase_parser import parse_ase
from validator import Validator

# VERSION 0.1.0

# This is the template for the Cp Complexes dataset.
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
            "globus_subject": "https://figshare.com/articles/Synthesis_Characterization_and_Some_Properties_of_Cp_W_NO_H_sup_3_sup_allyl_Complexes/2158483",                      # REQ string: Unique value (should be URI if possible)
            "acl": ["public"],                                 # REQ list of strings: UUID(s) of users/groups allowed to access data, or ["public"]
            "mdf_source_name": "cp_complexes",                     # REQ string: Unique name for dataset
            "mdf-publish.publication.collection": "Cp*W(NO)(H)(η3‑allyl) Complexes",  # RCM string: Collection the dataset belongs to
            "mdf_data_class": "CIF",                      # RCM string: Type of data in all records in the dataset (do not provide for multi-type datasets)

            "cite_as": ["Baillie, Rhett A.; Holmes, Aaron S.; Lefèvre, Guillaume P.; Patrick, Brian O.; Shree, Monica V.; Wakeham, Russell J.; Legzdins, Peter; Rosenfeld, Devon C. (2015): Synthesis, Characterization, and Some Properties of Cp*W(NO)(H)(η3‑allyl) Complexes. ACS Publications. https://doi.org/10.1021/acs.inorgchem.5b00747.s002"],                             # REQ list of strings: Complete citation(s) for this dataset.
            "license": "https://creativecommons.org/licenses/by-nc/4.0/",                             # RCM string: License to use the dataset (preferrably a link to the actual license).
            "mdf_version": "0.1.0",                         # REQ string: The metadata version in use (see VERSION above).

            "dc.title": "Synthesis, Characterization, and Some Properties of Cp*W(NO)(H)(η3‑allyl) Complexes",                            # REQ string: Title of dataset
            "dc.creator": "The University of British Columbia, The Dow Chemical Company",                          # REQ string: Owner of dataset
            "dc.identifier": "https://figshare.com/articles/Synthesis_Characterization_and_Some_Properties_of_Cp_W_NO_H_sup_3_sup_allyl_Complexes/2158483",                       # REQ string: Link to dataset (dataset DOI if available)
            "dc.contributor.author": ["Baillie, Rhett A.", "Holmes, Aaron S.", "Lefèvre, Guillaume P.", "Patrick, Brian O.", "Shree, Monica V.", "Wakeham, Russell J.", "Legzdins, Peter", "Rosenfeld, Devon C."],               # RCM list of strings: Author(s) of dataset
            "dc.subject": ["THF", "DFT", "18 e PMe 3 adducts", "complex", "coordination isomers", "magnesium allyl reagent"],                          # RCM list of strings: Keywords about dataset
            "dc.description": "Sequential treatment at low temperatures of Cp*W­(NO)­Cl2 in THF with 1 equiv of a binary magnesium allyl reagent, followed by an excess of LiBH4, affords three new Cp*W­(NO)­(H)­(η3-allyl) complexes, namely, Cp*W­(NO)­(H)­(η3-CH2CHCMe2) (1), Cp*W­(NO)­(H)­(η3-CH2CHCHPh) (2), and Cp*W­(NO)­(H)­(η3-CH2CHCHMe) (3).",                      # RCM string: Description of dataset contents
            "dc.relatedidentifier": ["https://doi.org/10.1021/acs.inorgchem.5b00747"],                # RCM list of strings: Link(s) to related materials (such as an article)
            "dc.year": 2015                              # RCM integer: Year of dataset creation
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
#    dataset_validator = Validator(dataset_metadata, strict=False)
    # You can also force the Validator to treat warnings as errors with strict=True
    dataset_validator = Validator(dataset_metadata, strict=True)


    # Get the data
    # TODO: Write the code to convert your dataset's records into JSON-serializable Python dictionaries
    #    Each record should be exactly one dictionary
    #    It is recommended that you convert your records one at a time, but it is possible to put them all into one big list (see below)
    #    It is also recommended that you use a parser to help with this process if one is available for your datatype
    for file_data in find_files(input_path, ".cif"):
        record = parse_ase(os.path.join(file_data["path"], file_data["filename"]), data_format="cif")

        # TODO: Fill in these dictionary fields for each record
        # Fields can be:
        #    REQ (Required, must be present)
        #    RCM (Recommended, should be present if possible)
        #    OPT (Optional, can be present if useful)
        record_metadata = {
            "globus_subject": "https://figshare.com/articles/Synthesis_Characterization_and_Some_Properties_of_Cp_W_NO_H_sup_3_sup_allyl_Complexes/2158483#" + record["chemical_formula"],                      # REQ string: Unique value (should be URI to record if possible)
            "acl": ["public"],                                 # REQ list of strings: UUID(s) of users/groups allowed to access data, or ["public"]
#            "mdf-publish.publication.collection": ,  # OPT string: Collection the record belongs to (if different from dataset)
#            "mdf_data_class": ,                      # OPT string: Type of data in record (if not set in dataset metadata)
            "mdf-base.material_composition": record["chemical_formula"],       # RCM string: Chemical composition of material in record

#            "cite_as": ,                             # OPT list of strings: Complete citation(s) for this record (if different from dataset)
#            "license": ,                             # OPT string: License to use the record (if different from dataset) (preferrably a link to the actual license).

            "dc.title": "Cp Complexes - " + record["chemical_formula"],                            # REQ string: Title of record
#            "dc.creator": ,                          # OPT string: Owner of record (if different from dataset)
#            "dc.identifier": ,                       # RCM string: Link to record (record webpage, if available)
#            "dc.contributor.author": ,               # OPT list of strings: Author(s) of record (if different from dataset)
#            "dc.subject": ,                          # OPT list of strings: Keywords about record
#            "dc.description": ,                      # OPT string: Description of record
#            "dc.relatedidentifier": ,                # OPT list of strings: Link(s) to related materials (if different from dataset)
#            "dc.year": ,                             # OPT integer: Year of record creation (if different from dataset)

#            "data": {                                # RCM dictionary: Other record data (described below)
#                "raw": json.dumps(record),                             # RCM string: Original data record text, if feasible
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
    convert(paths.datasets+"cp_complexes", verbose=True)
