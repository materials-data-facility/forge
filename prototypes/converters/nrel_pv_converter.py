import json
import sys
from tqdm import tqdm
from parsers.tab_parser import parse_tab
from validator import Validator


# This is the converter for the NREL Photovoltaic dataset
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
            "globus_subject": "https://organicelectronics.nrel.gov",
            "acl": ["public"],
            "mdf_source_name": "nrel_pv",
            "mdf-publish.publication.collection": "NREL Organic Photovoltaic Database",
            "mdf_data_class": "csv",

            "cite_as": ["Gaussian 09, (Revisions B.01, C.01 and D.01), M. J. Frisch, et al., Gaussian, Inc., Wallingford CT, 2009. See gaussian.com", "Ross E. Larsen, J. Phys. Chem. C, 120, 9650-9660 (2016). DOI: 10.1021/acs .jpcc.6b02138"],
#            "license": ,

            "dc.title": "National Renewable Energy Laboratory Organic Photovoltaic Database",
            "dc.creator": "NREL",
            "dc.identifier": "https://organicelectronics.nrel.gov",
            "dc.contributor.author": ["Ross Larsen", "Dana Olson", "Nikos Kopidakis", "Zbyslaw Owczarczyk", "Scott Hammond", "Peter Graf", "Travis Kemper", "Scott Sides", "Kristin Munch", "David Evenson", "Craig Swank"],
#            "dc.subject": ,
            "dc.description": "Welcome to the National Renewable Energy Laboratory materials discovery database for organic electronic materials. The focus is on materials for organic photovoltaic (OPV) absorber materials but materials suitable for other applications may be found here as well.",
            "dc.relatedidentifier": ["https://dx.doi.org/10.1021/acs.jpcc.6b02138"]
#            "dc.year": 
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
    dataset_validator = Validator(dataset_metadata)


    # Get the data
    # Each record also needs its own metadata
    with open(input_path, 'r') as raw_in:
        for record in tqdm(parse_tab(raw_in.read()), desc="Processing files", disable= not verbose):
            record_metadata = {
                "globus_subject": record["URL"],
                "acl": ["public"],
#               "mdf-publish.publication.collection": ,
#                "mdf_data_class": "csv",
                "mdf-base.material_composition": record["common_tag"],

#                "cite_as": ,
#                "license": ,

                "dc.title": "NREL OPV - " + record["common_tag"],
#                "dc.creator": ,
                "dc.identifier": record["URL"],
#                "dc.contributor.author": ,
#                "dc.subject": ,
#                "dc.description": ,
#                "dc.relatedidentifier": ,
#                "dc.year": ,

                "data": {
                    "raw": json.dumps(record)
#                    "files": 
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
    convert(paths.datasets+"nrel_pv/polymer_export_0501179397151.csv", verbose=True)
