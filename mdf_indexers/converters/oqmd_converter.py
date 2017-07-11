import json
import sys
import os

from tqdm import tqdm

from ..validator.schema_validator import Validator

# VERSION 0.3.0

# This is the converter for the OQMD
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
            "title": "The Open Quantum Materials Database",
            "acl": ["public"],
            "source_name": "oqmd",
            "citation": ['Saal, J. E., Kirklin, S., Aykol, M., Meredig, B., and Wolverton, C. "Materials Design and Discovery with High-Throughput Density Functional Theory: The Open Quantum Materials Database (OQMD)", JOM 65, 1501-1509 (2013). doi:10.1007/s11837-013-0755-4', 'Kirklin, S., Saal, J.E., Meredig, B., Thompson, A., Doak, J.W., Aykol, M., Rühl, S. and Wolverton, C. "The Open Quantum Materials Database (OQMD): assessing the accuracy of DFT formation energies", npj Computational Materials 1, 15010 (2015). doi:10.1038/npjcompumats.2015.10'],
            "data_contact": {

                "given_name": "Chris",
                "family_name": "Wolverton",

                "email": "oqmd.questions@gmail.com",
                "institution": "Northwestern University",
                },

            "author": [
                {
                "given_name": "Chris",
                "family_name": "Wolverton",
                "institution": "Northwestern University"
                },
                {
                "given_name": "Scott",
                "family_name": "Kirklin",
                "institution": "Northwestern University"
                },
                {
                "given_name": "Vinay",
                "family_name": "Hegde",
                "institution": "Northwestern University"
                },
                {
                "given_name": "Logan",
                "family_name": "Ward",
                "institution": "Northwestern University",
                "orcid": "https://orcid.org/0000-0002-1323-5939"
                }
                ],

#            "license": ,

            "collection": "OQMD",
            "tags": ["dft"],

            "description": "The OQMD is a database of DFT-calculated thermodynamic and structural properties.",
            "year": 2013,

            "links": {

                "landing_page": "http://oqmd.org/",

                "publication": ["http://dx.doi.org/10.1007/s11837-013-0755-4", "http://dx.doi.org/10.1038/npjcompumats.2015.10"],
#                "dataset_doi": ,

#                "related_id": ,

                # data links: {

                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #}
                },

#            "mrr": ,

            "data_contributor": [{
                "given_name": "Jonathon",
                "family_name": "Gaff",
                "email": "jgaff@uchicago.edu",
                "institution": "The University of Chicago",
                "github": "jgaff"
                }]
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
    with open(os.path.join(input_path, "metadata_lookup.csv"), 'r') as lookup_file:
        # Discard header line
        lookup_file.readline()
        lookup = {}
        for line in lookup_file:
            pair = line.split(",")
            if len(pair) > 1:
                lookup[pair[0]] = pair[1]

    for filename in tqdm(os.listdir(os.path.join(input_path, "metadata-files")), desc="Processing files", disable= not verbose):
        full_path = os.path.join(input_path, "metadata-files", filename)
        if os.path.isfile(full_path):
            with open(full_path, 'r') as data_file:
                record = json.load(data_file)
            metadata_path = lookup.get(filename.split(".")[0], "None").replace("//", "/").strip()
            outcar_path = os.path.join(os.path.dirname(metadata_path)) if metadata_path != "None" else "Unavailable"

            record_metadata = {
            "mdf": {
                "title": "OQMD - " + record["composition"],
                "acl": ["public"],

#                "tags": ,
#                "description": ,
                
                "composition": record["composition"],
#                "raw": ,

                "links": {
                    "landing_page": record["url"],

#                    "publication": ,
#                    "dataset_doi": ,

#                    "related_id": ,

                    "metadata": {

                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",

                        "path": metadata_path,
                        },
                    "outcar": {
                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",

                        "path": outcar_path,
                        }
                    },

#                "citation": ,
#                "data_contact": {

#                    "given_name": ,
#                    "family_name": ,

#                    "email": ,
#                    "institution":,

                    # IDs
#                },

#                "author": ,

#                "license": ,
#                "collection": ,
#                "data_format": ,
#                "data_type": ,
#                "year": ,

#                "mrr":

    #            "processing": ,
    #            "structure":,
                },
                "oqmd": {
                "band_gap": record["band gap"]["value"],
                "configuration": record["configuration"],
                "converged": record["converged"],
                "stability": record.get("stability_data", {}).get("stability", {}).get("value", None),
                "crossreference": record["entry"]["crossreference"],
                "magnetic_moment": record.get("magnetic moment", None),
                "total_energy": record.get("total energy", None)
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
