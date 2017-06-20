import os
import json
from tqdm import tqdm
from parsers.utils import find_files
from validator import Validator


# This is the converter for the NIST Interatomic Potentials dataset.
# Arguments:
#   input_path (string): The file or directory where the data resides. This should not be hard-coded in the function, for portability.
#   verbose (bool): Should the script print status messages to standard output? Default False.
def convert(input_path, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    dataset_metadata = {
        "globus_subject": "https://www.ctcms.nist.gov/potentials/",
        "acl": ["public"],
        "mdf_source_name": "nist_ip",
        "mdf-publish.publication.collection": "NIST Interatomic Potentials",

        "cite_as": ['C.A. Becker, et al., "Considerations for choosing and using force fields and interatomic potentials in materials science and engineering," Current Opinion in Solid State and Materials Science, 17, 277-283 (2013). https://www.ctcms.nist.gov/potentials'],
#        "license": ,

        "dc.title": "NIST Interatomic Potentials Repository Project",
        "dc.creator": "National Institute of Standards and Technology",
        "dc.identifier": "https://www.ctcms.nist.gov/potentials/",
        "dc.contributor.author": ["C.A. Becker, et al."],
        "dc.subject": ["interatomic potential", "forcefield"],
        "dc.description": "This repository provides a source for interatomic potentials (force fields), related files, and evaluation tools to help researchers obtain interatomic models and judge their quality and applicability.",
#        "dc.relatedidentifier": ,
        "dc.year": 2013
        }


    # Make a Validator to help write the feedstock
    # You must pass the metadata to the constructor
    # Each Validator instance can only be used for a single dataset
    dataset_validator = Validator(dataset_metadata)


    # Get the data
    # Each record also needs its own metadata
    for file_data in tqdm(find_files(input_path, "\.json$"), desc="Processing files", disable= not verbose):
        try:
            with open(os.path.join(file_data["path"], file_data["filename"]), 'r') as ip_file:
                ip_data = json.load(ip_file)["interatomic-potential"]
            if not ip_data:
                raise ValueError("No data in file")
        except Exception as e:
            if verbose:
                print("Error reading '" + os.path.join(file_data["path"], file_data["filename"]) + "'")
            continue
        url_list = []
        link_texts = []
        for artifact in ip_data["implementation"]:
            for web_link in artifact["artifact"]:
                url = web_link.get("web-link", {}).get("URL", None)
                if url:
                    url_list.append(url)
                link_text = web_link.get("web-link", {}).get("link-text", None)
                if link_text:
                    link_texts.append(link_text)

        record_metadata = {
            "globus_subject": ip_data["id"],
            "acl": ["public"],
            "mdf-publish.publication.collection": "NIST Interatomic Potentials",
#            "mdf_data_class": ,
#            "mdf-base.material_composition": "".join(ip_data["element"]),

#            "cite_as": ,
#            "license": ,

            "dc.title": "NIST Interatomic Potential - " + ", ".join(link_texts),
#            "dc.creator": ,
            "dc.identifier": ip_data["id"],
#            "dc.contributor.author": ,
#            "dc.subject": ,
#            "dc.description": "; ".join(ip_data["description"]["notes"]),
#            "dc.relatedidentifier": url_list,
#            "dc.year": ,

            "data": {
                "raw": json.dumps(ip_data),
#                "files": ,
                }
            }
        if ip_data["element"]:
            record_metadata["mdf-base.material_composition"] = "".join(ip_data["element"])
        if url_list:
            record_metadata["dc.relatedidentifier"] = url_list

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
    convert(paths.datasets+"nist_ip", True)
