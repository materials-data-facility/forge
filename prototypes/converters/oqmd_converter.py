import json
import sys
import os
from tqdm import tqdm
from validator import Validator


# This is the converter for the OQMD 
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
            "globus_subject": "http://oqmd.org/",
            "acl": ["public"],
            "mdf_source_name": "oqmd",
            "mdf-publish.publication.collection": "Open Quantum Materials Database",
            "mdf_data_class": "vasp",

            "cite_as": ['Saal, J. E., Kirklin, S., Aykol, M., Meredig, B., and Wolverton, C. "Materials Design and Discovery with High-Throughput Density Functional Theory: The Open Quantum Materials Database (OQMD)", JOM 65, 1501-1509 (2013). doi:10.1007/s11837-013-0755-4', 'Kirklin, S., Saal, J.E., Meredig, B., Thompson, A., Doak, J.W., Aykol, M., Rühl, S. and Wolverton, C. "The Open Quantum Materials Database (OQMD): assessing the accuracy of DFT formation energies", npj Computational Materials 1, 15010 (2015). doi:10.1038/npjcompumats.2015.10'],

            "dc.title": "Open Quantum Materials Database",
            "dc.creator": "Northwestern University",
            "dc.identifier": "http://oqmd.org/",
            "dc.contributor.author": ["Kirklin, S.", "Saal, J.E.", "Meredig, B.", "Thompson, A.", "Doak, J.W.", "Aykol, M.", "Rühl, S.", "Wolverton, C."],
            "dc.subject": ["dft"],
            "dc.description": "The OQMD is a database of DFT-calculated thermodynamic and structural properties.",
            "dc.relatedidentifier": ["http://dx.doi.org/10.1007/s11837-013-0755-4", "http://dx.doi.org/10.1038/npjcompumats.2015.10"],
            "dc.year": 2013
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
                "globus_subject": record["url"],
                "acl": ["public"],
#                "mdf-publish.publication.collection": ,
#                "mdf_data_class": ,
                "mdf-base.material_composition": record["composition"],

#                "cite_as": ,
#                "license": ,

                "dc.title": "OQMD - " + record["composition"],
#                "dc.creator": ,
                "dc.identifier": record["url"],
#                "dc.contributor.author": ,
#                "dc.subject": ,
#                "dc.description": ,
#                "dc.relatedidentifier": ,
#                "dc.year": ,

                "data": {
#                    "raw": ,
                    "files": {
                        "metadata": "https://data.materialsdatafacility.org" + metadata_path,
                        "outcar": "https://data.materialsdatafacility.org" + outcar_path + "/OUTCAR.gz"
                        },
                    "band gap": record["band_gap"],
                    "configuration": record["configuration"],
                    "converged": record["converged"]
                    }
                }
            # Enter values that may be missing
            if record.get("magnetic moment", None):
                record_metadata["data"]["magnetic moment"] = record["magnetic moment"]
            if record.get("stability_data", None):
                record_metadata["data"]["stability_data"] = record["stability_data"]
            if record.get("total energy", None):
                record_metadata["data"]["total energy"] = record["total energy"]



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
    convert(paths.datasets+"oqmd/", None, True)
