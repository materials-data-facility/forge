import json
import sys
import os
from tqdm import tqdm
from parsers.tab_parser import parse_tab
from validator import Validator

# VERSION 0.1.0

# This is the converter for the gdb8-15 dataset: Electronic Spectra from TDDFT and Machine Learning in Chemical Space
# Arguments:
#   input_path (string): The file or directory where the data resides. This should not be hard-coded in the function, for portability.
#   metadata (string or dict): The path to the JSON dataset metadata file, a dict or json.dumps string containing the dataset metadata, or None to specify the metadata here. Default None.
#   verbose (bool): Should the script print status messages to standard output? Default False.
#       NOTE: The converter should have NO output if verbose is False, unless there is an error.
def convert(input_path, metadata=None, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    if not metadata:
        dataset_metadata = {
            "globus_subject": "http://qmml.org/datasets.html#gdb8-15",
            "acl": ["public"],
            "mdf_source_name": "gdb8-15",
            "mdf-publish.publication.collection": "gdb8-15",
            "mdf_data_class": "txt",

            "cite_as": ["Electronic spectra of 22k molecules Raghunathan Ramakrishnan, Mia Hartmann, Enrico Tapavicza, O. Anatole von Lilienfeld, J. Chem. Phys. submitted (2015)", "Structures of 22k molecules Raghunathan Ramakrishnan, Pavlo Dral, Matthias Rupp, O. Anatole von Lilienfeld Scientific Data 1, Article number: 140022 (2014). doi:10.1038/sdata.2014.22"],
#            "license": ,
            "mdf_version": "0.1.0",

            "dc.title": "Electronic spectra from TDDFT and machine learning in chemical space",
            "dc.creator": "University of Basel, California State University, Argonnne National Labratory",
            "dc.identifier": "http://aip.scitation.org/doi/suppl/10.1063/1.4928757",
            "dc.contributor.author": ["Raghunathan Ramakrishnan", "Mia Hartmann", "Enrico Tapavicza", "O. Anatole von Lilienfeld"],
            "dc.subject": ["Density functional theory, Excitation energies, Computer modeling, Oscillators, Molecular spectra"],
            "dc.description": "Due to its favorable computational efficiency, time-dependent (TD) density functional theory (DFT) enables the prediction of electronic spectra in a high-throughput manner across chemical space. Its predictions, however, can be quite inaccurate. We resolve this issue with machine learning models trained on deviations of reference second-order approximate coupled-cluster (CC2) singles and doubles spectra from TDDFT counterparts, or even from DFT gap. We applied this approach to low-lying singlet-singlet vertical electronic spectra of over 20 000 synthetically feasible small organic molecules with up to eight CONF atoms.",
            "dc.relatedidentifier": ["http://dx.doi.org/10.1063/1.4928757http://dx.doi.org/10.1063/1.4928757"],
            "dc.year": 2015
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
    #dataset_validator = Validator(dataset_metadata, strict=False)
    # You can also force the Validator to treat warnings as errors with strict=True
    dataset_validator = Validator(dataset_metadata, strict=True)


    # Get the data
    #    Each record should be exactly one dictionary
    #    It is recommended that you convert your records one at a time, but it is possible to put them all into one big list (see below)
    #    It is also recommended that you use a parser to help with this process if one is available for your datatype
    headers = ["Index", "E1-CC2", "E2-CC2", "f1-CC2", "f2-CC2", "E1-PBE0", "E2-PBE0", "f1-PBE0", "f2-PBE0", "E1-PBE0", "E2-PBE0", "f1-PBE0", "f2-PBE0", "E1-CAM", "E2-CAM", "f1-CAM", "f2-CAM"]
    # Each record also needs its own metadata
    with open(input_path, 'r') as raw_in:
        data = raw_in.read()
    #Start at line 29 for data
    starter = data.find("       1      0.43295186     0.43295958")
    #Remove the spaces before the index column
    decomp = data[starter:].split("\n")
    stripped_decomp = []
    for line in decomp:
        stripped_decomp.append(line.strip())
    #Open gdb9-14 feedstock to get chemical composition
    with open(os.path.join(paths.feedstock, "gdb9-14_all.json"), 'r') as json_file:
        lines = json_file.readlines()
        full_json_data = [json.loads(line) for line in lines]
        #Composition needed doesn't begin until after record 6095
        json_data = full_json_data[6095:]
    for record in tqdm(parse_tab("\n".join(stripped_decomp), headers=headers, sep="     "), desc="Processing files", disable=not verbose):
        comp = json_data[int(record["Index"])]["mdf-base.material_composition"]
        uri = "https://data.materialsdatafacility.org/collections/gdb-8-15/gdb8_22k_elec_spec.txt#" + record["Index"]
        record_metadata = {
            "globus_subject": uri,
            "acl": ["public"],
#            "mdf-publish.publication.collection": ,
#            "mdf_data_class": ,
            "mdf-base.material_composition": comp,

#            "cite_as": ,
#            "license": ,

            "dc.title": "gdb8-15 - " + "record: " + record["Index"],
#            "dc.creator": ,
            "dc.identifier": uri,
#            "dc.contributor.author": ,
#            "dc.subject": ,
#            "dc.description": ,
#            "dc.relatedidentifier": ,
#            "dc.year": ,

            "data": {
                "raw": json.dumps(record),
#                "files": ,
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

    # Alternatively, if the only way you can process your data is in one large list, you can pass the list to the Validator
    # You still must add the required metadata to your records
    # It is recommended to use the previous method if possible
    # result = dataset_validator.write_dataset(your_records_with_metadata)
    #if result["success"] is not True:
        #print("Error:", result["message"])

    # You're done!
    if verbose:
        print("Finished converting")


# Optionally, you can have a default call here for testing
# The convert function may not be called in this way, so code here is primarily for testing
if __name__ == "__main__":
    import paths
    convert(paths.datasets + "gdb8-15/gdb8_22k_elec_spec.txt", verbose=True)
