import json
import sys
import os
from tqdm import tqdm
from parsers.utils import find_files
from parsers.ase_parser import parse_ase
from validator import Validator

# VERSION 0.1.0

# This is the converter for the GDB7-13(QM7b) dataset: Machine learning of molecular electronic properties in chemical compound space
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
            "globus_subject": "http://qmml.org/datasets.html" + "#gdb7-13",
            "acl": ["public"],
            "mdf_source_name": "gdb7-13",
            "mdf-publish.publication.collection": "gdb7-13",
            "mdf_data_class": "xyz",

            "cite_as": ["Gr\'egoire Montavon, Matthias Rupp, Vivekanand Gobre, Alvaro Vazquez-Mayagoitia, Katja Hansen, Alexandre Tkatchenko, Klaus-Robert M\"uller, O. Anatole von Lilienfeld: Machine learning of molecular electronic properties in chemical compound space, New Journal of Physics, 15(9): 095003, IOP Publishing, 2013.DOI: 10.1088/1367-2630/15/9/095003"],
            "license": "https://creativecommons.org/licenses/by/3.0/",
            "mdf_version": "0.1.0",

            "dc.title": "Machine learning of molecular electronic properties in chemical compound space",
            "dc.creator": "Argonne National Laboratory, Einstein Foundation, National Research Foundation of Korea",
            "dc.identifier": "http://qmml.org/datasets.html" + "#gdb7-13",
            "dc.contributor.author": ["Grégoire Montavon", "Matthias Rupp", "Vivekanand Gobre", "Alvaro Vazquez-Mayagoitia", "Katja Hansen", "Alexandre Tkatchenko", "Klaus-Robert Müller", "O. Anatole von Lilienfeld"],
#            "dc.subject": ,
            "dc.description": "7k small organic molecules, in their ground state, 14 combinations of properties and theory levels. 7,211 small organic molecules composed of H, C, N, O, S, Cl, saturated with H, and up to 7 non-H atoms. Molecules relaxed using DFT with PBE functional. Properties are atomization energy (DFT/PBE0), averaged polarizability (DFT/PBE0, SCS), HOMO and LUMO eigenvalues (GW, DFT/PBE0, ZINDO), and, ionization potential, electron affinity, first excitation energy, frequency of maximal absorption (all ZINDO).",                      # RCM string: Description of dataset contents
            "dc.relatedidentifier": ["https://doi.org/10.1088/1367-2630/15/9/095003"],
            "dc.year": 2013
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

    # Each record also needs its own metadata
    for data_file in tqdm(find_files(input_path, "xyz"), desc="Processing files", disable=not verbose):
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "xyz")
        uri = "https://data.materialsdatafacility.org/collections/" + "gdb7-13/gdb7-13_data/" + data_file["filename"]
        record_metadata = {
            "globus_subject": uri,
            "acl": ["public"],
#            "mdf-publish.publication.collection": ,
#            "mdf_data_class": ,
            "mdf-base.material_composition": record["chemical_formula"],

#            "cite_as": ,
#            "license": ,

            "dc.title": "gdb7-13 " + data_file["filename"],
#            "dc.creator": ,
#            "dc.identifier": ,
#            "dc.contributor.author": ,
#            "dc.subject": ,
#            "dc.description": ,
#            "dc.relatedidentifier": ,
#            "dc.year": ,

            "data": {
#                "raw": ,
                "files": {"xyz": uri},
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
    convert(paths.datasets + "gdb7-13/gdb7-13_data/", verbose=True)
