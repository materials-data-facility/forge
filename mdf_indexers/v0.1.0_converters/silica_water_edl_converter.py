import json
import sys
import os
from tqdm import tqdm
from parsers.utils import find_files
from parsers.ase_parser import parse_ase
from validator import Validator

# VERSION 0.1.0

# This is the converter for the Dynamic behaviour of the silica-water-bio electrical double layer in the presence of a divalent electrolyte dataset
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
            "globus_subject": "https://eprints.soton.ac.uk/401018/",
            "acl": ["public"],
            "mdf_source_name": "silica_water_edl",
            "mdf-publish.publication.collection": "Silica Water EDL",
            "mdf_data_class": "xyz",

            "cite_as": ["Lowe, B.M., Maekawa, Y., Shibuta, Y., Sakata, T., Skylaris, C.-K. and Green, N.G. (2016) Dynamic Behaviour of the Silica-Water-Bio Electrical Double Layer in the Presence of a Divalent Electrolyte. Physical Chemistry Chemical Physics, https://doi.org/10.1039/C6CP04101A"],
            "license": "http://creativecommons.org/licenses/by/4.0/",
            "mdf_version": "0.1.0",

            "dc.title": "Dynamic behaviour of the silica-water-bio electrical double layer in the presence of a divalent electrolyte",
            "dc.creator": "University of Southampton",
            "dc.identifier": "http://dx.doi.org/10.5258/SOTON/401018",
            "dc.contributor.author": ["Lowe, Benjamin", "Mark, Maekawa", "Yuki, Shibuta", "Yasushi, Sakata", "Toshiya, Skylaris", "Chris-Kriton", "Green, Nicolas"],
            "dc.subject": ["BioFET", "BioFETs", "BioFED", "molecular dynamics", "MD"],
            "dc.description": "In this paper, explicit-solvent atomistic calculations of this electric field are presented and the structure and dynamics of the interface are investigated in different ionic strengths using molecular dynamics simulations. Novel results from simulation of the addition of DNA molecules and divalent ions are also presented, the latter of particular importance in both physiological solutions and biosensing experiments",
            "dc.relatedidentifier": ["http://eprints.soton.ac.uk/401017", "http://dx.doi.org/10.1039/C6CP04101A"],
            "dc.year": 2016
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
    for data_file in tqdm(find_files(input_path, "record-0.xyz"), desc="Processing files", disable=not verbose):
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "xyz")
        uri = "https://data.materialsdatafacility.org/collections/" + "silica_water_edl/" + data_file["no_root_path"] + "/" + data_file["filename"]
        record_metadata = {
            "globus_subject": uri,
            "acl": ["public"],
#            "mdf-publish.publication.collection": ,
#            "mdf_data_class": ,
            "mdf-base.material_composition": record["chemical_formula"],

#            "cite_as": ,
#            "license": ,

            "dc.title": "Silica Water EDL - " + data_file["filename"],
#            "dc.creator": ,
            "dc.identifier": uri,
#            "dc.contributor.author": ,
#            "dc.subject": ,
#            "dc.description": ,
#            "dc.relatedidentifier": ,
#            "dc.year": ,

            "data": {
#               "raw": ,
                "files": {"xyz": uri},
                "number_of_frames": 3000
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
    convert(paths.datasets + "silica_water_edl_data/separated_data/", verbose=True)
