import os
from tqdm import tqdm
from validator import Validator
from parsers.utils import find_files
from parsers.ase_parser import parse_ase

# This is the converter for the High-throughput Ab-initio Dilute Solute Diffusion Database dataset from Dane Morgan's group 
# Arguments:
#   input_path (string): The file or directory where the data resides. This should not be hard-coded in the function, for portability.
#   verbose (bool): Should the script print status messages to standard output? Default False.
def convert(input_path, verbose=False):

    # Collect the metadata
    dataset_metadata = {
        "globus_subject": "https://publish.globus.org/jspui/handle/ITEM/164",
        "acl": ["public"],
        "mdf_source_name": "ab_initio_solute_database",
        "mdf-publish.publication.collection": "High-throughput Ab-initio Dilute Solute Diffusion Database",

        "dc.title": "High-throughput Ab-initio Dilute Solute Diffusion Database",
        "dc.creator": "Materials Data Facility",
        "dc.identifier": "http://dx.doi.org/doi:10.18126/M2X59R",
        "dc.contributor.author": ["Wu, Henry", "Mayeshiba, Tam", "Morgan, Dane,"],
        "dc.subject": ["dilute", "solute", "DFT", "diffusion"],
        "dc.description": "We demonstrate automated generation of diffusion databases from high-throughput density functional theory (DFT) calculations. A total of more than 230 dilute solute diffusion systems in Mg, Al, Cu, Ni, Pd, and Pt host lattices have been determined using multi-frequency diffusion models. We apply a correction method for solute diffusion in alloys using experimental and simulated values of host self-diffusivity.",
        "dc.relatedidentifier": ["http://dx.doi.org/10.1038/sdata.2016.54", "http://dx.doi.org/10.6084/m9.figshare.1546772"],
        "dc.year": 2016
        }


    # Make a Validator to help write the feedstock
    # You must pass the metadata to the constructor
    # Each Validator instance can only be used for a single dataset
    dataset_validator = Validator(dataset_metadata)


    # Get the data
    #    Each record should be exactly one dictionary
    #    It is recommended that you convert your records one at a time, but it is possible to put them all into one big list (see below)
    #    It is also recommended that you use a parser to help with this process if one is available for your datatype

    # Each record also needs its own metadata
    for dir_data in tqdm(find_files(root=input_path, file_pattern="^OUTCAR$", keep_dir_name_depth=3), desc="Processing data files", disable= not verbose):
        file_data = parse_ase(file_path=os.path.join(dir_data["path"], dir_data["filename"] + dir_data["extension"]), data_format="vasp", verbose=False)

        uri = "globus://82f1b5c6-6e9b-11e5-ba47-22000b92c6ec/published/publication_164/data/"
        for dir_name in dir_data["dirs"]:
            uri = os.path.join(uri, dir_name)
        record_metadata = {
            "globus_subject": uri,
            "acl": ["public"],
            "mdf-publish.publication.collection": "High-throughput Ab-initio Dilute Solute Diffusion Database",
            "mdf_data_class": "vasp",
            "mdf-base.material_composition": file_data["frames"][0]["chemical_formula"],

            "dc.title": "High-throughput Ab-initio Dilute Solute Diffusion Database" + file_data["frames"][0]["chemical_formula"],
            #"dc.creator": ,
            "dc.identifier": uri,
            #"dc.contributor.author": ,
            #"dc.subject": ,
            #"dc.description": ,
            #"dc.relatedidentifier": ,
            #"dc.year": ,

            "data": {
                "raw": str(file_data),
                "files": {"outcar": uri}
                }
            }

        # Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)

        # Check if the Validator accepted the record, and print a message if it didn't
        # If the Validator returns "success" == True, the record was written successfully
        if result["success"] is not True:
            print("Error:", result["message"], ":", result["invalid_data"])

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
    print("Begin conversion")
    convert(paths.datasets + "dane_morgan/data", True)
