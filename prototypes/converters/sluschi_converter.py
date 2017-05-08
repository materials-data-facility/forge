import os
from tqdm import tqdm
from validator import Validator
from parsers.utils import find_files
from parsers.ase_parser import parse_ase


# This is the converter for SLUSCHI data.
# Arguments:
#   input_path (string): The file or directory where the data resides. This should not be hard-coded in the function, for portability.
#   verbose (bool): Should the script print status messages to standard output? Default False.
def convert(input_path, verbose=False):

    # Collect the metadata
    dataset_metadata = {
        "globus_subject": "http://blogs.brown.edu/qhong/?page_id=102",
        "acl": ["public"],
        "mdf_source_name": "sluschi",
        "mdf-publish.publication.collection": "SLUSCHI",
        "mdf_data_class": "vasp",

        "cite_as": ["Qi-Jun Hong, Axel van de Walle, A user guide for SLUSCHI: Solid and Liquid in Ultra Small Coexistence with Hovering Interfaces, Calphad, Volume 52, March 2016, Pages 88-97, ISSN 0364-5916, http://doi.org/10.1016/j.calphad.2015.12.003."],
        "dc.title": "Solid and Liquid in Ultra Small Coexistence with Hovering Interfaces",
        "dc.creator": "Brown University",
        "dc.identifier": "http://doi.org/10.1016/j.calphad.2015.12.003",
        "dc.contributor.author": ["Qi-Jun Hong", "Axel van de Walle"],
        "dc.subject": ["Melting temperature calculation", "Density functional theory", "Automated code"],
        "dc.description": "Although various approaches for melting point calculations from first principles have been proposed and employed for years, their practical implementation has hitherto remained a complex and time-consuming process. The SLUSCHI code (Solid and Liquid in Ultra Small Coexistence with Hovering Interfaces) drastically simplifies this procedure into an automated package, by implementing the recently-developed small-size coexistence method and putting together a series of steps that lead to final melting point evaluation. Based on density functional theory, SLUSCHI employs Born–Oppenheimer molecular dynamics techniques under the isobaric–isothermal (NPT) ensemble, with interface to the first-principles code VASP.",
        "dc.relatedidentifier": ["http://blogs.brown.edu/qhong/?page_id=102"],
        "dc.year": 2015
        }


    # Make a Validator to help write the feedstock
    # You must pass the metadata to the constructor
    # Each Validator instance can only be used for a single dataset
    dataset_validator = Validator(dataset_metadata)

    # Each record also needs its own metadata
    for dir_data in tqdm(find_files(root=input_path, file_pattern="^OUTCAR$"), desc="Processing data files", disable= not verbose):
        file_data = parse_ase(file_path=os.path.join(dir_data["path"], dir_data["filename"]), data_format="vasp", verbose=False)

        # If no data, skip record
        if not file_data or not file_data["frames"]:
            continue

        uri = "globus:sluschi/" + dir_data["no_root_path"] + "/" + dir_data["filename"]
        record_metadata = {
            "globus_subject": uri,
            "acl": ["public"],
            "mdf-publish.publication.collection": "SLUSCHI",
            "mdf-base.material_composition": file_data["frames"][0]["chemical_formula"],

            "dc.title": "SLUSCHI - " + file_data["frames"][0]["chemical_formula"],
#            "dc.creator": ,
#            "dc.identifier": ,
#            "dc.contributor.author": ,
#            "dc.subject": ,
#            "dc.description": ,
#            "dc.relatedidentifier": ,
#            "dc.year": ,

            "data": {
#                "raw": str(file_data),
                "files": {"outcar": uri}
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
    print("Begin conversion")
    convert(paths.datasets + "sluschi/sluschi", True)
