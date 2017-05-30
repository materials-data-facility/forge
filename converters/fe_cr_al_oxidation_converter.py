import json
import sys
import os
from tqdm import tqdm
from parsers.utils import find_files
from parsers.tab_parser import parse_tab
from validator import Validator


# This is the converter for the Fe-Cr-Al Oxidation Studies dataset
# Arguments:
#   input_path (string): The file or directory where the data resides. This should not be hard-coded in the function, for portability.
#   metadata (string or dict): The path to the JSON dataset metadata file, a dict containing the dataset metadata, or None to specify the metadata here. Default None.
#   verbose (bool): Should the script print status messages to standard output? Default False.
#       NOTE: The converter should have NO output if verbose is False, unless there is an error.
def convert(input_path, metadata=None, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    if not metadata:
        dataset_metadata = {
            "globus_subject": "http://hdl.handle.net/11256/836",
            "acl": ["public"],
            "mdf_source_name": "fe_cr_al_oxidation",
            "mdf-publish.publication.collection": "Fe-Cr-Al Oxidation Studies",
#            "mdf_data_class": ,

            "cite_as": ["Bunn, Jonathan K.; Fang, Randy L.; Albing, Mark R.; Mehta, Apurva; Kramer, Matt J.; Besser, Matt F.; Hattrick-Simpers, Jason R High-throughput Diffraction and Spectroscopic Data for Fe-Cr-Al Oxidation Studies (2015-06-28)"],
            "license": "http://creativecommons.org/licenses/by-sa/3.0/us/",

            "dc.title": "High-throughput Diffraction and Spectroscopic Data for Fe-Cr-Al Oxidation Studies",
            "dc.creator": "University of South Carolina, SLAC National Accelerator Laboratory, Iowa State University",
            "dc.identifier": "http://hdl.handle.net/11256/836",
            "dc.contributor.author": ["Bunn, Jonathan K.", "Fang, Randy L.", "Albing, Mark R.", "Mehta, Apurva", "Kramer, Matt J.", "Besser, Matt F.", "Hattrick-Simpers, Jason R"],
#            "dc.subject": ,
            "dc.description": "The data set was used to evaluate a Fe-Cr-Al thin film samples in a narrow composition region centered on known bulk compositions. The data are composed of two individual studies. The first set of data is a low temperature oxidation study on composition spread sampled performed at SLAC Beamline 1-5. Only the integrated and background subtracted 1-D spectra are included, the 2-D data and calibrations are available upon request. The second set of data was taken during high temperature oxidation of selected samples. These data are exclusively Raman data with values taken as a function of total oxidation time.",
            "dc.relatedidentifier": ["http://iopscience.iop.org/article/10.1088/0957-4484/26/27/274003/meta", "http://dx.doi.org/10.1088/0957-4484/26/27/274003"],
            "dc.year": 2015
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
    #dataset_validator = Validator(dataset_metadata, strict=False)
    # You can also force the Validator to treat warnings as errors with strict=True
    dataset_validator = Validator(dataset_metadata, strict=True)


    # Get the data
    with open(os.path.join(input_path, "Fe_Cr_Al_data", "Point Number to Composition.csv")) as composition_file:
        composition_list = list(parse_tab(composition_file.read()))
        compositions = {}
        for comp in composition_list:
            compositions[int(comp.pop("Sample Number"))] = comp
    # Each record also needs its own metadata
    for data_file in tqdm(find_files(input_path, ".txt"), desc="Processing files", disable= not verbose):
        link = "https://data.materialsdatafacility.org/collections/" + data_file["no_root_path"] + "/" + data_file["filename"]
        temp_k = data_file["filename"].split(" ")[0]
        point_num = int(data_file["filename"].replace("_", " ").split(" ")[-1].split(".")[0])
        record_metadata = {
            "globus_subject": link,
            "acl": ["public"],
#            "mdf-publish.publication.collection": , 
#            "mdf_data_class": ,
            "mdf-base.material_composition": "FeCrAl",

#            "cite_as": ,
#            "license": ,

            "dc.title": "Fe-Cr-Al Oxidation - " + data_file["filename"].split(".")[0],
#            "dc.creator": ,
            "dc.identifier": link,
#            "dc.contributor.author": ,
#            "dc.subject": ,
#            "dc.description": ,
#            "dc.relatedidentifier": ,
#            "dc.year": ,

            "data": {
#                "raw": ,
                "files": {"csv": link},
                "temperature_k": temp_k,
                "atomic_composition_percent": {
                    "Fe": compositions[point_num]["Fe at. %"],
                    "Cr": compositions[point_num]["Cr at. %"],
                    "Al": compositions[point_num]["Al at. %"]
                    }
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

    if verbose:
        print("Finished converting")


# Optionally, you can have a default call here for testing
# The convert function may not be called in this way, so code here is primarily for testing
if __name__ == "__main__":
    import paths
    convert(paths.datasets+"Fe_Cr_Al_oxidation", verbose=True)
