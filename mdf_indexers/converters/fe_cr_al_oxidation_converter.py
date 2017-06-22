import json
import sys
import os

from tqdm import tqdm

from ..validator.schema_validator import Validator
from ..parsers.tab_parser import parse_tab
from ..utils.file_utils import find_files

# VERSION 0.2.0

# This is the converter for the Fe-Cr-Al Oxidation Studies dataset
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
            "mdf-title": "High-throughput Diffraction and Spectroscopic Data for Fe-Cr-Al Oxidation Studies",
            "mdf-acl": ["public"],
            "mdf-source_name": "fe_cr_al_oxidation",
            "mdf-citation": ["Bunn, Jonathan K.; Fang, Randy L.; Albing, Mark R.; Mehta, Apurva; Kramer, Matt J.; Besser, Matt F.; Hattrick-Simpers, Jason R High-throughput Diffraction and Spectroscopic Data for Fe-Cr-Al Oxidation Studies (2015-06-28)"],
            "mdf-data_contact": {

                "given_name": "Jason",
                "family_name": "Hattrick-Simpers",

                "email": "simpers@cec.sc.edu",
                "institution": "University of South Carolina Columbia",

                },

#            "mdf-author": ,

#            "mdf-license": ,

            "mdf-collection": "Fe-Cr-Al Oxidation Studies",
            "mdf-data_format": "csv",
            "mdf-data_type": "Text",
#            "mdf-tags": ,

            "mdf-description": "The data set was used to evaluate a Fe-Cr-Al thin film samples in a narrow composition region centered on known bulk compositions. The data are composed of two individual studies. The first set of data is a low temperature oxidation study on composition spread sampled performed at SLAC Beamline 1-5. Only the integrated and background subtracted 1-D spectra are included, the 2-D data and calibrations are available upon request. The second set of data was taken during high temperature oxidation of selected samples. These data are exclusively Raman data with values taken as a function of total oxidation time.",
            "mdf-year": 2015,

            "mdf-links": {

                "mdf-landing_page": "https://materialsdata.nist.gov/dspace/xmlui/handle/11256/836",

                "mdf-publication": "http://dx.doi.org/10.1088/0957-4484/26/27/274003",
                "mdf-dataset_doi": "http://hdl.handle.net/11256/836",

#                "mdf-related_id": ,

                # data links: {

                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #}
                },

#            "mdf-mrr": ,

            "mdf-data_contributor": {
                "given_name": "Jonathon",
                "family_name": "Gaff",
                "email": "jgaff@uchicago.edu",
                "institution": "The University of Chicago",
                "github": "jgaff"
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
    with open(os.path.join(input_path, "Fe_Cr_Al_data", "Point Number to Composition.csv")) as composition_file:
        composition_list = list(parse_tab(composition_file.read()))
        compositions = {}
        for comp in composition_list:
            compositions[int(comp.pop("Sample Number"))] = comp
    # Each record also needs its own metadata
    for data_file in tqdm(find_files(input_path, ".txt"), desc="Processing files", disable= not verbose):
        temp_k = data_file["filename"].split(" ")[0]
        point_num = int(data_file["filename"].replace("_", " ").split(" ")[-1].split(".")[0])
        record_metadata = {
            "mdf-title": "Fe-Cr-Al Oxidation - " + data_file["filename"].split(".")[0],
            "mdf-acl": ["public"],

#            "mdf-tags": ,
#            "mdf-description": ,
            
            "mdf-composition": "FeCrAl",
#            "mdf-raw": ,

            "mdf-links": {
#                "mdf-landing_page": ,

#                "mdf-publication": ,
#                "mdf-dataset_doi": ,

#                "mdf-related_id": ,

                "csv": {
 
                    "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                    "http_host": "https://data.materialsdatafacility.org",

                    "path": "/collections/" + data_file["no_root_path"] + "/" + data_file["filename"],
                    },
                },

#            "mdf-citation": ,
#            "mdf-data_contact": {

#                "given_name": ,
#                "family_name": ,

#                "email": ,
#                "institution":,

                # IDs
#                },

#            "mdf-author": ,

#            "mdf-license": ,
#            "mdf-collection": ,
#            "mdf-data_format": ,
#            "mdf-data_type": ,
#            "mdf-year": ,

#            "mdf-mrr":

#            "mdf-processing": ,
#            "mdf-structure":,

            "temperature_k": temp_k,
            "atomic_composition_percent": {
                "Fe": compositions[point_num]["Fe at. %"],
                "Cr": compositions[point_num]["Cr at. %"],
                "Al": compositions[point_num]["Al at. %"]
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
