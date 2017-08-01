import json
import sys
import os

from tqdm import tqdm

from mdf_refinery.validator import Validator
from mdf_refinery.parsers.tab_parser import parse_tab
from mdf_forge.toolbox import find_files

# VERSION 0.3.0

# This is the converter for the Ni-Co-Al-Ti-Cr quinary alloys dataset
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
            "title": 'Research Data Supporting "The microstructure and hardness of Ni-Co-Al-Ti-Cr quinary alloys"',
            "acl": ["public"],
            "source_name": "quinary_alloys",
            "citation": ['Christofidou, K. A., Jones, N. G., Pickering, E. J., Flacau, R., Hardy, M. C., & Stone, H. J. Research Data Supporting "The microstructure and hardness of Ni-Co-Al-Ti-Cr quinary alloys" [Dataset]. https://doi.org/10.17863/CAM.705'],
            "data_contact": {

                "given_name": "Howard",
                "family_name": "Stone",

                "email": "hjs1002@cam.ac.uk",
                "institution": "University of Cambridge"

                },

            "author": [{

                "given_name": "Howard",
                "family_name": "Stone",

                "email": "hjs1002@cam.ac.uk",
                "institution": "University of Cambridge"

                },
                {

                "given_name": "Katerina",
                "family_name": "Christofidou",

                "institution": "University of Cambridge",
                "orcid": "https://orcid.org/0000-0002-8064-5874"

                },
                {

                "given_name": "Nicholas",
                "family_name": "Jones",

                "institution": "University of Cambridge"

                },
                {

                "given_name": "Edward",
                "family_name": "Pickering",

                "institution": "University of Cambridge"

                },
                {

                "given_name": "Roxana",
                "family_name": "Flacau",

                "institution": "University of Cambridge"

                },
                {

                "given_name": "Mark",
                "family_name": "Hardy",

                "institution": "University of Cambridge"

                }],

            "license": "http://creativecommons.org/licenses/by/4.0/",

            "collection": "Ni-Co-Al-Ti-Cr Quinary Alloys",
#            "data_format": ,
#            "data_type": ,
            "tags": ["alloys"],

            "description": "DSC files, neutron diffraction data, hardness measurements, SEM and TEM images and thermodynamic simulations are provided for all alloy compositions studied and presented in this manuscript.",
            "year": 2016,

            "links": {

                "landing_page": "https://www.repository.cam.ac.uk/handle/1810/256771",

                "publication": "https://doi.org/10.1016/j.jallcom.2016.07.159",
                "data_doi": "https://doi.org/10.17863/CAM.705",

#                "related_id": ,

                # data links: {

                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #}
                },

#            "mrr": ,

            "data_contributor": {
                "given_name": "Jonathon",
                "family_name": "Gaff",
                "email": "jgaff@uchicago.edu",
                "institution": "The University of Chicago",
                "github": "jgaff"
                }
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
    with open(os.path.join(input_path, "alloy_data.csv"), 'r') as adata:
        raw_data = adata.read()
    for record in tqdm(parse_tab(raw_data), desc="Processing records", disable= not verbose):
        links = {}
        for ln in find_files(input_path, record["Alloy"]):
            key = "_".join(ln["no_root_path"].split("/")).replace(" ", "_")
            links[key] = {
                "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                "http_host": "https://data.materialsdatafacility.org",
                "path":  os.path.join("/collections/quinary_alloys", ln["no_root_path"], ln["filename"])
                }
        links["csv"] = {
            "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
            "http_host": "https://data.materialsdatafacility.org",
            "path": "/collections/quinary_alloys/alloy_data.csv"
            }
        record_metadata = {
        "mdf": {
            "title": "Ni-Co-Al-Ti-Cr Quinary Alloys " + record["Alloy"],
            "acl": ["public"],

#            "tags": ,
#            "description": ,
            
            "composition": "NiCoAlTiCr",
            "raw": json.dumps(record),

            "links": links, #{
#                "landing_page": ,

#                "publication": ,
#                "dataset_doi": ,

#                "related_id": ,

#                 "csv": {
 
#                    "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
#                    "http_host": "https://data.materialsdatafacility.org",

#                    "path": "/collections/quinary_alloys/alloy_data.csv",
#                    },
#                },

#            "citation": ,
#            "data_contact": {

#                "given_name": ,
#                "family_name": ,

#                "email": ,
#               "institution":,

                # IDs
#                },

#            "author": ,

#            "license": ,
#            "collection": ,
#            "data_format": ,
#            "data_type": ,
#            "year": ,

#            "mrr":

#            "processing": ,
#            "structure":,
            },
            "quinary_alloys": {

            "atomic_composition_percent": {
                "Ni": record["Ni"],
                "Co": record["Co"],
                "Al": record["Al"],
                "Ti": record["Ti"],
                "Cr": record["Cr"]
                }
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
