import json
import sys
import os

from tqdm import tqdm

from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for Nanomine.
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
            "title": "NanoMine",
            "acl": ["public"],
            "source_name": "nanomine",
            "citation": ["Publication pending"],
            "data_contact": {

                "given_name": "L. Catherine",
                "family_name": "Brinson",

                "email": "cbrinson@northwestern.edu",
                "institution": "Northwestern University",

                # IDs
                },

            "author": {
                "given_name": "Yixing",
                "family_name": "Wang",

                "email": "yixingwang2014_at_u.northwestern.edu",
                "institution": "Northwestern University"
                },

#            "license": ,

            "collection": "NanoMine",
            "tags": ["polymer", "nanocomposites"],

            "description": "Material Informatics for Polymer Nanocomposites",
            "year": 2014,

            "links": {

                "landing_page": "http://nanomine.northwestern.edu:8000/",

#                "publication": ,
#                "dataset_doi": ,

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
    with open(os.path.join(input_path, "nanomine.dump"), 'r') as dump_file:
        for line in tqdm(dump_file, desc="Processing files", disable= not verbose):
            record = json.loads(line)
            # Try extracting required data
            try:
                citation = record["content"]["PolymerNanocomposite"]["DATA_SOURCE"]["Citation"]["CommonFields"] # Shortcut
                uri = "http://nanomine.northwestern.edu:8000/explore/detail_result_keyword?id=" + record["_id"]["$oid"]
                record_metadata = {
                "mdf": {
                    "title": citation["Title"],
                    "acl": ["public"],

#                    "tags": ,
#                    "description": ,
                    
#                    "composition": ,
                    "raw": json.dumps(record),

                    "links": {
                        "landing_page": uri,

#                        "publication": ,
#                        "dataset_doi": ,

#                        "related_id": ,

                        # data links: {
         
                            #"globus_endpoint": ,
                            #"http_host": ,

                            #"path": ,
                            #},
                        },

#                    "citation": ,
#                    "data_contact": {

#                        "given_name": ,
#                        "family_name": ,

#                        "email": ,
#                        "institution":,

                        # IDs
                    #    },

#                    "author": ,

#                    "license": ,
#                    "collection": ,
#                    "data_format": ,
#                    "data_type": ,
#                    "year": ,

#                    "mrr":

        #            "processing": ,
        #            "structure":,
                    }
                }
            except Exception as e:
                # Something required failed. Skip record.
#                print(repr(e))
                continue

            # Now extract non-required data (which can be missing)
            # Material composition
            mat_comp = get_nanomine_materials(record)
            if mat_comp:
                record_metadata["mdf"]["composition"] = mat_comp

            # Related identifiers (DOI, URL, and image links)
            image_list = record["content"]["PolymerNanocomposite"].get("MICROSTRUCTURE", {}).get("ImageFile", [])
            if type(image_list) is not list:
                image_list = [image_list]
            related_list = [citation.get("DOI", "").replace("doi:", "http://dx.doi.org/"), citation.get("URL", "")] + [image["File"] for image in image_list if image.get("File", None)]
            if related_list:
                record_metadata["mdf"]["links"]["publication"] = [rel for rel in related_list if rel]

            # Year
            year = citation.get("PublicationYear")
            if year:
                record_metadata["mdf"]["year"] = int(year)

            # Pass each individual record to the Validator
            result = dataset_validator.write_record(record_metadata)

            # Check if the Validator accepted the record, and print a message if it didn't
            # If the Validator returns "success" == True, the record was written successfully
            if result["success"] is not True:
                print("Error:", result["message"])

    if verbose:
        print("Finished converting")


#Pulls out materials information
def get_nanomine_materials(data):
    materials_data = data["content"]["PolymerNanocomposite"]["MATERIALS"]
    to_fetch_polymer = ["ChemicalName", "Abbreviation", "ConstitutionalUnit", "PlasticType", "PolymerClass", "PolymerType"]
    to_fetch_particle = ["ChemicalName", "Abbreviation", "TradeName"]
    materials_list = set()
    
    if not materials_data.get("Polymer", None):
        polymer = []
    elif type(materials_data.get("Polymer", None)) is not list:
        polymer = [materials_data.get("Polymer", None)]
    else:
        polymer = materials_data.get("Polymer", [])

    for elem in polymer:
        for field in to_fetch_polymer:
            if field in elem.keys():
                materials_list.add(elem[field])
    
    if not materials_data.get("Particle", None):
        particle = []
    elif type(materials_data.get("Particle", None)) is not list:
        particle = [materials_data.get("Particle", None)]
    else:
        particle = materials_data.get("Particle", [])
    
    for elem in particle:
        for field in to_fetch_particle:
            if field in elem.keys():
                materials_list.add(elem[field])

    return "".join(list(materials_list))

