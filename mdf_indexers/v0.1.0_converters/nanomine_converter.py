import json
import sys
from tqdm import tqdm
from validator import Validator


# This is the converter for Nanomine. 
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
            "globus_subject": "http://nanomine.northwestern.edu:8000/",                      # REQ string: Unique value (should be URI if possible)
            "acl": ["public"],                                 # REQ list of strings: UUID(s) of users/groups allowed to access data, or ["public"]
            "mdf_source_name": "nanomine",                     # REQ string: Unique name for dataset
            "mdf-publish.publication.collection": "NanoMine",  # RCM string: Collection the dataset belongs to

            "cite_as": ["Citation pending"],                             # REQ list of strings: Complete citation(s) for this dataset.
#            "license": ,                             # RCM string: License to use the dataset (preferrably a link to the actual license).

            "dc.title": "NanoMine",                            # REQ string: Title of dataset
            "dc.creator": "Northwestern University",                          # REQ string: Owner of dataset
            "dc.identifier": "http://nanomine.northwestern.edu:8000/",                       # REQ string: Link to dataset (dataset DOI if available)
#            "dc.contributor.author": ,               # RCM list of strings: Author(s) of dataset
#            "dc.subject": ,                          # RCM list of strings: Keywords about dataset
            "dc.description": "Material Informatics for Polymer Nanocomposites",                      # RCM string: Description of dataset contents
#            "dc.relatedidentifier": ,                # RCM list of strings: Link(s) to related materials (such as an article)
#            "dc.year":                               # RCM integer: Year of dataset creation
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
    with open(input_path, 'r') as dump_file:
        for line in tqdm(dump_file, desc="Processing files", disable= not verbose):
            record = json.loads(line)
            # Try extracting required data
            try:
                citation = record["content"]["PolymerNanocomposite"]["DATA_SOURCE"]["Citation"]["CommonFields"] # Shortcut
                uri = "http://nanomine.northwestern.edu:8000/explore/detail_result_keyword?id=" + record["_id"]["$oid"]

                record_metadata = {
                    "globus_subject": uri,
                    "acl": ["public"],
                    "mdf-publish.publication.collection": "NanoMine",
#                    "mdf_data_class": ,
#                    "mdf-base.material_composition": get_nanomine_materials(record),

#                    "cite_as": ,
#                    "license": ,

                    "dc.title": citation["Title"],
#                    "dc.creator": ,
                    "dc.identifier": uri,
#                    "dc.contributor.author": ,
#                    "dc.subject": ,
#                    "dc.description": ,
#                    "dc.relatedidentifier": ,
#                    "dc.year": citation["PublicationYear"],

                    "data": {
                        "raw": json.dumps(record)
#                        "file":,
                        }
                    }
            except Exception as e:
                # Something required failed. Skip record.
                continue

            # Now extract non-required data (which can be missing)
            # Material composition
            mat_comp = get_nanomine_materials(record)
            if mat_comp:
                record_metadata["mdf-base.material_composition"] = mat_comp

            # Author list
            authors = citation.get("Author", None)
            if authors:
                if type(authors) is not list:
                    authors = [authors]
                record_metadata["dc.contributor.author"] = authors

            # Related identifiers (DOI, URL, and image links)
            image_list = record["content"]["PolymerNanocomposite"].get("MICROSTRUCTURE", {}).get("ImageFile", [])
            if type(image_list) is not list:
                image_list = [image_list]
            related_list = [citation.get("DOI", "").replace("doi:", "http://dx.doi.org/"), citation.get("URL", "")] + [image["File"] for image in image_list if image.get("File", None)]
            if related_list:
                record_metadata["dc.relatedidentifier"] = [rel for rel in related_list if rel]

            # Year
            year = citation.get("PublicationYear")
            if year:
                record_metadata["dc.year"] = int(year)


            # Pass each individual record to the Validator
            result = dataset_validator.write_record(record_metadata)

            # Check if the Validator accepted the record, and print a message if it didn't
            # If the Validator returns "success" == True, the record was written successfully
            if result["success"] is not True:
                print("Error:", result["message"], ":", result.get("invalid_metadata", ""))

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



# Optionally, you can have a default call here for testing
# The convert function may not be called in this way, so code here is primarily for testing
if __name__ == "__main__":
    import paths
    convert(paths.datasets+"nanomine/nanomine.dump", verbose=True)
