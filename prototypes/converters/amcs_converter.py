from validator import Validator
from parsers.pymatgen_parser import parse_pymatgen
from parsers.utils import find_files

import os
from tqdm import tqdm

#This is a converter for the American Mineralogist Crystal Structure Database
#Arguments:
#   input_path (string): The file or directory where the data resides. This should not be hard-coded in the function, for portability.
#   verbose (bool): Should the script print status messages to standard output? Default False.
def convert(input_path, verbose=False):

    #Collect the metadata
    dataset_metadata = {
        "mdf_source_name" : "amcs",
        "cite_as": ["Downs, R.T. and Hall-Wallace, M. (2003) The American Mineralogist Crystal Structure Database. American Mineralogist 88, 247-250."],
        "dc.title" : "AMCS DB",
        "dc.creator" : "The American Mineralogist Crystal Structure Database",
        "dc.contributor.author" : ["Downs, R.T.", "Hall-Wallace, M."],
        "dc.identifier" : "http://rruff.geo.arizona.edu/AMS/amcsd.php",
#       "dc.subject" : [],
#       "dc.description" : "",
#       "dc.relatedidentifier" : [],
        "dc.year" : 2003,
        "acl" : ["public"],
#       "mdf-base.material_composition" : chemical_formula,
        "mdf-publish.publication.collection" : "AMCS",
        }
    dataset_metadata["globus_subject"] = dataset_metadata["dc.identifier"]


    
    #Make a Validator to help write the feedstock
    #You can pass the metadata to the constructor
    dataset_validator = Validator(dataset_metadata, strict=True)


    #Get the data
    for cif in tqdm(find_files(root=input_path, file_pattern=".cif", verbose=verbose), desc="Processing files", disable= not verbose):
        cif_data = parse_pymatgen(os.path.join(cif["path"], cif["filename"]))["structure"]
        if cif_data:
            #Each record also needs its own metadata
            with open(os.path.join(cif["path"], cif["filename"])) as cif_file:
                cif_file.readline()
                mineral_name = cif_file.readline().split("'")[1]
            link = "http://rruff.geo.arizona.edu/AMS/minerals/" + mineral_name
            clink = "http://rruff.geo.arizona.edu/AMS/xtal_data/CIFfiles/" + cif["filename"]
            dlink = "http://rruff.geo.arizona.edu/AMS/xtal_data/DIFfiles/" + cif["filename"].replace(".cif", ".txt")
            record_metadata = {
                "globus_subject": clink,
                "acl": ["public"],
#                "mdf_source_name" : "amcs",
                "dc.title" : "AMCS - " + mineral_name,
#                "dc.creator" : "The American Mineralogist Crystal Structure Database",
#                "dc.contributor.author" : ["Downs, R.T.", "Hall-Wallace, M."],
                "dc.identifier" : link,
#               "dc.subject" : [],
#               "dc.description" : "",
#               "dc.relatedidentifier" : [],
#                "dc.year" : 2003,
                "mdf-base.material_composition" : cif_data["material_composition"],
#                "mdf-publish.publication.collection" : "AMCS",
                "data" : {
                    "files": {
                        "cif": clink,
                        "dif": dlink,
                        "web": link
                        }
                    }
                }
            
            #Pass each individual record to the Validator
            result = dataset_validator.write_record(record_metadata)

            #Check if the Validator accepted the record, and print a message if it didn't
            #If the Validator returns "success" == True, the record was written successfully
            if result["success"] != True:
                print("Error:", result["message"], ":", result.get("invalid_metadata", ""))
                raise Exception()

    if verbose:
        print("Finished converting")



if __name__ == "__main__":
    import paths
    convert(paths.datasets+"amcs", True)
