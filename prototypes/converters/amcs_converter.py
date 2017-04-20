from validator import Validator
from parsers.ase_parser import parse_ase
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
    dataset_validator = Validator(dataset_metadata)


    #Get the data
    for cif in tqdm(find_files(root=input_path, file_pattern=".cif", verbose=verbose), desc="Processing files", disable= not verbose):
        #TODO:FIX
            cif_data = parse_ase(file_path=os.path.join(cif["path"], cif["filename"]), data_format="cif", verbose=False)
        if cif_data:
            #Each record also needs its own metadata
            record_metadata = {
                "mdf_source_name" : "amcs",
                "dc.title" : "AMCS - " + cif_data["chemical_formula"],
                "dc.creator" : "The American Mineralogist Crystal Structure Database",
                "dc.contributor.author" : ["Downs, R.T.", "Hall-Wallace, M."],
                "dc.identifier" : "http://rruff.geo.arizona.edu/AMS/minerals/" + cif_data["_chemical_name_mineral"],
#               "dc.subject" : [],
#               "dc.description" : "",
#               "dc.relatedidentifier" : [],
                "dc.year" : 2003,
                "mdf-base.material_composition" : cif_data["chemical_formula"],
                "mdf-publish.publication.collection" : "AMCS",
                "data" : cif_data #This is important: the actual record data (any fields not already here) go here
                }
            
            #Pass each individual record to the Validator
            result = dataset_validator.write_record(record_metadata)

            #Check if the Validator accepted the record, and print a message if it didn't
            #If the Validator returns "success" == True, the record was written successfully
            if result["success"] != True:
                print("Error:", result["message"], ":", result.get("invalid_metadata", ""))

    if verbose:
        print("Finished converting")



if __name__ == "__main__":
    import paths
    convert(paths.datasets+"amcs", True)
