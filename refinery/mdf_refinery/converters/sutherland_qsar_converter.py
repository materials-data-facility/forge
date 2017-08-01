import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: A Comparison of Methods for Modeling Quantitative Structure−Activity Relationships
# Arguments:
#   input_path (string): The file or directory where the data resides.
#       NOTE: Do not hard-code the path to the data in the converter (the filename can be hard-coded, though). The converter should be portable.
#   metadata (string or dict): The path to the JSON dataset metadata file, a dict or json.dumps string containing the dataset metadata, or None to specify the metadata here. Default None.
#   verbose (bool): Should the script print status messages to standard output? Default False.
#       NOTE: The converter should have NO output if verbose is False, unless there is an error.
def convert(input_path, metadata=None, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    # NOTE: For fields that represent people (e.g. mdf-data_contact), other IDs can be added (ex. "github": "jgaff").
    #    It is recommended that all people listed in mdf-data_contributor have a github username listed.
    #
    # If there are other useful fields not covered here, another block (dictionary at the same level as "mdf") can be created for those fields.
    # The block must be called the same thing as the source_name for the dataset.
    if not metadata:
        ## Metadata:dataset
        dataset_metadata = {
            "mdf": {

                "title": "A Comparison of Methods for Modeling Quantitative Structure−Activity Relationships",
                "acl": ["public"],
                "source_name": "sutherland_qsar",

                "data_contact": {
                    
                    "given_name": "Donald F.",
                    "family_name": "Weaver",
                    "email": "weaver@chem3.chem.dal.ca",
                    "institution": "Dalhousie University",

                },

                "data_contributor": [{
                    
                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78",

                }],

                "citation": ["A Comparison of Methods for Modeling Quantitative Structure-Activity Relationships Jeffrey J. Sutherland, Lee A. O'Brien, and Donald F. Weaver J. Med. Chem.; 2004; 47(22) pp 5541 - 5554", "Depriest, S. A.; Mayer, D.; Naylor, C. B.; Marshall, G. R. 3D-QSAR of angiotensin-converting enzyme and thermolysin inhibitors - a comparison of CoMFA models based on deduced and experimentally determined active-site geometries. J. Am. Chem. Soc. 1993, 115, 5372-5384.", "Gohlke, H.; Klebe, G. Drugscore meets CoMFA: Adaptation of fields for molecular comparison (AFMoC) or how to tailor knowledge-based pair-potentials to a particular protein. J. Med. Chem. 2002, 45, 4153-4170.", "Bohm, M.; Sturzebecher, J.; Klebe, G. Three-dimensional quantitative structure-activity relationship analyses using comparative molecular field analysis and comparative molecular similarity indices analysis to elucidate selectivity differences of inhibitors binding to trypsin, thrombin, and factor Xa. J. Med. Chem. 1999, 42, 458-477."],

                "author": [{

                    "given_name": "Jeffrey J.",
                    "family_name": "Sutherland",
                    "institution": "Queen’s University",

                },
                {

                    "given_name": "Lee A.",
                    "family_name": "O'Brien",
                    "institution": "Queen’s University",

                },
                {

                    "given_name": "Donald F.",
                    "family_name": "Weaver",
                    "email": "weaver@chem3.chem.dal.ca",
                    "institution": "Dalhousie University",

                }],

                "license": "https://creativecommons.org/licenses/by-nc/4.0/",
                "collection": "Sutherland QSAR",
                "tags": ["CORINA", "2.5 D descriptors", "EVA", "function approximation algorithm", "HQSAR", "method", "glycogen phosphorylase b", "test sets", "PLS", "3 D descriptors", "QSAR", "accuracy", "model"],
                "description": "A large number of methods are available for modeling quantitative structure−activity relationships (QSAR). We examine the predictive accuracy of several methods applied to data sets of inhibitors for angiotensin converting enzyme, acetylcholinesterase, benzodiazepine receptor, cyclooxygenase-2, dihydrofolate reductase, glycogen phosphorylase b, thermolysin, and thrombin. Descriptors calculated with CoMFA, CoMSIA, EVA, HQSAR, and traditional 2D and 2.5D descriptors were used for developing models with partial least squares (PLS).",
                "year": 2004,

                "links": {

                    "landing_page": "https://figshare.com/articles/A_Comparison_of_Methods_for_Modeling_Quantitative_Structure_Activity_Relationships/3319567",
                    "publication": ["http://pubs.acs.org/doi/full/10.1021/jm0497141"],
                    #"data_doi": "",
                    #"related_id": ,

                    "tar.gz": {

                        #"globus_endpoint": ,
                        "http_host": "https://ndownloader.figshare.com",

                        "path": "/files/5158327",
                        },
                    },
                },

            #"mrr": {

                #},

            #"dc": {

                #},


        }
        ## End metadata
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
    # If the metadata is incorrect, the constructor will throw an exception and the program will exit
    dataset_validator = Validator(dataset_metadata)


    # Get the data
    #    Each record should be exactly one dictionary
    #    You must write your records using the Validator one at a time
    #    It is recommended that you use a parser to help with this process if one is available for your datatype
    #    Each record also needs its own metadata
    for data_file in tqdm(find_files(input_path, "(sdf$|mol$)"), desc="Processing files", disable=not verbose):
        ftype = data_file["filename"].split(".")[-1]
        try:
            record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), ftype)
        except Exception as e:
            #Error is at end of 6 files, but ase still get's the necessary information
            #print(os.path.join(data_file["path"], data_file["filename"]))
            #print(repr(e))
            pass
        ## Metadata:record
        record_metadata = {
            "mdf": {

                "title": "Sutherland QSAR - " + record["chemical_formula"],
                "acl": ["public"],
                "composition": record["chemical_formula"],

#                "tags": ,
#                "description": ,
               #"raw": json.dumps(record),

                "links": {

#                    "landing_page": ,
#                    "publication": ,
#                    "data_doi": ,
#                    "related_id": ,

                    ftype: {

                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",

                        "path": "/collections/sutherland_qsar/" + data_file["no_root_path"] + "/" + data_file["filename"],
                        },
                    },

#                "citation": ,

#                "data_contact": {

#                    "given_name": ,
#                    "family_name": ,
#                    "email": ,
#                    "institution": ,

#                    },

#                "author": [{

#                    "given_name": ,
#                    "family_name": ,
#                    "email": ,
#                    "institution": ,

#                    }],

#                "year": ,

                },

           # "dc": {

           # },


        }
        ## End metadata
        if ftype == "mol":
            name = data_file["no_root_path"].split("/")[-1].split("_")[0]
            record_metadata["mdf"]["links"]["txt"] = {
                "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                "http_host": "https://data.materialsdatafacility.org",
                
                "path": "/collections/sutherland_qsar/" + data_file["no_root_path"] + "/" + name + "_data_sybyl.txt",
            }
        if "corina" in data_file["no_root_path"]:
            name = data_file["no_root_path"].split("/")[-1].split("_")[0]
            record_metadata["mdf"]["links"]["txt"] = {
                "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                "http_host": "https://data.materialsdatafacility.org",
                
                "path": "/collections/sutherland_qsar/25d_descriptors/" + name + "_25d_desc.txt",
            }

        # Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)

        # Check if the Validator accepted the record, and stop processing if it didn't
        # If the Validator returns "success" == True, the record was written successfully
        if not result["success"]:
            if not dataset_validator.cancel_validation()["success"]:
                print("Error cancelling validation. The partial feedstock may not be removed.")
            raise ValueError(result["message"] + "\n" + result.get("details", ""))


    # You're done!
    if verbose:
        print("Finished converting")
