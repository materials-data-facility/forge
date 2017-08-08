import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.tab_parser import parse_tab
from mdf_refinery.validator import Validator
import pandas as pd

# VERSION 0.3.0

# This is the converter for: KLH Dataset I
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

                "title": "KLH Dataset I",
                "acl": ["public"],
                "source_name": "klh_1",

                "data_contact": {

                    "given_name": "Clinton S",
                    "family_name": "Potter",
                    "email": "cpotter@scripps.edu",
                    "institution": "The Scripps Research Institute",

                },

                "data_contributor": [{

                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78",

                }],

                #"citation": [""],

                "author": [{

                    "given_name": "Yuanxin",
                    "family_name": "Zhu",
                    "institution": "The Scripps Research Institute",

                },
                {

                    "given_name": "Bridget",
                    "family_name": "Carragher",
                    "institution": "The Scripps Research Institute",

                },
                {

                    "given_name": "Robert M",
                    "family_name": "Glaeser",
                    "institution": "University of California, Berkeley",

                },
                {

                    "given_name": "Denis",
                    "family_name": "Fellmann",
                    "institution": "The Scripps Research Institute",

                },
                {

                    "given_name": "Chandrajit",
                    "family_name": "Bajaj",
                    "institution": "University of Texas at Austin,",

                },
                {

                    "given_name": "Marshall",
                    "family_name": "Bern",
                    "institution": "Palo Alto Research Center",

                },
                {

                    "given_name": "Fabrice",
                    "family_name": "Mouche",
                    "institution": "The Scripps Research Institute",

                },
                {

                    "given_name": "Felix",
                    "family_name": "de Haas",
                    "institution": "FEI Company, Eindhoven",

                },
                {

                    "given_name": "Richard J",
                    "family_name": "Hall",
                    "institution": "Imperial College London",

                },
                {

                    "given_name": "David J",
                    "family_name": "Kriegman",
                    "institution": "University of California, San Diego",

                },
                {

                    "given_name": "Steven J",
                    "family_name": "Ludtke",
                    "institution": "Baylor College of Medicine",

                },
                {

                    "given_name": "Satya P",
                    "family_name": "Mallick",
                    "institution": "University of California, San Diego",

                },
                {

                    "given_name": "Pawel A",
                    "family_name": "Penczek",
                    "institution": "University of Texas-Houston Medical School",

                },
                {

                    "given_name": "Alan M",
                    "family_name": "Roseman",
                    "institution": "MRC Laboratory of Molecular Biology",

                },
                {

                    "given_name": "Fred J",
                    "family_name": "Sigworth",
                    "institution": "Yale University School of Medicine",

                },
                {

                    "given_name": "Niels",
                    "family_name": "Volkmann",
                    "institution": "The Burnham Institute",

                },
                {

                    "given_name": "Clinton S",
                    "family_name": "Potter",
                    "email": "cpotter@scripps.edu",
                    "institution": "The Scripps Research Institute",

                }],

                #"license": "",
                "collection": "Keyhole Limpet Hemocyanin",
                "tags": ["Electron microscopy", "Single-particle reconstruction", "Automatic particle selection", "Image processing", "Pattern recognition"],
                "description": "Manual selection of single particles in images acquired using cryo-electron microscopy (cryoEM) will become a significant bottleneck when datasets of a hundred thousand or even a million particles are required for structure determination at near atomic resolution. Algorithm development of fully automated particle selection is thus an important research objective in the cryoEM field. A number of research groups are making promising new advances in this area. Evaluation of algorithms using a standard set of cryoEM images is an essential aspect of this algorithm development. With this goal in mind, a particle selection \"bakeoff\" was included in the program of the Multidisciplinary Workshop on Automatic Particle Selection for cryoEM. Twelve groups participated by submitting the results of testing their own algorithms on a common dataset. The dataset consisted of 82 defocus pairs of high-magnification micrographs, containing keyhole limpet hemocyanin particles, acquired using cryoEM.",
                "year": 2004,

                "links": {

                    "landing_page": "http://emg.nysbc.org/redmine/projects/public-datasets/wiki/KLH_dataset_I",
                    "publication": ["http://www.sciencedirect.com/science/article/pii/S1047847703002004#!"],
                    #"data_doi": "",
                    #"related_id": "",

                    #"data_link": {

                        #"globus_endpoint": ,
                        #"http_host": "",

                        #"path": "",

                    #},

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
    for data_file in tqdm(find_files(input_path, "map$"), desc="Processing Files", disable=not verbose):
        with open(os.path.join(data_file["path"], data_file["filename"]), 'r') as raw_in:
            map_data = raw_in.read()
        headers = ["index", "image", "coordinate"]
        for line in parse_tab(map_data, headers=headers, sep=" "):
            ifile_1 = line["image"].replace(".002", ".001")
            ifile_2 = line["image"]
            cfile = line["coordinate"]
            df = pd.read_csv(os.path.join(data_file["path"], cfile), delim_whitespace=True)
            ## Metadata:record
            record_metadata = {
                "mdf": {
    
                    "title": "Keyhole Limpet Hemocyanin 1 - " + cfile,
                    "acl": ["public"],
                    #"composition": ,
    
                    #"tags": ,
                    "description": "Images under exposure1 are near-to-focus (NTF). Images under exposure2 are far-from-focus (FFF).",
                    #"raw": ,
    
                    "links": {
    
                        #"landing_page": ,
                        #"publication": ,
                        #"data_doi": ,
                        #"related_id": ,
    
                        "klh": {
    
                            "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                            "http_host": "https://data.materialsdatafacility.org",
    
                            "path": "/collections/klh_1/" + data_file["no_root_path"] + "/" + cfile,
    
                            },
    
                        "jpg": {
        
                                "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                                "http_host": "https://data.materialsdatafacility.org",
        
                                "path": "/collections/klh_1/exposure1_jpeg/" + ifile_1.replace(".mrc", ".jpg"),
        
                            },
    
    
                        "mrc": {
        
                                "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                                "http_host": "https://data.materialsdatafacility.org",
        
                                "path": "/collections/klh_1/exposure1_mrc/" + ifile_1,
        
                            },
                        
                        "jpg2": {
        
                                "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                                "http_host": "https://data.materialsdatafacility.org",
        
                                "path": "/collections/klh_1/exposure2_jpeg/" + ifile_2.replace(".mrc", ".jpg"),
        
                            },
    
    
                        "mrc2": {
        
                                "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                                "http_host": "https://data.materialsdatafacility.org",
        
                                "path": "/collections/klh_1/exposure2_mrc/" + ifile_2,
        
                            },
                    },
    
                    #"citation": ,
    
                    #"data_contact": {
    
                        #"given_name": ,
                        #"family_name": ,
                        #"email": ,
                        #"institution": ,
    
                    #},
    
                    #"author": [{
    
                        #"given_name": ,
                        #"family_name": ,
                        #"email": ,
                        #"institution": ,
    
                    #}],
    
                    #"year": ,
    
                },
    
                #"dc": {
    
                #},
    
    
            }
            ## End metadata
    
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
