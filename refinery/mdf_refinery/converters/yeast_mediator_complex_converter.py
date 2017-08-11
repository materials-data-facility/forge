import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: Molecular architecture of the yeast Mediator complex
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

                "title": "Molecular architecture of the yeast Mediator complex",
                "acl": ["public"],
                "source_name": "yeast_mediator_complex",

                "data_contact": {

                    "given_name": "Benjamin",
                    "family_name": "Webb",
                    "email": "ben@salilab.org",
                    "institution": "University of California San Francisco",

                },

                "data_contributor": [{

                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78",

                }],

                "citation": ["Robinson, Philip J, Trnka, Michael J, Pellarin, Riccardo, Greenberg, Charles H, Bushnell, David A, Davis, Ralph, … Kornberg, Roger D. (2015). Molecular architecture of the yeast Mediator complex [Data set]. eLife. Zenodo. http://doi.org/10.5281/zenodo.802915"],

                "author": [{

                    "given_name": "Philip J",
                    "family_name": "Robinson",
                    "institution": "Stanford University",

                },
                {

                    "given_name": "Michael J",
                    "family_name": "Trnka",
                    "institution": "University of California San Francisco",

                },
                {

                    "given_name": "Riccardo",
                    "family_name": "Pellarin",
                    "institution": "University of California San Francisco",

                },
                {

                    "given_name": "Charles H",
                    "family_name": "Greenberg",
                    "institution": "University of California San Francisco",

                },
                {

                    "given_name": "David A",
                    "family_name": "Bushnell",
                    "institution": "Stanford University",

                },
                {

                    "given_name": "Ralph",
                    "family_name": "Davis",
                    "institution": "Stanford University",

                },
                {

                    "given_name": "Alma L",
                    "family_name": "Burlingame",
                    "institution": "University of California San Francisco",

                },
                {

                    "given_name": "Andrej",
                    "family_name": "Sali",
                    "institution": "University of California San Francisco",

                },
                {

                    "given_name": "Roger D",
                    "family_name": "Kornberg",
                    "institution": "Stanford University",

                }],

                "license": "http://www.opensource.org/licenses/LGPL-2.1",
                "collection": "Yeast Mediator Complex",
                "tags": ["Integrative Modeling Platform (IMP)", "Chemical crosslinks", "PMI", "X-ray crystallography"],
                "description": "The 21-subunit Mediator complex transduces regulatory information from enhancers to promoters, and performs an essential role in the initiation of transcription in all eukaryotes. This repository contains files used in the 3-D modeling of the entire Mediator complex, using an integrative modeling approach that combines information from chemical cross-linking and mass spectrometry; X-ray crystallography; homology modeling; and cryo-electron microscopy.",
                "year": 2015,

                "links": {

                    "landing_page": "https://zenodo.org/record/802915",
                    "publication": ["https://doi.org/10.7554/eLife.08719", "https://github.com/integrativemodeling/mediator/tree/v1.0.3"],
                    "data_doi": "https://doi.org/10.5281/zenodo.802915",
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
    for data_file in tqdm(find_files(input_path, "pdb"), desc="Processing Files", disable=not verbose):
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "proteindatabank")
        ## Metadata:record
        record_metadata = {
            "mdf": {

                "title": "Yeast Mediator Complex - " + record["chemical_formula"],
                "acl": ["public"],
                "composition": record["chemical_formula"],

                #"tags": ,
                #"description": ,
                #"raw": ,

                "links": {

                    #"landing_page": ,
                    #"publication": ,
                    #"data_doi": ,
                    #"related_id": ,

                    "pdb": {

                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",

                        "path": "/collections/yeast_mediator_complex/" + data_file["no_root_path"] + "/" + data_file["filename"],

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
