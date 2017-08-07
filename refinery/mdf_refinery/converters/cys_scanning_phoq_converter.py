import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: Cys-Scanning Disulfide Crosslinking and Bayesian Modeling Probe the Transmembrane Signaling Mechanism of the Histidine Kinase, PhoQ
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

                "title": "Cys-Scanning Disulfide Crosslinking and Bayesian Modeling Probe the Transmembrane Signaling Mechanism of the Histidine Kinase, PhoQ",
                "acl": ["public"],
                "source_name": "cys_scanning_phoq",

                "data_contact": {

                    "given_name": "William F",
                    "family_name": "DeGrado",
                    "email": "william.degrado@ucsf.edu",
                    "institution": "University of California, San Francisco",

                },

                "data_contributor": [{

                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78",

                }],

                "citation": ["Molnar, K. S., Bonomi, M., Pellarin, R., Clinthorne, G. D., Gonzalez, G., Goldberg, S. D., … DeGrado, W. F. (2014). Multi-state modeling of the PhoQ two-component system [Data set]. Structure. Zenodo. http://doi.org/10.5281/zenodo.46600"],

                "author": [{

                    "given_name": "Kathleen S",
                    "family_name": "Molnar",
                    "institution": "University of Pennsylvania",

                },
                {

                    "given_name": "Massimiliano",
                    "family_name": "Bonomi",
                    "institution": "University of California, San Francisco",

                },
                {

                    "given_name": "Riccardo",
                    "family_name": "Pellarin",
                    "institution": "University of California, San Francisco",

                },
                {

                    "given_name": "Graham D",
                    "family_name": "Clinthorne",
                    "institution": "University of Pennsylvania",

                },
                {

                    "given_name": "Gabriel",
                    "family_name": "Gonzalez",
                    "institution": "University of Pennsylvania",

                },
                {

                    "given_name": "Shalom D",
                    "family_name": "Goldberg",
                    "institution": "University of Pennsylvania",

                },
                {

                    "given_name": "Mark",
                    "family_name": "Goulian",
                    "institution": "University of Pennsylvania",

                },
                {

                    "given_name": "Andrej",
                    "family_name": "Sali",
                    "institution": "University of California, San Francisco",

                },
                {

                    "given_name": "William F",
                    "family_name": "DeGrado",
                    "email": "william.degrado@ucsf.edu",
                    "institution": "University of California, San Francisco",

                }],

                "license": "http://www.opensource.org/licenses/LGPL-2.1",
                "collection": "Cys-Scanning PhoQ",
                "tags": ["Integrative Modeling Platform (IMP)", "Cysteine crosslinks", "Multi-state"],
                "description": "Bacteria transduce signals across the membrane using two-component systems (TCSs), consisting of a membrane-spanning sensor histidine kinase and a cytoplasmic response regulator. In gram-negative bacteria, the PhoPQ TCS senses cations and antimicrobial peptides, yet little is known about the structural changes involved in transmembrane signaling. We construct a model of PhoQ signal transduction using Bayesian inference, based on disulfide crosslinking data and homologous crystal structures.",
                "year": 2014,

                "links": {

                    "landing_page": "https://doi.org/10.5281/zenodo.46600",
                    "publication": ["https://doi.org/10.1016/j.str.2014.04.019"],
                    #"data_doi": "",
                    #"related_id": "",

                    "zip": {

                        #"globus_endpoint": ,
                        "http_host": "https://zenodo.org",

                        "path": "/record/46600/files/phoq-v1.0.zip",

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
    for data_file in tqdm(find_files(input_path, "pdb"), desc="Processing Files", disable=not verbose):
        if "data" not in data_file["no_root_path"]:        #frame files are under pqr format which currently we do not have a file reader
            continue
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "proteindatabank")
        ## Metadata:record
        record_metadata = {
            "mdf": {

                "title": "Cys-Scanning PhoQ - " + record["chemical_formula"],
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

                        "path": "/collections/cys_scanning_phoq/" + data_file["no_root_path"] + "/" + data_file["filename"],

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
