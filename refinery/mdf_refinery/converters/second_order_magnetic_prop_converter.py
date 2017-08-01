import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.parsers.tab_parser import parse_tab
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: On the calculation of second-order magnetic properties using subsystem approaches in the relativistic framework
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

                "title": "On the calculation of second-order magnetic properties using subsystem approaches in the relativistic framework",
                "acl": ["public"],
                "source_name": "second_order_magnetic_prop",

                "data_contact": {
                    
                    "given_name": "Andre Severo Pereira",
                    "family_name": "Gomes",
                    "email": "andre.gomes@univ-lille1.fr",
                    "institution": "Universit´e de Lille",

                },

                "data_contributor": [{
                    
                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78",

                }],

                "citation": ["Olejniczak, Małgorzata, Bast, Radovan, & Gomes, Andre Severo Pereira. (2016). On the calculation of second-order magnetic properties using subsystem approaches in the relativistic framework - supplementary information [Data set]. Zenodo. http://doi.org/10.5281/zenodo.179720"],

                "author": [{

                    "given_name": "Małgorzata",
                    "family_name": "Olejniczak",
                    "email": "gosia.olejniczak@univ-lille1.fr",
                    "institution": "Universit´e de Lille",

                },
                {

                    "given_name": "Radovan",
                    "family_name": "Bast",
                    "email": "radovan.bast@uit.no",
                    "institution": "UiT The Arctic University of Norway",

                },
                {

                    "given_name": "Andre Severo Pereira",
                    "family_name": "Gomes",
                    "email": "andre.gomes@univ-lille1.fr",
                    "institution": "Universit´e de Lille",

                }],

                "license": "https://creativecommons.org/licenses/by/4.0/",
                "collection": "Second-Order Magnetic Properties",
                #"tags": [""],
                "description": "We report an implementation of the nuclear magnetic resonance (NMR) shielding (σ), isotopeindependent indirect spin-spin coupling (K) and the magnetizability (ξ) tensors in the frozen density embedding scheme using the four-component (4c) relativistic Dirac–Coulomb (DC) Hamiltonian and the non-collinear spin density functional theory.",
                "year": 2016,

                "links": {

                    "landing_page": "https://doi.org/10.5281/zenodo.179720",
                    "publication": ["https://arxiv.org/abs/arXiv:1610.04280"],
                    #"data_doi": "",
                    #"related_id": ,

                    "tar.gz": {

                        #"globus_endpoint": ,
                        "http_host": "https://zenodo.org",

                        "path": "/record/179720/files/supplementary_info_fde_mag.tar.gz",
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
    for data_file in tqdm(find_files(input_path, "(xyz$|csv$|cube$)"), desc="Processing files", disable=not verbose):
        ftype = data_file["filename"].split(".")[-1]
        if ftype == "xyz" or ftype == "cube":
            records = [parse_ase(os.path.join(data_file["path"], data_file["filename"]), ftype)]
        else:
            with open(os.path.join(data_file["path"], data_file["filename"]), 'r') as raw_in:
                headers = []
                for head in raw_in.readline().split(","):
                    head = head.strip()
                    headers.append(head)
                data = raw_in.read()[1:]
            records = list(parse_tab(data, headers=headers))

        for rec in records:
            if "xyz" in data_file["filename"] or "cube" in data_file["filename"]:
                comp = rec["chemical_formula"]
            elif "shielding" in data_file["filename"]:
                comp = rec["nucleus"]
            elif "spinspin" in data_file["filename"]:
                comp = rec["At1"] + rec["At2"]
            elif "magnetizability" in data_file["filename"]:
                comp = rec["mol"]
                if comp == "sum":
                    continue

            record_metadata = {
                "mdf": {
    
                    "title": "Second-Order Magnetic Properties - " + comp,
                    "acl": ["public"],
                    "composition": comp,
    
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
    
                            "path": "/collections/second_order_magnetic_prop/" + data_file["no_root_path"] + "/" + data_file["filename"],
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
