import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: Molecular architecture of the 40S⋅eIF1⋅eIF3 translation initiation complex
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

                "title": "Molecular architecture of the 40S⋅eIF1⋅eIF3 translation initiation complex",
                "acl": ["public"],
                "source_name": "translation_initiation_complex",

                "data_contact": {
                    
                    "given_name": "Jan P.",
                    "family_name": "Erzberger",
                    "email": "jan.erzberger@mol.biol.ethz.ch",
                    "institution": "Institute of Molecular Biology and Biophysics, ETH Zurich",

                },

                "data_contributor": [{
                    
                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78",

                }],

                "citation": ["Erzberger, J. P., Stengel, F., Pellarin, R., Zhang, S., Schaefer, T., Aylett, C. H. S., … Ban, N. (2014). Molecular architecture of the 40S⋅eIF1⋅eIF3 translation initiation complex [Data set]. Cell. Zenodo. http://doi.org/10.5281/zenodo.46415"],

                "author": [{

                    "given_name": "Jan P.",
                    "family_name": "Erzberger",
                    "email": "jan.erzberger@mol.biol.ethz.ch",
                    "institution": "Institute of Molecular Biology and Biophysics, ETH Zurich",

                },
                {

                    "given_name": "Florian",
                    "family_name": "Stengel",
                    "institution": "Institute of Molecular Biology and Biophysics, ETH Zurich",

                },
                {

                    "given_name": "Riccardo",
                    "family_name": "Pellarin",
                    "institution": "University of California, San Francisco",

                },
                {

                    "given_name": "Suyang",
                    "family_name": "Zhang",
                    "institution": "Institute of Molecular Biology and Biophysics, ETH Zurich",

                },
                {

                    "given_name": "Tanja",
                    "family_name": "Schaefer",
                    "institution": "Institute of Molecular Biology and Biophysics, ETH Zurich",

                },
                {

                    "given_name": "Christopher H.S.",
                    "family_name": "Aylett",
                    "institution": "Institute of Molecular Biology and Biophysics, ETH Zurich",

                },
                {

                    "given_name": "Peter",
                    "family_name": "Cimermančič",
                    "institution": "University of California, San Francisco",

                },
                {

                    "given_name": "Daniel",
                    "family_name": "Boehringer",
                    "institution": "Institute of Molecular Biology and Biophysics, ETH Zurich",

                },
                {

                    "given_name": "Andrej",
                    "family_name": "Sali",
                    "institution": "University of California, San Francisco",

                },
                {

                    "given_name": "Ruedi",
                    "family_name": "Aebersold",
                    "institution": "University of Zurich",

                },
                {

                    "given_name": "Nenad",
                    "family_name": "Ban",
                    "institution": "Institute of Molecular Biology and Biophysics, ETH Zurich",

                }],

                "license": "http://www.opensource.org/licenses/LGPL-2.1",
                "collection": "Translation Initiation Complex",
                "tags": ["Integrative Modeling Platform (IMP)", "Chemical crosslinks", "MODELLER", "PMI", "X-ray crystallography"],
                "description": "Eukaryotic translation initiation requires the recruitment of the large, multiprotein eIF3 complex to the 40S ribosomal subunit. Using X-ray structures of all major components of the minimal, six-subunit Saccharomyces cerevisiae eIF3 core, together with cross-linking coupled to mass spectrometry, we were able to use IMP to position and orient all eIF3 components on the 40S•eIF1 complex, revealing an extended, modular arrangement of eIF3 subunits.",
                "year": 2014,

                "links": {

                    "landing_page": "https://doi.org/10.5281/zenodo.46415",
                    "publication": ["https://github.com/integrativemodeling/40s-eif1-eif3/tree/v1.0", "https://doi.org/10.1016/j.cell.2014.07.044"],
                    #"data_doi": "",
                    #"related_id": ,

                    "zip": {

                        #"globus_endpoint": ,
                        "http_host": "https://zenodo.org",

                        "path": "/record/46415/files/40s-eif1-eif3-v1.0.zip",
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
    for data_file in tqdm(find_files(input_path, "pdb"), desc="Processing files", disable=not verbose):
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "proteindatabank")
        ## Metadata:record
        record_metadata = {
            "mdf": {

                "title": "Translation Initiation Complex - " + record["chemical_formula"] + " record - " + data_file["filename"],
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

                    "pdb": {

                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",

                        "path": "/collections/translation_initiation_complex/" + data_file["no_root_path"] + "/" + data_file["filename"],
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
