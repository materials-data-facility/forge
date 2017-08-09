import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: Benchmark of the FRETR Bayesian restraint
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

                "title": "Benchmark of the FRETR Bayesian restraint",
                "acl": ["public"],
                "source_name": "fretr_bayesian_restraint",

                "data_contact": {

                    "given_name": "Andrej",
                    "family_name": "Sali",
                    "email": "sali@salilab.org",
                    "institution": "University of California, San Francisco",

                },

                "data_contributor": [{

                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78",

                }],

                "citation": ["Bonomi, M., Pellarin, R., Kim, S. J., Russel, D., Sundin, B. A., Riffle, M., … Sali, A. (2014). Benchmark of the FRETR Bayesian restraint [Data set]. Mol Cell Proteomics. Zenodo. http://doi.org/10.5281/zenodo.46558"],

                "author": [{

                    "given_name": "Massimiliano",
                    "family_name": "Bonomi",
                    "email": "mb2006@cam.ac.uk",
                    "institution": "University of California, San Francisco, University of Cambridge",

                },
                {

                    "given_name": "Riccardo",
                    "family_name": "Pellarin",
                    "institution": "University of California, San Francisco",

                },
                {

                    "given_name": "Seung Joong",
                    "family_name": "Kim",
                    "institution": "University of California, San Francisco",

                },
                {

                    "given_name": "Daniel",
                    "family_name": "Russel",
                    "institution": "University of California, San Francisco",

                },
                {

                    "given_name": "Bryan A.",
                    "family_name": "Sundin",
                    "institution": "University of Washington",

                },
                {

                    "given_name": "Michael",
                    "family_name": "Riffle",
                    "institution": "University of Washington",

                },
                {

                    "given_name": "Daniel",
                    "family_name": "Jaschob",
                    "institution": "University of Washington",

                },
                {

                    "given_name": "Richard",
                    "family_name": "Ramsden",
                    "institution": "University of Washington",

                },
                {

                    "given_name": "Trisha N.",
                    "family_name": "Davis",
                    "institution": "University of Washington",

                },
                {

                    "given_name": "Eric G. D.",
                    "family_name": "Muller",
                    "email": "emuller@u.washington.edu",
                    "institution": "University of Washington",

                },
                {

                    "given_name": "Andrej",
                    "family_name": "Sali",
                    "email": "sali@salilab.org",
                    "institution": "University of California, San Francisco",

                }],

                "license": "http://www.opensource.org/licenses/LGPL-2.1",
                "collection": "FRETR Bayesian Restraint",
                "tags": ["Integrative Modeling Platform (IMP)", "Benchmark", "Förster resonance energy transfer (FRET)"],
                "description": "The use of in vivo Förster resonance energy transfer (FRET) data to determine the molecular architecture of a protein complex in living cells is challenging due to data sparseness, sample heterogeneity, signal contributions from multiple donors and acceptors, unequal fluorophore brightness, photobleaching, flexibility of the linker connecting the fluorophore to the tagged protein, and spectral cross-talk. We addressed these challenges by using a Bayesian approach that produces the posterior probability of a model, given the input data. The posterior probability is defined as a function of the dependence of our FRET metric FRETR on a structure (forward model), a model of noise in the data, as well as prior information about the structure, relative populations of distinct states in the sample, forward model parameters, and data noise.",
                "year": 2014,

                "links": {

                    "landing_page": "https://zenodo.org/record/46558",
                    "publication": ["https://doi.org/10.1074/mcp.M114.040824", "https://github.com/integrativemodeling/fret_benchmark/tree/v1.0"],
                    "data_doi": "https://doi.org/10.5281/zenodo.46558",
                    #"related_id": "",

                    "zip": {

                        #"globus_endpoint": ,
                        "http_host": "https://zenodo.org",

                        "path": "/record/46558/files/fret_benchmark-v1.0.zip",

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
    for data_file in tqdm(find_files(input_path, "pdb$"), desc="Processing Files", disable=not verbose):
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "proteindatabank")
        ## Metadata:record
        record_metadata = {
            "mdf": {

                "title": "FRETR Bayesian Restraint - " + record["chemical_formula"],
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

                        "path": "/collections/fretr_bayesian_restraint/" + data_file["no_root_path"] + "/" + data_file["filename"],

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
