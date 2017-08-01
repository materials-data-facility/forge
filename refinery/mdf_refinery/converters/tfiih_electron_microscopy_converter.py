import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: Modeling of the TFIIH complex using chemical cross-links and electron microscopy (EM) density maps
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

                "title": "Modeling of the TFIIH complex using chemical cross-links and electron microscopy (EM) density maps",
                "acl": ["public"],
                "source_name": "tfiih_electron_microscopy",

                "data_contact": {

                    "given_name": "Benjamin",
                    "family_name": "Webb",
                    "email": "ben@salilab.org",
                    "institution": "University of California, San Francisco",

                },

                "data_contributor": [{

                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78",

                }],

                "citation": ["Luo, J, Cimermancic, P, Viswanath, S, Ebmeier, CC, Kim, B, Dehecq, M, … Ranish, J. (2015). Modeling of the TFIIH complex using chemical cross-links and electron microscopy (EM) density maps [Data set]. Mol Cell. Zenodo. http://doi.org/10.5281/zenodo.495501"],

                "author": [{

                    "given_name": "Jie",
                    "family_name": "Luo",
                    "institution": "Institute for Systems Biology",

                },
                {

                    "given_name": "Peter",
                    "family_name": "Cimermancic",
                    "institution": "University of California, San Francisco",

                },
                {

                    "given_name": "Shruthi",
                    "family_name": "Viswanath",
                    "institution": "University of California, San Francisco",

                },
                {

                    "given_name": "Christopher C.",
                    "family_name": "Ebmeier",
                    "institution": "University of Colorado, Boulder",

                },
                {

                    "given_name": "Bong",
                    "family_name": "Kim",
                    "institution": "Institute for Systems Biology",

                },
                {

                    "given_name": "Marine",
                    "family_name": "Dehecq",
                    "institution": "Fred Hutchinson Cancer Research Center, Génétique des Interactions Macromoléculaires Institut Pasteur",

                },
                {

                    "given_name": "Vishnu",
                    "family_name": "Raman",
                    "institution": "University of Colorado, Boulder",

                },
                {

                    "given_name": "Charles H.",
                    "family_name": "Greenberg",
                    "institution": "University of California, San Francisco",

                },
                {

                    "given_name": "Riccardo",
                    "family_name": "Pellarin",
                    "institution": "University of California, San Francisco",

                },
                {

                    "given_name": "Andrej",
                    "family_name": "Sali",
                    "institution": "University of California, San Francisco",

                },
                {

                    "given_name": "Dylan J.",
                    "family_name": "Taatjes",
                    "institution": "University of Colorado, Boulder",

                },
                {

                    "given_name": "Steven",
                    "family_name": "Hahn",
                    "institution": "Fred Hutchinson Cancer Research Center",

                },
                {

                    "given_name": "Jeff",
                    "family_name": "Ranish",
                    "email": "jranish@systemsbiology.org",
                    "institution": "Institute for Systems Biology",

                }],

                "license": "http://www.opensource.org/licenses/LGPL-2.1",
                "collection": "TFIIH Electron Microscopy",
                "tags": ["Integrative Modeling Platform (IMP)", "Chemical crosslinks", "Electron microscopy density map", "PMI"],
                "description": "TFIIH is essential for both RNA polymerase II transcription and DNA repair, and mutations in TFIIH can result in human disease. Here, we determine the molecular architecture of human and yeast TFIIH by an integrative approach using chemical crosslinking/mass spectrometry (CXMS) data, biochemical analyses, and previously published electron microscopy maps. We identified four new conserved \"topological regions\" that function as hubs for TFIIH assembly and more than 35 conserved topological features within TFIIH, illuminating a network of interactions involved in TFIIH assembly and regulation of its activities. We show that one of these conserved regions, the p62/Tfb1 Anchor region, directly interacts with the DNA helicase subunit XPD/Rad3 in native TFIIH and is required for the integrity and function of TFIIH. We also reveal the structural basis for defects in patients with xeroderma pigmentosum and trichothiodystrophy, with mutations found at the interface between the p62 Anchor region and the XPD subunit.",
                "year": 2015,

                "links": {

                    "landing_page": "https://doi.org/10.5281/zenodo.495501",
                    "publication": ["https://github.com/integrativemodeling/tfiih/tree/v1.0", "https://doi.org/10.1016/j.molcel.2015.07.016"],
                    #"data_doi": "",
                    #"related_id": "",

                    "zip": {

                        #"globus_endpoint": ,
                        "http_host": "https://zenodo.org",

                        "path": "/record/495501/files/integrativemodeling/tfiih-v1.0.zip",

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
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "proteindatabank")
        ## Metadata:record
        record_metadata = {
            "mdf": {

                "title": "TFIIH Electron Microscopy - " + record["chemical_formula"],
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

                        "path": "/collections/tfiih_electron_microscopy/" + data_file["no_root_path"] + "/" + data_file["filename"],

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


    # TODO: Save your converter as [mdf-source_name]_converter.py
    # You're done!
    if verbose:
        print("Finished converting")
