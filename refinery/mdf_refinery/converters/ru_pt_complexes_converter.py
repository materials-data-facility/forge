import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: Uniting Ruthenium(II) and Platinum(II) Polypyridine Centers in Heteropolymetallic Complexes Giving Strong Two-Photon Absorption
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

                "title": "Uniting Ruthenium(II) and Platinum(II) Polypyridine Centers in Heteropolymetallic Complexes Giving Strong Two-Photon Absorption",
                "acl": ["public"],
                "source_name": "ru_pt_complexes",

                "data_contact": {
                    
                    "given_name": "Pengfei",
                    "family_name": "Shi",
                    "email": "shipf@hhit.edu.cn",
                    "institution": "Huaihai Institute of Technology",

                },

                "data_contributor": [{
                    
                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78",

                }],

                "citation": ["Shi, Pengfei; Coe, Benjamin J.; Sánchez, Sergio; Wang, Daqi; Tian, Yupeng; Nyk, Marcin; Samoc, Marek (2015): Uniting Ruthenium(II) and Platinum(II) Polypyridine Centers in Heteropolymetallic Complexes Giving Strong Two-Photon Absorption. ACS Publications. https://doi.org/10.1021/acs.inorgchem.5b02089 Retrieved: 15:54, Jul 27, 2017 (GMT)"],

                "author": [{

                    "given_name": "Pengfei",
                    "family_name": "Shi",
                    "email": "shipf@hhit.edu.cn",
                    "institution": "Huaihai Institute of Technology",

                },
                {

                    "given_name": "Benjamin J.",
                    "family_name": "Coe",
                    "email": "b.coe@manchester.ac.uk",
                    "institution": "The University of Manchester",

                },
                {

                    "given_name": "Sergio",
                    "family_name": "Sánchez",
                    "institution": "The University of Manchester",

                },
                {

                    "given_name": "Daqi",
                    "family_name": "Wang",
                    "institution": "Liaocheng University",

                },
                {

                    "given_name": "Yupeng",
                    "family_name": "Tian",
                    "institution": "Anhui University",

                },
                {

                    "given_name": "Marcin",
                    "family_name": "Nyk",
                    "institution": "Wrocław University of Technology",

                },
                {

                    "given_name": "Marek",
                    "family_name": "Samoc",
                    "institution": "Wrocław University of Technology",

                }],

                "license": "https://creativecommons.org/licenses/by-nc/4.0/",
                "collection": "Ru Pt Heteropolymetallic Complexes",
                "tags": ["Heteropolymetallic Complexes", "850 nm", "834 nm", "polymetallic species", "Pt coordination", "spectra change", "moietie", "qpy", "MLCT", "2 PA activities", "complex", "301 GM", "PtII", "RuII", "523 GM", "heptanuclear RuPt 6", "absorption bands"],
                "description": "New trinuclear RuPt2 and heptanuclear RuPt6 complex salts are prepared by attaching PtII 2,2′:6′,2″-terpyridine (tpy) moieties to RuII 4,4′:2′,2″:4″,4‴-quaterpyridine (qpy) complexes. Characterization includes single crystal X-ray structures for both polymetallic species. The visible absorption bands are primarily due to RuII → qpy metal-to-ligand charge-transfer (MLCT) transitions, according to time-dependent density functional theory (TD-DFT) calculations. These spectra change only slightly on Pt coordination, while the orange-red emission from the complexes shows corresponding small red-shifts, accompanied by decreases in intensity. Cubic molecular nonlinear optical behavior has been assessed by using Z-scan measurements. These reveal relatively high two-photon absorption (2PA) cross sections σ2, with maximal values of 301 GM at 834 nm (RuPt2) and 523 GM at 850 nm (RuPt6) when dissolved in methanol or acetone, respectively. Attaching PtII(tpy) moieties triples or quadruples the 2PA activities when compared with the RuII-based cores.",
                "year": 2015,

                "links": {

                    "landing_page": "https://figshare.com/collections/Uniting_Ruthenium_II_and_Platinum_II_Polypyridine_Centers_in_Heteropolymetallic_Complexes_Giving_Strong_Two_Photon_Absorption/2204182",
                    "publication": ["https://doi.org/10.1021/acs.inorgchem.5b02089"],
                    #"data_doi": "",
                    #"related_id": ,

                    #"data_link": {

                        #"globus_endpoint": ,
                        #"http_host": ,

                        #"path": ,
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
    for data_file in tqdm(find_files(input_path, "(xyz|cif)"), desc="Processing files", disable=not verbose):
        dtype = data_file["filename"].split(".")[-1]
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), dtype)
        ## Metadata:record
        record_metadata = {
            "mdf": {

                "title": "Ru Pt Heteropolymetallic Complexes - " + record["chemical_formula"],
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

                    dtype: {

                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",

                        "path": "/collections/ru_pt_complexes/" + data_file["filename"],
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
