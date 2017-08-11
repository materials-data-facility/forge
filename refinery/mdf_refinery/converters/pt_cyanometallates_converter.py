import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: Cyclometalated Platinum(II) Cyanometallates: Luminescent Blocks for Coordination Self-Assembly
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

                "title": "Cyclometalated Platinum(II) Cyanometallates: Luminescent Blocks for Coordination Self-Assembly",
                "acl": ["public"],
                "source_name": "pt_cyanometallates",

                "data_contact": {

                    "given_name": "Igor O.",
                    "family_name": "Koshevoy",
                    "email": "igor.koshevoy@uef.fi",
                    "institution": "University of Eastern Finland",

                },

                "data_contributor": [{

                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78",

                }],

                "citation": ["Schneider, Leon; Sivchik, Vasily; Chung, Kun-you; Chen, Yi-Ting; Karttunen, Antti J.; Chou, Pi-Tai; Koshevoy, Igor O. (2017): Cyclometalated Platinum(II) Cyanometallates: Luminescent Blocks for Coordination Self-Assembly. ACS Publications. https://doi.org/10.1021/acs.inorgchem.7b00006"],

                "author": [{

                    "given_name": "Leon",
                    "family_name": "Schneider",
                    "institution": "Julius-Maximilians-Universität",

                },
                {

                    "given_name": "Vasily",
                    "family_name": "Sivchik",
                    "institution": "University of Eastern Finland",

                },
                {

                    "given_name": "Kun-you",
                    "family_name": "Chung",
                    "institution": "National Taiwan University",

                },
                {

                    "given_name": "Yi-Ting",
                    "family_name": "Chen",
                    "institution": "National Taiwan University",

                },
                {

                    "given_name": "Antti J.",
                    "family_name": "Karttunen",
                    "email": "antti.karttunen@aalto.fi",
                    "institution": "Aalto University",

                },
                {

                    "given_name": "Pi-Tai",
                    "family_name": "Chou",
                    "email": "chop@ntu.edu.tw",
                    "institution": "National Taiwan University",
                    "orcid": "orcid.org/0000-0002-8925-7747",

                },
                {

                    "given_name": "Igor O.",
                    "family_name": "Koshevoy",
                    "email": "igor.koshevoy@uef.fi",
                    "institution": "University of Eastern Finland",
                    "orcid": "orcid.org/0000-0003-4380-1302",

                }],

                "license": "https://creativecommons.org/licenses/by-nc/4.0/",
                "collection": "Cyclometalated Platinum(II) Cyanometallates",
                "tags": ["coordination geometries", "compound", "luminescence studies", "Coordination Self-Assembly", "Luminescent Blocks", "emission performance", "cyclometalated fragment", "chromophoric cycloplatinated metalloligands", "frontier orbitals", "complexes exhibit", "tetranuclear complexes", "time-dependent density", "η 1", "photophysical behavior", "cyanide-bridged heterometallic aggregates", "squarelike arrangement", "F 2 ppy", "phosphine motifs", "Cu", "M Pt LCT contribution", "alternative cluster topology", "metal ions", "tolpy", "10 fragments", "Ag", "room-temperature phosphorescence", "HF 2 ppy"],
                "description": "A family of cyanide-bridged heterometallic aggregates has been constructed of the chromophoric cycloplatinated metalloligands and coordinatively unsaturated d10 fragments {M(PPh3)n}. The tetranuclear complexes of general composition [Pt(C^N)(CN)2M(PPh3)2]2 [C^N = ppy, M = Cu (1), Ag (2); C^N = tolpy (Htolpy = 2-(4-tolyl)-pyridine), M = Cu (4), Ag (5); C^N = F2ppy (HF2ppy = 2-(4, 6-difluorophenyl)-pyridine), M = Cu (7), Ag (8)] demonstrate a squarelike arrangement of the molecular frameworks, which is achieved due to favorable coordination geometries of the bridging ligands and the metal ions. Variation of the amount of the ancillary phosphine (for M = Ag) afforded compounds [Pt(C^N)(CN)2Ag(PPh3)]2 (C^N = ppy, 3; C^N = tolpy, 6); for the latter one an alternative cluster topology, stabilized by the Pt–Ag metallophilic and η1-Cipso(C^N)–Ag bonding, was observed.",
                "year": 2017,

                "links": {

                    "landing_page": "https://figshare.com/collections/Cyclometalated_Platinum_II_Cyanometallates_Luminescent_Blocks_for_Coordination_Self-Assembly/3730237",
                    "publication": ["https://doi.org/10.1021/acs.inorgchem.7b00006"],
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
    for data_file in tqdm(find_files(input_path, "(xyz|cif)"), desc="Processing Files", disable=not verbose):
        dtype = data_file["filename"].split(".")[-1]
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), dtype)
        ## Metadata:record
        record_metadata = {
            "mdf": {

                "title": "Cyclometalated Platinum(II) Cyanometallates - " + record["chemical_formula"],
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

                    dtype: {

                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",

                        "path": "/collections/pt_cyanometallates/" + data_file["no_root_path"] + "/" + data_file["filename"],

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
