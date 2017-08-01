import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: Platinum Acetate Blue: Synthesis and Characterization


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

                "title": "Platinum Acetate Blue: Synthesis and Characterization",
                "acl": ["public"],
                "source_name": "pt_acetate_blue",

                "data_contact": {
                    
                    "given_name": "Michael N.",
                    "family_name": "Vargaftik",
                    "email": "wahr36@gmail.com",
                    "institution": "Kurnakov Institute of General and Inorganic Chemistry",

                },

                "data_contributor": [{
                    
                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78",

                }],

                "citation": ["Cherkashina, Natalia V.; Kochubey, Dmitry I.; Kanazhevskiy, Vladislav V.; Zaikovskii, Vladimir I.; Ivanov, Vladimir K.; Markov, Alexander A.; Klyagina, Alla P.; Dobrokhotova, Zhanna V.; Kozitsyna, Natalia Yu.; Baranovsky, Igor B.; Ellert, Olga G.; Efimov, Nikolai N.; Nefedov, Sergei E.; Novotortsev, Vladimir M.; Vargaftik, Michael N.; Moiseev, Ilya I. (2014): Platinum Acetate Blue: Synthesis and Characterization. ACS Publications. https://doi.org/10.1021/ic500940a.s002"],

                "author": [{

                    "given_name": "Natalia V.",
                    "family_name": "Cherkashina",
                    "institution": "Kurnakov Institute of General and Inorganic Chemistry",

                },
                {

                    "given_name": "Dmitry I.",
                    "family_name": "Kochubey",
                    "institution": "Boreskov Institute of Catalysis",

                },
                {

                    "given_name": "Vladislav V.",
                    "family_name": "Kanazhevskiy",
                    "institution": "Boreskov Institute of Catalysis",

                },
                {

                    "given_name": "Vladimir I.",
                    "family_name": "Zaikovskii",
                    "institution": "Novosibirsk State University",

                },
                {

                    "given_name": "Vladimir K.",
                    "family_name": "Ivanov",
                    "institution": "Kurnakov Institute of General and Inorganic Chemistry",

                },
                {

                    "given_name": "Alexander A.",
                    "family_name": "Markov",
                    "institution": "Kurnakov Institute of General and Inorganic Chemistry",

                },
                {

                    "given_name": "Alla P.",
                    "family_name": "Klyagina",
                    "institution": "Kurnakov Institute of General and Inorganic Chemistry",

                },
                {

                    "given_name": "Zhanna V.",
                    "family_name": "Dobrokhotova",
                    "institution": "Kurnakov Institute of General and Inorganic Chemistry",

                },
                {

                    "given_name": "Natalia Yu.",
                    "family_name": "Kozitsyna",
                    "institution": "Kurnakov Institute of General and Inorganic Chemistry",

                },
                {

                    "given_name": "Igor B.",
                    "family_name": "Baranovsky",
                    "institution": "Kurnakov Institute of General and Inorganic Chemistry",

                },
                {

                    "given_name": "Olga G.",
                    "family_name": "Ellert",
                    "institution": "Kurnakov Institute of General and Inorganic Chemistry",

                },
                {

                    "given_name": "Nikolai N.",
                    "family_name": "Efimov",
                    "institution": "Kurnakov Institute of General and Inorganic Chemistry",

                },
                {

                    "given_name": "Sergei E.",
                    "family_name": "Nefedov",
                    "institution": "Kurnakov Institute of General and Inorganic Chemistry",

                },
                {

                    "given_name": "Vladimir M.",
                    "family_name": "Novotortsev",
                    "institution": "Kurnakov Institute of General and Inorganic Chemistry",

                },
                {

                    "given_name": "Michael N.",
                    "family_name": "Vargaftik",
                    "email": "wahr36@gmail.com",
                    "institution": "Kurnakov Institute of General and Inorganic Chemistry",

                },
                {

                    "given_name": "Ilya I.",
                    "family_name": "Moiseev",
                    "institution": "Kurnakov Institute of General and Inorganic Chemistry",

                }],

                "license": "https://creativecommons.org/licenses/by-nc/4.0/",
                "collection": "Platinum Acetate Blue",
                "tags": ["transmission electron microscopy", "building blocks", "Pt", "Platinum Acetate Blue", "Characterization", "Platinum acetate", "Typical PAB samples", "III", "PtII", "synthesis", "II", "scanning electron microscopy", "oxidation state", "quantum chemical density", "platinum complexes"],
                "description": "A noncrystalline platinum acetate blue (PAB) of the empirical formula Pt(OOCMe)2.5±0.25 was proposed as an easily prepared starting material instead of the crystalline platinum(II) acetate. Three new platinum(II) and (III) complexes: PtII(dipy)(OOCMe)2, PtIII2(OOCMe)4(O3SPhMe)2, and PtII(μ−OOCMe)4CoII(OH2) were synthesized using PAB as starting material and structurally characterized with X-ray diffraction analysis. The combined EXAFS, SEM, TEM, XRD, DTA-TG, magnetochemical, and DFT−MM+ study revealed the chemical nature of the PAB with the main structural unit Pt9(OOCMe)23.",
                "year": 2014,

                "links": {

                    "landing_page": "https://figshare.com/articles/Platinum_Acetate_Blue_Synthesis_and_Characterization/2263327",
                    "publication": ["https://doi.org/10.1021/ic500940a"],
                    #"data_doi": "",
                    #"related_id": ,

                    "zip": {

                        #"globus_endpoint": ,
                        "http_host": "https://ndownloader.figshare.com",

                        "path": "/files/3899320",
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
    for data_file in tqdm(find_files(input_path, "cif"), desc="Processing files", disable=not verbose):
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "cif")
        ## Metadata:record
        record_metadata = {
            "mdf": {

                "title": "Platinum Acetate Blue - " + record["chemical_formula"],
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

                    "cif": {

                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",

                        "path": "/collections/pt_acetate_blue/" + data_file["filename"],
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
