import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: Synthesis of Ti3AuC2, Ti3Au2C2 and Ti3IrC2 by noble-metal substitution reaction in Ti3SiC2 for high-temperature-stable ohmic contacts to SiC
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

                "title": "Synthesis of Ti3AuC2, Ti3Au2C2 and Ti3IrC2 by noble-metal substitution reaction in Ti3SiC2 for high-temperature-stable ohmic contacts to SiC",
                "acl": ["public"],
                "source_name": "ohmic_si_c_contacts",

                "data_contact": {
                    
                    "given_name": "Per",
                    "family_name": "Eklund",
                    "email": "perek@ifm.liu.se",
                    "institution": "Department of Physics, Chemistry, and Biology (IFM), Linköping University",

                },

                "data_contributor": [{
                    
                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78",

                }],

                "citation": ["Fashandi, Hossein, Dahlqvist, Martin, Lu, Jun, Palisaitis, Justinas, Simak, Sergei I, Abrikosov, Igor A, … Eklund, Per. (2017). Synthesis of Ti3AuC2, Ti3Au2C2 and Ti3IrC2 by noble-metal substitution reaction in Ti3SiC2 for high-temperature-stable ohmic contacts to SiC [Data set]. Zenodo. http://doi.org/10.5281/zenodo.376969"],

                "author": [{

                    "given_name": "Hossein",
                    "family_name": "Fashandi",
                    "institution": "Department of Physics, Chemistry, and Biology (IFM), Linköping University",

                },
                {

                    "given_name": "Martin",
                    "family_name": "Dahlqvist",
                    "institution": "Department of Physics, Chemistry, and Biology (IFM), Linköping University",

                },
                {

                    "given_name": "Jun",
                    "family_name": "Lu",
                    "institution": "Department of Physics, Chemistry, and Biology (IFM), Linköping University",

                },
                {

                    "given_name": "Justinas",
                    "family_name": "Palisaitis",
                    "institution": "Department of Physics, Chemistry, and Biology (IFM), Linköping University",

                },
                {

                    "given_name": "Sergei I",
                    "family_name": "Simak",
                    "institution": "Department of Physics, Chemistry, and Biology (IFM), Linköping University",

                },
                {

                    "given_name": "Igor A",
                    "family_name": "Abrikosov",
                    "institution": "Department of Physics, Chemistry, and Biology (IFM), Linköping University",

                },
                {

                    "given_name": "Johanna",
                    "family_name": "Rosen",
                    "institution": "Department of Physics, Chemistry, and Biology (IFM), Linköping University",

                },
                {

                    "given_name": "Lars",
                    "family_name": "Hultman",
                    "institution": "Department of Physics, Chemistry, and Biology (IFM), Linköping University",

                },
                {

                    "given_name": "Mike",
                    "family_name": "Andersson",
                    "institution": "Department of Physics, Chemistry, and Biology (IFM), Linköping University",

                },
                {

                    "given_name": "Anita Lloyd",
                    "family_name": "Spetz",
                    "institution": "Department of Physics, Chemistry, and Biology (IFM), Linköping University",

                },
                {

                    "given_name": "Per",
                    "family_name": "Eklund",
                    "email": "perek@ifm.liu.se",
                    "institution": "Department of Physics, Chemistry, and Biology (IFM), Linköping University",

                }],

                "license": "https://creativecommons.org/licenses/by/4.0/",
                "collection": "Ohmic Contact to SiC",
                "tags": ["electronic structure calculations", "MAX phase", "XRD", "I/V measurement", "spin-orbit coupling", "density of states", "Nanoscale materials", "Structure of solids and liquids", "Surfaces, interfaces and thin films", "Two-dimensional materials"],
                "description": "The large class of layered ceramics encompasses both van der Waals (vdW) and non-vdW solids. While intercalation of noble metals in vdW solids is known, formation of compounds by incorporation of noble-metal layers in non-vdW layered solids is largely unexplored. Here, we show formation of Ti3AuC2 and Ti3Au2C2 phases with up to 31% lattice swelling by a substitutional solid-state reaction of Au into Ti3SiC2 single-crystal thin films with simultaneous out-diffusion of Si. Ti3IrC2 is subsequently produced by a substitution reaction of Ir for Au in Ti3Au2C2. These phases form Ohmic electrical contacts to SiC and remain stable after 1,000 h of ageing at 600 °C in air. The present results, by combined analytical electron microscopy and ab initio calculations, open avenues for processing of noble-metal-containing layered ceramics that have not been synthesized from elemental sources, along with tunable properties such as stable electrical contacts for high-temperature power electronics or gas sensors.",
                "year": 2017,

                "links": {

                    "landing_page": "https://doi.org/10.5281/zenodo.376969",
                    "publication": ["http://www.nature.com/nmat/journal/v16/n8/full/nmat4896.html"],
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
    errors=0
    for data_file in tqdm(find_files(input_path, "(OUTCAR|cif$)"), desc="Processing files", disable=not verbose):
        dtype = data_file["filename"].split(".")[-1]
        if dtype == "cif":
            ftype = "cif"
        else:
            ftype = "vasp-out"
        try:
            record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), ftype)
        except:
            errors+=1
        ## Metadata:record
        record_metadata = {
            "mdf": {

                "title": "Ohmic Contact to SiC - " + record["chemical_formula"],
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

                        "path": "/collections/ohmic_si_c_contacts/" + data_file["no_root_path"] + "/" + data_file["filename"],
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
        print("Errors: " + str(errors))
        print("Finished converting")
