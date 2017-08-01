import json
import sys
import os

from tqdm import tqdm

from mdf_refinery.validator import Validator
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase

# VERSION 0.3.0

# This is the converter for the Doak Strain Energies dataset
# Arguments:
#   input_path (string): The file or directory where the data resides.
#       NOTE: Do not hard-code the path to the data in the converter. The converter should be portable.
#   metadata (string or dict): The path to the JSON dataset metadata file, a dict or json.dumps string containing the dataset metadata, or None to specify the metadata here. Default None.
#   verbose (bool): Should the script print status messages to standard output? Default False.
#       NOTE: The converter should have NO output if verbose is False, unless there is an error.
def convert(input_path, metadata=None, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    if not metadata:
        dataset_metadata = {
        "mdf": {
            "title": "GeTe-PbTe PbS-PbTe PbSe-PbS PbTe-PbSe PbTe-SnTe SnTe-GeTe mixing and coherency strain energies",
            "acl": ["public"],
            "source_name": "doak_strain_energies",
            "citation": ["Doak JW, Wolverton C (2012) Coherent and incoherent phase stabilities of thermoelectric rocksalt IV-VI semiconductor alloys. Phys. Rev. B 86: 144202 http://dx.doi.org/10.1103/PhysRevB.86.144202"],
            "data_contact": {

                "given_name": "Chris",
                "family_name": "Wolverton",

                "email": "c-wolverton@northwestern.edu",
                "institution": "Northwestern University",

                },

            "author": [{

                "given_name": "Chris",
                "family_name": "Wolverton",

                "email": "c-wolverton@northwestern.edu",
                "institution": "Northwestern University",

                },{

                "given_name": "Jeff",
                "family_name": "Doak",

                "institution": "Northwestern University",

                }],

#            "license": ,

            "collection": "Doak Strain Energies",
            "tags": ["dft"],

            "description": "We use density functional theory calculations to investigate the coherent and incoherent phase stability of the IV–VI rocksalt semiconductor alloy systems Pb(S,Te), Pb(Te,Se), Pb(Se,S), (Pb,Sn)Te, (Sn,Ge)Te, and (Ge,Pb)Te.",
            "year": 2012,

            "links": {

                "landing_page": "https://materialsdata.nist.gov/dspace/xmlui/handle/11256/85",

                "publication": "http://dx.doi.org/10.1103/PhysRevB.86.144202",
                "data_doi": "http://hdl.handle.net/11256/85",

#                "related_id": ,

                # data links: {

                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #}
                },

#            "mrr": ,

            "data_contributor": {
                "given_name": "Jonathon",
                "family_name": "Gaff",
                "email": "jgaff@uchicago.edu",
                "institution": "The University of Chicago",
                "github": "jgaff"
                }
            }
        }
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


    dataset_validator = Validator(dataset_metadata)


    # Get the data
    for data_file in tqdm(find_files(input_path, "OUTCAR"), desc="Processing files", disable= not verbose):
        data = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "vasp-out")
        record_metadata = {
        "mdf": {
            "title": "Strain Energy - " + data["chemical_formula"],
            "acl": ["public"],

#            "tags": ,
#            "description": ,
            
            "composition": data["chemical_formula"],
#            "raw": ,

            "links": {
#                "landing_page": ,

#                "publication": ,
#                "dataset_doi": ,

 #               "related_id": ,

                "outcar": {
 
                    "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                    "http_host": "https://data.materialsdatafacility.org",

                    "path": "/collections/" + data_file["no_root_path"] + "/" + data_file["filename"],
                    },
                },

#            "citation": ,
#            "data_contact": {

#                "given_name": ,
#                "family_name": ,

#                "email": ,
#                "institution":,

                # IDs
#                },

#            "author": ,

#            "license": ,
#            "collection": ,
#            "data_format": ,
#            "data_type": ,
#            "year": ,

#            "mrr":

#            "processing": ,
#            "structure":,
            }
        }

        # Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)

        # Check if the Validator accepted the record, and print a message if it didn't
        # If the Validator returns "success" == True, the record was written successfully
        if result["success"] is not True:
            print("Error:", result["message"])


    if verbose:
        print("Finished converting")
