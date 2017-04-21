import os

from tqdm import tqdm

from validator import Validator
from parsers.utils import find_files
from parsers.ase_parser import parse_ase

# This is the converter for the QM MD Trajectories of C7O2H10 dataset
# Arguments:
#   input_path (string): The file or directory where the data resides. This should not be hard-coded in the function, for portability.
#   verbose (bool): Should the script print status messages to standard output? Default False.
def convert(input_path, verbose=False):

    # Collect the metadata
    dataset_metadata = {
        "globus_subject": "http://quantum-machine.org/datasets/#C7O2H10",
        "acl": ["public"],
        "mdf_source_name": "qm_mdt_c",
        "mdf-publish.publication.collection": "Quantum Machine",

        "cite_as": ["S. Chmiela, A. Tkatchenko, H. E. Sauceda, I. Poltavsky, K. T. Schütt, K.-R. Müller Machine Learning of Accurate Energy-Conserving Molecular Force Fields, 2017.", "K. T. Schütt, F. Arbabzadah, S. Chmiela, K.-R. Müller, A. Tkatchenko Quantum-Chemical Insights from Deep Tensor Neural Networks, Nat. Commun. 8, 13890, 2017."],
        "dc.title": "Quantum Machine - MD Trajectories of C7O2H10",
        "dc.creator": "Quantum Machine",
        "dc.identifier": "http://quantum-machine.org/datasets/#md-datasets",
        "dc.contributor.author": ["L. Ruddigkeit", "R. van Deursen", "L. C. Blum", "J.-L. Reymond", "R. Ramakrishnan", "P. O. Dral", "M. Rupp", "O. A. von Lilienfeld"],
        "dc.subject": ["molecular", "dynamics", "trajectories", "DFT", "density functional theory", "PBE", "exchange"],
        "dc.description": "This data set consists of molecular dynamics trajectories of 113 randomly selected C7O2H10 isomers calculated at a temperature of 500 K and resolution of 1fs using density functional theory with the PBE exchange-correlation potential.",
        "dc.relatedidentifier": ["http://quantum-machine.org/data/c7o2h10_md.tar.gz"],
        "dc.year": 2016
        }


    # Make a Validator to help write the feedstock
    dataset_validator = Validator(dataset_metadata)


    # Get the data
    # Each record also needs its own metadata
    for file_data in tqdm(find_files(input_path, "xyz"), desc="Processing QM_MDT_C", disable= not verbose):
        file_path = os.path.join(file_data["path"], file_data["filename"])
        record = parse_ase(file_path, "xyz")
        record_metadata = {
            "globus_subject": "https://data.materialsdatafacility.org/collections/test/md_trajectories_of_c7o2h10/c7o2h10_md/" + file_data["no_root_path"] + "/" if file_data["no_root_path"] else "" + file_data["filename"],
            "acl": ["public"],
            "mdf-publish.publication.collection": "Quantum Machine",
            "mdf_data_class": "xyz",
            "mdf-base.material_composition": record.get("chemical_formula", ""),

            "dc.title": "MD Trajectories of C7O2H10 - " + record.get("chemical_formula", "") + " - " + file_data["filename"],
            "dc.creator": "Quantum Machine",
            "dc.identifier": "http://quantum-machine.org/datasets/#md-datasets",
            #"dc.contributor.author": ,               # OPT list of strings: Author(s) of record (if different from dataset)
            #"dc.subject": ,                          # OPT list of strings: Keywords about record
            #"dc.description": ,                      # OPT string: Description of record
            #"dc.relatedidentifier": ,                # OPT list of strings: Link(s) to related materials (if different from dataset)
            #"dc.year": ,                             # OPT integer: Year of record creation (if different from dataset)

            "data": {
                #"raw": ,                             # RCM string: Original data record text, if feasible
                "files": {
                    "xyz" : "https://data.materialsdatafacility.org/collections/test/md_trajectories_of_c7o2h10/c7o2h10_md/" + file_data["filename"],
                    "energy.dat" : "https://data.materialsdatafacility.org/collections/test/md_trajectories_of_c7o2h10/c7o2h10_md/" + file_data["filename"].replace(".xyz", "") + ".energy.dat"
                    },
                "temperature" : {
                    "value": 500,
                    "unit": "kelvin"
                    },
                "resolution" : {
                    "value" : 1,
                    "unit" : "femtosecond"
                    }
                }
            }

        # Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)

        # Check if the Validator accepted the record, and print a message if it didn't
        # If the Validator returns "success" == True, the record was written successfully
        if result["success"] is not True:
            print("Error:", result["message"], ":", result.get("invalid_data", ""))

    if verbose:
        print("Finished converting")


# Optionally, you can have a default call here for testing
# The convert function may not be called in this way, so code here is primarily for testing
if __name__ == "__main__":
    import paths
    convert(paths.datasets+"quantum_machine/md_trajectories_of_c7o2h10/c7o2h10_md", True)
