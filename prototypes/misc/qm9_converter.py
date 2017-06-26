import os

from tqdm import tqdm

from validator import Validator
from parsers.utils import find_files
from parsers.ase_parser import parse_ase

# This is the converter for the QM9 dataset
# Arguments:
#   input_path (string): The file or directory where the data resides. This should not be hard-coded in the function, for portability.
#   verbose (bool): Should the script print status messages to standard output? Default False.
def convert(input_path, verbose=False):

    # Collect the metadata
    dataset_metadata = {
        "globus_subject": "http://quantum-machine.org/datasets/#qm9",
        "acl": ["public"],
        "mdf_source_name": "qm9",
        "mdf-publish.publication.collection": "Quantum Machine",
        "mdf_data_class": "xyz",

        "cite_as": ["L. Ruddigkeit, R. van Deursen, L. C. Blum, J.-L. Reymond, Enumeration of 166 billion organic small molecules in the chemical universe database GDB-17, J. Chem. Inf. Model. 52, 2864–2875, 2012.", "R. Ramakrishnan, P. O. Dral, M. Rupp, O. A. von Lilienfeld, Quantum chemistry structures and properties of 134 kilo molecules, Scientific Data 1, 140022, 2014."],
        "dc.title": "Quantum Machine - QM9",
        "dc.creator": "Quantum Machine",
        "dc.identifier": "http://quantum-machine.org/datasets/#qm9",
        "dc.contributor.author": ["L. Ruddigkeit", "R. van Deursen", "L. C. Blum", "J.-L. Reymond", "R. Ramakrishnan", "P. O. Dral", "M. Rupp", "O. A. von Lilienfeld"],
        "dc.subject": ["gdb-17"],
        "dc.description": ("Computational de novo design of new drugs and materials requires rigorous and unbiased exploration of chemical compound space. "
                           "However, large uncharted territories persist due to its size scaling combinatorially with molecular size. We report computed geometric, "
                           "energetic, electronic, and thermodynamic properties for 134k stable small organic molecules made up of CHONF. These molecules correspond "
                           "to the subset of all 133,885 species with up to nine heavy atoms (CONF) out of the GDB-17 chemical universe of 166 billion organic "
                           "molecules. We report geometries minimal in energy, corresponding harmonic frequencies, dipole moments, polarizabilities, along with "
                           "energies, enthalpies, and free energies of atomization. All properties were calculated at the B3LYP/6-31G(2df,p) level of quantum "
                           "chemistry. Furthermore, for the predominant stoichiometry, C7H10O2, there are 6,095 constitutional isomers among the 134k molecules. We "
                           "report energies, enthalpies, and free energies of atomization at the more accurate G4MP2 level of theory for all of them. As such, this "
                           "data set provides quantum chemical properties for a relevant, consistent, and comprehensive chemical space of small organic molecules. "
                           "This database may serve the benchmarking of existing methods, development of new methods, such as hybrid quantum mechanics/machine "
                           "learning, and systematic identification of structure-property relationships."),
        "dc.relatedidentifier": ["https://doi.org/10.6084/m9.figshare.978904"],
        "dc.year": 2014
        }

    # Make a Validator to help write the feedstock
    dataset_validator = Validator(dataset_metadata)

    # Get the data
    # Each record also needs its own metadata
    for file_data in tqdm(find_files(input_path, "xyz"), desc="Processing QM9", disable= not verbose):
        file_path = os.path.join(file_data["path"], file_data["filename"])
        record = parse_ase(file_path, "xyz")
        record_metadata = {
            "globus_subject": "https://data.materialsdatafacility.org/collections/test/qm9/" + file_data["no_root_path"] + "/" + file_data["filename"],
            "acl": ["public"],
            "mdf-publish.publication.collection": "Quantum Machine",
            "mdf-base.material_composition": record.get("chemical_formula", ""),

            "dc.title": "QM9 - " + record.get("chemical_formula", "") + " - " + file_data["filename"],
            "dc.creator": "Quantum Machine",
            "dc.identifier": "http://quantum-machine.org/datasets/#qm9",
            #"dc.contributor.author": ,               # OPT list of strings: Author(s) of record (if different from dataset)
            #"dc.subject": ,                          # OPT list of strings: Keywords about record
            #"dc.description": ,                      # OPT string: Description of record
            #"dc.relatedidentifier": ,                # OPT list of strings: Link(s) to related materials (if different from dataset)
            #"dc.year": ,                             # OPT integer: Year of record creation (if different from dataset)

            "data": {
                #"raw": ,                             # RCM string: Original data record text, if feasible
                "files": {
                    "xyz" : "https://data.materialsdatafacility.org/collections/test/qm9/" + file_data["no_root_path"] + "/" + file_data["filename"]
                    },
                "quantum chemistry level" : {
                    "B3LYP/6-31G(2df,p)"
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
    convert(paths.datasets+"quantum_machine/qm9", True)
