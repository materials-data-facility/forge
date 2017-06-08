import json
from validator import Validator

import paths

base_file = paths.datasets+"hopv/"

# This is the converter for the Harvard Organic Photovoltaic Database.
# Arguments:
#   input_path (string): The file or directory where the data resides. This should not be hard-coded in the function, for portability.
#   verbose (bool): Should the script print status messages to standard output? Default False.
def convert(input_path, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    dataset_metadata = {
        "globus_subject": "https://figshare.com/articles/HOPV15_Dataset/1610063/4",
        "acl": ["public"],
        "mdf_source_name": "hopv",
        "mdf-publish.publication.collection": "Harvard Organic Photovoltaic Dataset",

        "cite_as": ["Aspuru-Guzik, Alan (2016): The Harvard Organic Photovoltaics 2015 (HOPV) dataset: An experiment-theory calibration resource.. Figshare. https://doi.org/10.6084/m9.figshare.1610063.v4"],
        "dc.title": "Harvard Organic Photovoltaic Dataset",
        "dc.creator": "Harvard University",
        "dc.identifier": "https://dx.doi.org/10.6084/m9.figshare.1610063.v4",
        "dc.contributor.author": ["Aspuru-Guzik, Alan"],
        "dc.subject": ["Organic Photovoltaic Cells", "quantum chemistry", "density functional theory", "calibration"],
#        "dc.description": ,
        "dc.relatedidentifier": ["https://dx.doi.org/10.1038/sdata.2016.86"],
        "dc.year": 2016
        }


    # Make a Validator to help write the feedstock
    # You must pass the metadata to the constructor
    # Each Validator instance can only be used for a single dataset
    dataset_validator = Validator(dataset_metadata)


    # Get the data
    # Each record also needs its own metadata
    with open(input_path, 'r') as in_file:
        index = 0
        eof = False
        smiles = in_file.readline() # Priming read
        if not smiles:
            eof = True
        while not eof:
            index += 1
            filename = "hopv_" + str(index) + ".txt"
            #Molecule level
            molecule = {}
            molecule["smiles"] = smiles.strip()
            molecule["inchi"] = in_file.readline().strip()
            exp_data = in_file.readline().strip().split(',')
            molecule["experimental_data"] = {
                "doi" : exp_data[0],
                "inchikey" : exp_data[1],
                "construction" : exp_data[2],
                "architecture" : exp_data[3],
                "complement" : exp_data[4],
                "homo" : float(exp_data[5]),
                "lumo" : float(exp_data[6]),
                "electrochemical_gap" : float(exp_data[7]),
                "optical_gap" : float(exp_data[8]),
                "pce" : float(exp_data[9]),
                "voc" : float(exp_data[10]),
                "jsc" : float(exp_data[11]),
                "fill_factor" : float(exp_data[12])
                }
            molecule["pruned_smiles"] = in_file.readline().strip()
            molecule["num_conformers"] = int(in_file.readline().strip())
            #Conformer level
            list_conformers = []
            for i in range(molecule["num_conformers"]):
                conformer = {}
                conformer["conformer_number"] = int(in_file.readline().strip("\n Cconformer"))
                conformer["num_atoms"] = int(in_file.readline().strip())
                #Atom level
                list_atoms = []
                for j in range(conformer["num_atoms"]):
                    atom_data = in_file.readline().strip().split(' ')
                    atom = {
                        "element" : atom_data[0],
                        "x_coordinate" : float(atom_data[1]),
                        "y_coordinate" : float(atom_data[2]),
                        "z_coordinate" : float(atom_data[3])
                        }
                    list_atoms.append(atom)
                conformer["atoms"] = list_atoms
                #Four sets of calculated data
                list_calc = []
                for k in range(4):
                    calc_data = in_file.readline().strip().split(",")
                    calculated = {
                        "set_description" : calc_data[0],
                        "homo" : float(calc_data[1]),
                        "lumo" : float(calc_data[2]),
                        "gap" : float(calc_data[3]),
                        "scharber_pce" : float(calc_data[4]),
                        "scharber_voc" : float(calc_data[5]),
                        "scharber_jsc" : float(calc_data[6])
                        }
                    list_calc.append(calculated)
                conformer["calculated_data"] = list_calc
                list_conformers.append(conformer)
            molecule["conformers"] = list_conformers

            uri = "https://data.materialsdatafacility.org/collections/hopv/" + filename
            record_metadata = {
#                "globus_subject": "hopv://" + molecule["smiles"],
                "globus_subject": uri,
                "acl": ["public"],
                "mdf-publish.publication.collection": "Harvard Organic Photovoltaic Dataset",
#                "mdf_data_class": ,
                "mdf-base.material_composition": molecule["smiles"],

                "dc.title": "HOPV - " + molecule["smiles"],
#                "dc.creator": ,
                "dc.identifier": uri,
#                "dc.contributor.author": ,
#                "dc.subject": ,
#                "dc.description": ,
#                "dc.relatedidentifier": ,
#                "dc.year": ,

                "data": {
#                    "raw": json.dumps(molecule),
                    "experimental_data": json.dumps(molecule["experimental_data"]),
                    "files": {
                        "molecule": uri,
                        "original": "https://data.materialsdatafacility.org/collections/hopv/HOPV_15_revised_2.data"
                        }
                    }
                }

            # Pass each individual record to the Validator
            result = dataset_validator.write_record(record_metadata)

            # Check if the Validator accepted the record, and print a message if it didn't
            # If the Validator returns "success" == True, the record was written successfully
            if result["success"] is not True:
                print("Error:", result["message"], ":", result.get("invalid_metadata", ""))
            else:
                with open(base_file + filename, 'w') as outfile:
                    json.dump(molecule, outfile)

            smiles = in_file.readline() #Next molecule
            if not smiles: #Blank line is EOF
                eof = True

    if verbose:
        print("Finished converting")


# Optionally, you can have a default call here for testing
# The convert function may not be called in this way, so code here is primarily for testing
if __name__ == "__main__":
    import paths
    convert(paths.datasets + "hopv/HOPV_15_revised_2.data", True)
