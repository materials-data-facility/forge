import pymatgen as mg


def parse_pymatgen(path, structure=True, molecule=True):
    data = {}
    if structure:
        try:
            struct = mg.Structure.from_file(path)
        except Exception:
            struct = None
            struct_data = {}
        if struct:
            struct_data = {
                "material_composition": struct.formula
                }
        data["structure"] = struct_data

    if molecule:
        try:
            molec = mg.Molecule.from_file(path)
        except Exception:
            molec = None
            molec_data = {}
        if molec:
            molec_data = {
                "material_composition": molec.formula
                }
        data["molecule"] = molec_data

    return data


if __name__ == "__main__":
    print("\nThis is the parser for Pymatgen molecule and/or structure data.")
    print("USAGE:\n\nparse_pymatgen(path, structure=True, molecule=True)")
    print("Arguments:\n\tpath: The path to the file to parse")
    print("\tstructure: Try to parse as a Structure? Default True.")
    print("\tmolecule: Try to parse as a Molecule? Default True.")
    print("Returns: A dictionary containing the parsed data in the form {'structure': structure_data, 'molecule': molecule_data}.")
    print("\tThe data for both types is currently as follows: {'material_composition': composition}")
    print("\tThe return dictionary will always have exactly the parse arguments that are True.")
    print("\tIf the file cannot parse as a given type, the value will be an empty dictionary.")
