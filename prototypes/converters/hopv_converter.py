from json import dump
from tqdm import tqdm

import paths

hopv_file = paths.datasets + "hopv/HOPV_15_revised_2.data"
feedstock_file = paths.raw_feed + "hopv_all.json"
feedsack_size = 10
feedsack_file = paths.sack_feed + "hopv_" + str(feedsack_size) + ".json"

#Takes float or nan and returns correct value for JSON serialization - float version if not nan, string "nan" otherwise
#Currently unused, as only the package 'ujson' requires it, and this script uses 'json'
def float_nan(value):
	return float(value) if float(value) == float(value) else "nan" #nan != nan

#Takes a/the HOPV file and parses its interesting structure
def hopv_converter(in_filename, out_filename, sack_size=0, sack_filename=None, uri_dup_check=True, verbose=False):
	all_uri = []
	if verbose:
		print("Opening files")
	with open(in_filename, 'r') as in_file:
		with open(out_filename, 'w') as out_file:
			if sack_size > 0 and sack_filename:
				sack_file = open(sack_filename, 'w')
			count = 0
			eof = False
			smiles = in_file.readline() #Priming read
			if not smiles: #Blank line is EOF in Python
				eof = True
			if verbose:
				print("Processing input file")
			while not eof:
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
				for i in tqdm(range(molecule["num_conformers"]), desc="Processing molecule " + str(count + 1), disable= not verbose):
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
				molecule["uri"] = molecule["inchi"]
				all_uri.append(molecule["uri"])
				try:
					dump(molecule, out_file)
					out_file.write("\n")
				except:
					print("Error on:\n", molecule)
					#return
				if sack_filename and count < sack_size:
					dump(molecule, sack_file)
					sack_file.write("\n")
				count += 1
				smiles = in_file.readline() #Next molecule
				if not smiles: #Blank line is EOF
					eof = True
	
			if sack_size > 0 and sack_filename:
				sack_file.close()
	if verbose:
		print("Processed", count, "molecules successfully.")
	duplicates = [x for x in all_uri if all_uri.count(x) > 1]
	if duplicates:
		print("Warning: Duplicate URIs found:\n", set(duplicates))



if __name__ == "__main__":
	hopv_converter(hopv_file, feedstock_file, feedsack_size, feedsack_file, uri_dup_check=True, verbose=True)

