import os
from pickle import dump
from tqdm import tqdm
from ase.io import read
import re
import json

#Pick one or more datasets to process
datasets_to_process = []
#datasets_to_process.append("danemorgan")
#datasets_to_process.append("khazana_polymer")
datasets_to_process.append("khazana_vasp")

#Export data to a JSON feedstock file?
feedstock = False


#Currently-supported formats are listed below.
#	VASP ('vasp'): Processed as directory containing OUTCAR file.
#	.CIF ('cif'): Processed as individual files.
supported_formats = ['vasp', 'cif']

def read_vasp(filename='CONTCAR'):
    """Import POSCAR/CONTCAR type file.

    Reads unitcell, atom positions and constraints from the POSCAR/CONTCAR
    file and tries to read atom types from POSCAR/CONTCAR header, if this fails
    the atom types are read from OUTCAR or POTCAR file.
    """

    from ase import Atoms
    from ase.constraints import FixAtoms, FixScaled
    from ase.data import chemical_symbols
    import numpy as np

    if isinstance(filename, str):
        f = open(filename)
    else:  # Assume it's a file-like object
        f = filename

    # The first line is in principle a comment line, however in VASP
    # 4.x a common convention is to have it contain the atom symbols,
    # eg. "Ag Ge" in the same order as later in the file (and POTCAR
    # for the full vasp run). In the VASP 5.x format this information
    # is found on the fifth line. Thus we save the first line and use
    # it in case we later detect that we're reading a VASP 4.x format
    # file.
    line1 = f.readline()

    lattice_constant = float(f.readline().split()[0])

    # Now the lattice vectors
    a = []
    for ii in range(3):
        s = f.readline().split()
        floatvect = float(s[0]), float(s[1]), float(s[2])
        a.append(floatvect)

    basis_vectors = np.array(a) * lattice_constant

    # Number of atoms. Again this must be in the same order as
    # in the first line
    # or in the POTCAR or OUTCAR file
    atom_symbols = []
    numofatoms = f.readline().split()
    # Check whether we have a VASP 4.x or 5.x format file. If the
    # format is 5.x, use the fifth line to provide information about
    # the atomic symbols.
    vasp5 = False
    try:
        int(numofatoms[0])
    except ValueError:
        vasp5 = True
        atomtypes = numofatoms
        numofatoms = f.readline().split()

    # check for comments in numofatoms line and get rid of them if necessary
    commentcheck = np.array(['!' in s for s in numofatoms])
    if commentcheck.any():
        # only keep the elements up to the first including a '!':
        numofatoms = numofatoms[:np.arange(len(numofatoms))[commentcheck][0]]

    if not vasp5:
        atomtypes = line1.split()

        numsyms = len(numofatoms)
        if len(atomtypes) < numsyms:
            # First line in POSCAR/CONTCAR didn't contain enough symbols.

            # Sometimes the first line in POSCAR/CONTCAR is of the form
            # "CoP3_In-3.pos". Check for this case and extract atom types
            if len(atomtypes) == 1 and '_' in atomtypes[0]:
                atomtypes = get_atomtypes_from_formula(atomtypes[0])
            else:
                atomtypes = atomtypes_outpot(f.name, numsyms)
        else:
            try:
                for atype in atomtypes[:numsyms]:
                    if atype not in chemical_symbols:
                        raise KeyError
            except KeyError:
                atomtypes = atomtypes_outpot(f.name, numsyms)

    for i, num in enumerate(numofatoms):
        numofatoms[i] = int(num)
        [atom_symbols.append(atomtypes[i]) for na in range(numofatoms[i])]

    # Check if Selective dynamics is switched on
    sdyn = f.readline()
    selective_dynamics = sdyn[0].lower() == 's'

    # Check if atom coordinates are cartesian or direct
    if selective_dynamics:
        ac_type = f.readline()
    else:
        ac_type = sdyn
    cartesian = ac_type[0].lower() == 'c' or ac_type[0].lower() == 'k'
    tot_natoms = sum(numofatoms)
    atoms_pos = np.empty((tot_natoms, 3))
    if selective_dynamics:
        selective_flags = np.empty((tot_natoms, 3), dtype=bool)
    for atom in range(tot_natoms):
        ac = f.readline().split()
        atoms_pos[atom] = (float(ac[0]), float(ac[1]), float(ac[2]))
        if selective_dynamics:
            curflag = []
            for flag in ac[3:6]:
                curflag.append(flag == 'F')
            selective_flags[atom] = curflag
    # Done with all reading
    if isinstance(filename, str):
        f.close()
    if cartesian:
        atoms_pos *= lattice_constant
    atoms = Atoms(symbols=atom_symbols, cell=basis_vectors, pbc=True)
    if cartesian:
        atoms.set_positions(atoms_pos)
    else:
        atoms.set_scaled_positions(atoms_pos)
    if selective_dynamics:
        constraints = []
        indices = []
        for ind, sflags in enumerate(selective_flags):
            if sflags.any() and not sflags.all():
                constraints.append(FixScaled(atoms.get_cell(), ind, sflags))
            elif sflags.all():
                indices.append(ind)
        if indices:
            constraints.append(FixAtoms(indices))
        if constraints:
            atoms.set_constraint(constraints)
    return atoms


def read_vasp_out(filename=None, index=-1, force_consistent=False):
    """Import OUTCAR type file.

    Reads unitcell, atom positions, energies, and forces from the OUTCAR file
    and attempts to read constraints (if any) from CONTCAR/POSCAR, if present.
    """
    import numpy as np
    from ase.calculators.singlepoint import SinglePointCalculator
    from ase import Atoms, Atom

    if not filename:
    	filename = os.path.join(os.getcwd(), 'OUTCAR')

    data_path = os.path.dirname(filename)

    try:  # try to read constraints, first from CONTCAR, then from POSCAR
        constr = read_vasp(os.path.join(data_path, 'CONTCAR')).constraints
    except Exception:
        try:
            constr = read_vasp(os.path.join(data_path, 'POSCAR')).constraints
        except Exception:
            constr = None

    if isinstance(filename, str):
        f = open(filename)
    else:  # Assume it's a file-like object
        f = filename
    data = f.readlines()
    natoms = 0
    images = []
    atoms = Atoms(pbc=True, constraint=constr)
    energy = 0
    species = []
    species_num = []
    symbols = []
    ecount = 0
    poscount = 0
    magnetization = []

    for n, line in enumerate(data):
        if 'POTCAR:' in line:
            temp = line.split()[2]
            for c in ['.', '_', '1']:
                if c in temp:
                    temp = temp[0:temp.find(c)]
            species += [temp]
        if 'ions per type' in line:
            species = species[:len(species) // 2]
            temp = line.split()
            for ispecies in range(len(species)):
                species_num += [int(temp[ispecies + 4])]
                natoms += species_num[-1]
                for iatom in range(species_num[-1]):
                    symbols += [species[ispecies]]
        if 'direct lattice vectors' in line:
            cell = []
            for i in range(3):
                temp = data[n + 1 + i].split()
                cell += [[float(temp[0]), float(temp[1]), float(temp[2])]]
            atoms.set_cell(cell)
        if 'FREE ENERGIE OF THE ION-ELECTRON SYSTEM' in line:
            # choose between energy wigh smearing extrapolated to zero
            # or free energy (latter is consistent with forces)
            energy_zero = float(data[n + 4].split()[6])
            energy_free = float(data[n + 2].split()[4])
            energy = energy_zero
            if force_consistent:
                energy = energy_free
            if ecount < poscount:
                # reset energy for LAST set of atoms, not current one -
                # VASP 5.11? and up
                images[-1].calc.results['energy'] = energy
                images[-1].calc.set(energy=energy)
            ecount += 1
        if 'magnetization (x)' in line:
            magnetization = []
            for i in range(natoms):
                magnetization += [float(data[n + 4 + i].split()[4])]
        if 'POSITION          ' in line:
            forces = []
            positions = []
            for iatom in range(natoms):
                temp = data[n + 2 + iatom].split()
                atoms += Atom(symbols[iatom],
                              [float(temp[0]), float(temp[1]), float(temp[2])])
                forces += [[float(temp[3]), float(temp[4]), float(temp[5])]]
                positions += [[float(temp[0]), float(temp[1]), float(temp[2])]]
                atoms.set_calculator(SinglePointCalculator(atoms,
                                                           energy=energy,
                                                           forces=forces))
            images += [atoms]
            if len(magnetization) > 0:
                images[-1].calc.magmoms = np.array(magnetization, float)
            atoms = Atoms(pbc=True, constraint=constr)
            poscount += 1

    # return requested images, code borrowed from ase/io/trajectory.py
    if isinstance(index, int):
        return images[index]
    else:
        step = index.step or 1
        if step > 0:
            start = index.start or 0
            if start < 0:
                start += len(images)
            stop = index.stop or len(images)
            if stop < 0:
                stop += len(images)
        else:
            if index.start is None:
                start = len(images) - 1
            else:
                start = index.start
                if start < 0:
                    start += len(images)
            if index.stop is None:
                stop = -1
            else:
                stop = index.stop
                if stop < 0:
                    stop += len(images)
        return [images[i] for i in range(start, stop, step)]


#Exactly what it says on the tin. Works on file types listed in supported_formats.
#Arguments:
#	file_path: Path to the data file. Default None, which uses the current working directory.
#*	file_name: Name of the specific file to use. Default None. This is required for certain formats.
#*	data_format: Type of data found at the end of the path. Supported formats are listed in the global variable supported_formats. This is REQUIRED.
#	output_file: Name of a file to dump the final JSON to. Will be in pickle format. Default None, which suppresses dumping to file.
#	error_log: A file object (not file name) to log exceptions during processing instead of terminating the program. Default None, which suppresses logging and throws exceptions.
#	verbose: Print status messages? Default False.
def convert_to_json(file_path=None, file_name=None, data_format="", output_file=None, error_log=None, verbose=False):
	ase_dict = {
#		"constraints" : None,
		"all_distances" : None,
		"angular_momentum" : None,
		"atomic_numbers" : None,
		"cell" : None,
		"cell_lengths_and_angles" : None,
		"celldisp" : None,
		"center_of_mass" : None,
		"charges" : None,
		"chemical_formula" : None,
		"chemical_symbols" : None,
		"dipole_moment" : None,
		"forces" : None,
#		"forces_raw" : None,
		"initial_charges" : None,
		"initial_magnetic_moments" : None,
		"kinetic_energy" : None,
		"magnetic_moment" : None,
		"magnetic_moments" : None,
		"masses" : None,
		"momenta" : None,
		"moments_of_inertia" : None,
		"number_of_atoms" : None,
		"pbc" : None,
		"positions" : None,
		"potential_energies" : None,
		"potential_energy" : None,
#		"potential_energy_raw" : None,
		"reciprocal_cell" : None,
		"scaled_positions" : None,
		"stress" : None,
		"stresses" : None,
		"tags" : None,
		"temperature" : None,
		"total_energy" : None,
		"velocities" : None,
		"volume" : None,
		}
	if not file_path:
		file_path = os.getcwd()
	data_format = data_format.lower().strip('.')
	if data_format not in supported_formats:
		print "Error: Invalid data format '" + data_format + "'."
		return None
	elif data_format == 'vasp':
		if error_log:
			try:
				r = read_vasp_out(file_path)
			except Exception as err:
				error_log.write("ERROR: '" + str(err) + "' with the following VASP file:\n")
				error_log.write(file_path + "\n\n")
				#with open(error_log, 'a') as err_file:
				#	err_file.write("ERROR: '" + str(err) + "' with the following file\n:")
				#	err_file.write(outcar_path)
				return None
		else:
			r = read_vasp_out(file_path)
	elif data_format == 'cif':
		#if not file_name:
		#	print "Error: file_name required for 'cif' format."
		#	return None
		#full_path = os.path.join(file_path, file_name)
		if error_log:
			try:
				#r = read_cif(file_path)
				r = read(file_path)
			except Exception as err:
				error_log.write("ERROR: '" + str(err) + "' with the following CIF file:\n")
				error_log.write(file_path + "\n\n")
				#error_log.write(full_path + "\n\n")
				return None
		else:
			#r = read_cif(file_path)
			r = read(file_path)

				
	total_count = 0
	failure_count = 0
	success_count = 0
	none_count = 0
	for key in ase_dict.keys():
		total_count += 1
		try:
			ase_dict[key] = eval("r.get_" + key + "()")
			success_count += 1
		except: #(NotImplementedError, AttributeError): More exception types here than can reasonably be found, just catching everything
			failure_count +=1
			ase_dict[key] = None
	#Populate other fields
	
	total_count += 1
	ase_dict["constraints"] = r.constraints #Should always exist, even if empty
	success_count += 1
	
	total_count += 1
	try:
		ase_dict["forces_raw"] = r.get_forces(apply_constraint=False)
		success_count += 1
	except:
		failure_count += 1
		ase_dict["forces_raw"] = None
	
	total_count += 1
	try:
		ase_dict["potential_energy_raw"] = r.get_potential_energy(apply_constraints=False)
		success_count += 1
	except:
		failure_count += 1
		ase_dict["potential_energy_raw"] = None

#################################################
#	ase_dict["TESTING"] = "dane_morgan_test2"


	#Remove None results
	for key, value in ase_dict.items():
		if value is None:
			ase_dict.pop(key)
			none_count += 1

	#Guide problem children to make better choices
	#Because numpy ndarrays and FixAtoms instances aren't JSON serializable, but we need to convert our data to JSON
	for key in ase_dict.keys():
		if 'numpy' in str(type(ase_dict[key])).lower():
			ase_dict[key] = ase_dict[key].tolist()
		#elif 'fixatoms' in str(type(ase_dict[key])).lower():
		#	ase_dict[key] = ase_dict[key].get_indices().tolist()
		elif type(ase_dict[key]) is list:
			new_list = []
			for elem in ase_dict[key]:
				#if 'numpy' in str(type(elem)).lower():
				#	new_elem = elem.tolist()
				if 'fixatoms' in str(elem).lower():
					new_elem = elem.get_indices().tolist()
				else:
					new_elem = elem
				new_list.append(new_elem)
			ase_dict[key] = new_list

	#Print results
	if verbose:
		print "Processed " + str(total_count) + " items."
		print "There were " + str(success_count) + " successes and " + str(failure_count) + " failures."
		print "No data existed for " + str(none_count - failure_count) + " items." #none_count includes items that are None because they failed, don't want to report failures twice
	
	if output_file:
		with open(output_file, 'w') as out_file:
			dump(ase_dict, out_file)
		if verbose:
			print str(len(ase_dict)) + " valid items written to '" + output_file + "'."
	else:
		if verbose:
			print str(len(ase_dict)) + " valid items saved."

	return ase_dict


#Finds all directories containing a specified type of file and returns list of dicts with path to files and data gleaned from folder names
#root specifies the path to the first dir to start with. Default is current working directory.
#file_match is a string containing the file name to search for. Default is None, which matches all files.
#keep_dir_name_depth is how many layers of dir, counting from the base file up, contain data to save. -1 saves everything in the path. Default is 0, which disables saving anything.
#max_files is the maximum number of results to return. Default -1, which returns all results.
def find_files(root=None, file_pattern=None, keep_dir_name_depth=0, max_files=-1):
	if not root:
		root = os.getcwd()
	dir_list = []
	for path, dirs, files in tqdm(os.walk(root), desc="Finding files:"):
		for one_file in files:
			if not file_pattern or re.match(file_pattern, one_file): #Only care about dirs with desired data
				dir_names = []
				head, tail = os.path.split(path)
				dir_names.append(tail)
				while head:
					head, tail = os.path.split(head)
					dir_names.append(tail)	
				if keep_dir_name_depth >= 0:
					dir_names = dir_names[:keep_dir_name_depth]
				dir_names.reverse() #append leaves path elements in reverse order (/usr/xyz/stuff/ -> [stuff, xyz, usr]), should make right
				#Find and save extension
				name_and_ext = one_file.rsplit('.', 1)
				if name_and_ext[0]: #Hidden files on *nix cause issues ('.name') and are probably not part of data, so ignore them
					dir_data = {
						"path" : path,
						"dirs" : dir_names,
						"filename" : name_and_ext[0]
						}
					try:
						dir_data["extension"] = '.' + name_and_ext[1]
					except IndexError:
						dir_data["extension"] = ''
					dir_list.append(dir_data)
	if max_files >= 0:
		return dir_list[:max_files]
	else:
		return dir_list


#Essentially a main for this script, calls the other functions and ties everything together
#Returns a list of dicts ready for ingestion, and optionally (see below) writes that list out to a file
#arg_dict accepts:
#	metadata: a dict of desired metadata (e.g. globus_source, context, etc.), NOT including globus_subject. Default nothing.
#	uri: the prefix for globus_subject (ex. if uri="http://globus.org/" and uri_adds (see below) is [dir] then a file found in the directory "data/123/" would have globus_subject="http://globus.org/data/123/") Default is empty string.
#	keep_dir_name_depth: how many layers of directory, counting up from the base file, should be saved and added to the URI. -1 saves the entire path. The default (which is lossy but private) is 0.
#
#	root: The directory containing all directories containing processable files. Default is current directory.
#	file_pattern: A regular expression that matches to processable files of interest. Errors will result if this expression matches to files of a different type than specified. Default is None, which matches to all files.
#*	file_format: The format of the files to process. Only one format may be specified. Acceptable formats are listed in the global variable supported_formats. This is *REQUIRED*.
#	verbose: Bool to show system messages. Default is False (messages off).
#	output_file: The file to output the final list to. If this is not set, the output will not be written to file.
#
#	data_exception_log: File to log exceptions caught when processing the data. If a filename is listed here, such exceptions will be logged but otherwise ignored. Default None, which causes exceptions to terminate the program.i
#
#	uri_adds: A list of things to add to the end of 'uri' for each record. Options are 'dir' for the directory structure, 'filename' for the name of the file, and 'ext' for the file extension. 
#		Choose any combination, including none (empty list). The URI will be appended with the directories, filename, and extension, if selected, in that order. The order of uri_adds does not matter. Default 'filename'.
#	max_records: Maximum number of records to return (actual return value may be less due to failed files). Default -1, which returns all (valid) records.
#
def process_data(arg_dict):
	root = arg_dict.get("root", os.getcwd())
	keep_dir_name_depth = arg_dict.get("keep_dir_name_depth", 0)
	verbose  = arg_dict.get("verbose", False)
	file_pattern = arg_dict.get("file_pattern", None)
	uri_adds = arg_dict.get("uri_adds", ['filename'])
	max_records = arg_dict.get("max_records", -1)
	if arg_dict.get("file_format", "NONE") not in supported_formats:
		print "Error: file_format '" + arg_dict.get("file_format", "NONE") + "' is not supported."
		return None

	if arg_dict.get("data_exception_log", None):
		err_log = open(arg_dict["data_exception_log"], 'w')
	else:
		err_log = None
	if verbose:
		print "Finding files"
	dir_list = find_files(root=root, file_pattern=file_pattern, keep_dir_name_depth=keep_dir_name_depth, max_files=max_records)
	if verbose:
		print "Converting file data to JSON"
	all_data_list = []
	good_count = 0
	all_count = 0
	for dir_data in tqdm(dir_list, desc="Processing data files:"):
		all_count += 1
		formatted_data = {}
		uri = arg_dict.get("uri", "")
		full_path = os.path.join(dir_data["path"], dir_data["filename"] + dir_data["extension"])
		file_data = convert_to_json(file_path=full_path, data_format=arg_dict["file_format"], error_log=err_log)
		if file_data:
			file_data["dirs"] = dir_data["dirs"]
			file_data["filename"] = dir_data["filename"]
			file_data["ext"] = dir_data["extension"]

			if 'dir' in uri_adds:
				for dir_name in file_data["dirs"]:
					uri = os.path.join(uri, dir_name)
			if 'filename' in uri_adds:
				uri = os.path.join(uri, file_data["filename"])
			if 'ext' in uri_adds:
				uri += file_data["ext"]
#			formatted_data["uri"] = uri
#			for key, value in arg_dict.get("metadata", {}).iteritems():
#				formatted_data[key] = value
			
			formatted_data = file_data
			formatted_data["uri"] = uri
			all_data_list.append(formatted_data)
			good_count += 1
	
	if err_log:
		err_log.close()
	if verbose:
		print str(good_count) + "/" + str(all_count) + " data files successfully processed"

	if arg_dict.get("output_file", None):
		if verbose:
			print "Writing output to " + arg_dict["output_file"]
		with open(arg_dict["output_file"], 'w') as out_file:
			dump(all_data_list, out_file)
		if verbose:
			print "Data written"
	if verbose:
		print "Processing complete"
	return all_data_list


if __name__ == "__main__":
	print "\nBEGIN"
	
	#Dane Morgan
	if "danemorgan" in datasets_to_process:
		dane_args = {
#			"metadata" : {
#				"globus_source" : "High-throughput Ab-initio Dilute Solute Diffusion Database",
#				"globus_id" : "ddmorgan@wisc.edu cmgtam@globusid.org",
#				"context" : {
#					"dc" : "http://dublincore.org/documents/dcmi-terms"
#					}
#				},
			"uri" : "globus://82f1b5c6-6e9b-11e5-ba47-22000b92c6ec/published/publication_164/data/",
			"keep_dir_name_depth" : 2,
			"root" : "dane_morgan" + os.sep + "data",
			"file_pattern" : "^OUTCAR$",
			"file_format" : "vasp",
			"verbose" : True,
			"output_file" : "dane_morgan" + os.sep + "danemorgan_json.pickle",
			"data_exception_log" : "dane_morgan" + os.sep + "dane_errors.txt",
			"uri_adds" : ["dir"],
			"max_records" : -1
			}
		print "DANE PROCESSING"
		dane = process_data(dane_args)
		if feedstock:
			print "Creating JSON files"
			print "Dane Morgan All"
			with open("dane_morgan/danemorgan_all.json", 'w') as fd1:
				json.dump(dane, fd1)
			print "Done\nDane Morgan 100"
			with open("dane_morgan/danemorgan_100.json", 'w') as fd2:
				json.dump(dane[:100], fd2)
			print "Done\n"

	#Khazana Polymer
	if "khazana_polymer" in datasets_to_process:
		khazana_polymer_args = {
#			"metadata" : {
#				"globus_source" : "",
#				"globus_id" : "khazana",
#				"context" : {
#					"dc" : "http://dublincore.org/documents/dcmi-terms"
#					}
#				},
			"uri" : "http://khazana.uconn.edu/module_search/material_detail.php?id=",
			"keep_dir_name_depth" : 0,
			"root" : "khazana" + os.sep + "polymer_scientific_data_confirmed",
			"file_pattern" : None,
			"file_format" : "cif",
			"verbose" : True,
			"output_file" : "khazana" + os.sep + "khazana_polymer_json.pickle",		
			"data_exception_log" : "khazana" + os.sep + "khazana_polymer_errors.txt",
			"uri_adds" : ["filename"],
			"max_records" : -1
			}
		print "KHAZANA POLYMER PROCESSING"
		khaz_p = process_data(khazana_polymer_args)
		if feedstock:
			print "Creating JSON files"
			print "Khazana Polymer All"
			with open("khazana/khazana_polymer_all.json", 'w') as fk1:
				json.dump(khaz_p, fk1)
			print "Done\nKhazana Polymer 100"
			with open("khazana/khazana_polymer_100.json", 'w') as fk2:
				json.dump(khaz_p[:100], fk2)
			print "Done\n"

	#Khazana VASP
	if "khazana_vasp" in datasets_to_process:
		khazana_vasp_args = {
#			"metadata" : {
#				"globus_source" : "",
#				"globus_id" : "khazana",
#				"context" : {
#					"dc" : "http://dublincore.org/documents/dcmi-terms"
#					}
#				},
			"uri" : "http://khazana.uconn.edu",
			"keep_dir_name_depth" : 0,
			"root" : "khazana" + os.sep + "OUTCARS",
			"file_pattern" : "^OUTCAR",
			"file_format" : "vasp",
			"verbose" : True,
			"output_file" : "khazana" + os.sep + "khazana_vasp_json.pickle",		
			"data_exception_log" : "khazana" + os.sep + "khazana_vasp_errors.txt",
			"uri_adds" : ["filename"],
			"max_records" : -1
			}
		print "KHAZANA VASP PROCESSING"
		khaz_v = process_data(khazana_vasp_args)
		if feedstock:
			print "Creating JSON files"
			print "Khazana VASP All"
			with open("khazana/khazana_vasp_all.json", 'w') as fk1:
				json.dump(khaz_v, fk1)
			print "Done\nKhazana VASP 3"
			with open("khazana/khazana_vasp_3.json", 'w') as fk2:
				json.dump(khaz_v[:3], fk2)
			print "Done\n"
	
	print "END"

