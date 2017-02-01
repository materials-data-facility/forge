'''
Converter (multiple sources)
'''
import os
#from pickle import dump
from tqdm import tqdm
from ase.io import read
import re
from json import dump
import tarfile
import zipfile
import gzip
import warnings

import paths #Contains variables for relative paths to data

#Pick one or more datasets to process
datasets_to_process = []
datasets_to_process.append("danemorgan")
#datasets_to_process.append("khazana_polymer")
#datasets_to_process.append("khazana_vasp")
#datasets_to_process.append("cod")
#datasets_to_process.append("sluschi")

#Export a smaller feedstock file for testing?
#If False, will still write full feedstock file
feedsack = True
#These are the sizes of the small feedstock
dane_feedsack = 100
khaz_p_feedsack = 100
khaz_v_feedsack = 3
cod_feedsack = 100
sluschi_feedsack = 100

#Currently-supported formats are listed below.
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
                        raise KeyError()
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


def read_vasp_out(filename=None, index=slice(0), force_consistent=False):
    ###OLD INDEX DEFAULT: -1
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
	ase_template = {
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
		print("Error: Invalid data format '" + data_format + "'.")
		return None
	elif data_format == 'vasp':
		if error_log:
			try:
				rset = read_vasp_out(file_path)
			except Exception as err:
				error_log.write("ERROR: '" + str(err) + "' with the following VASP file:\n")
				error_log.write(file_path + "\n\n")
				#with open(error_log, 'a') as err_file:
				#	err_file.write("ERROR: '" + str(err) + "' with the following file\n:")
				#	err_file.write(outcar_path)
				return None
		else:
			rset = read_vasp_out(file_path)
	elif data_format == 'cif':
		#if not file_name:
		#	print "Error: file_name required for 'cif' format."
		#	return None
		#full_path = os.path.join(file_path, file_name)
		if error_log:
			try:
				#r = read_cif(file_path)
				rset = [read(file_path)]
			except Exception as err:
				error_log.write("ERROR: '" + str(err) + "' with the following CIF file:\n")
				error_log.write(file_path + "\n\n")
				#error_log.write(full_path + "\n\n")
				return None
		else:
			#r = read_cif(file_path)
			rset = [read(file_path)]

	ase_list = []
	for result in rset:
		ase_dict = ase_template.copy()
		total_count = 0
		failure_count = 0
		success_count = 0
		none_count = 0
		for key in ase_dict.keys():
			total_count += 1
			try:
				ase_dict[key] = eval("result.get_" + key + "()")
				success_count += 1
			except: #(NotImplementedError, AttributeError): More exception types here than can reasonably be found, just catching everything
				failure_count +=1
				ase_dict[key] = None
		#Populate other fields
		
		total_count += 1
		ase_dict["constraints"] = result.constraints #Should always exist, even if empty
		success_count += 1
		
		total_count += 1
		try:
			ase_dict["forces_raw"] = result.get_forces(apply_constraint=False)
			success_count += 1
		except:
			failure_count += 1
			ase_dict["forces_raw"] = None
		
		total_count += 1
		try:
			ase_dict["potential_energy_raw"] = result.get_potential_energy(apply_constraints=False)
			success_count += 1
		except:
			failure_count += 1
			ase_dict["potential_energy_raw"] = None

		#Remove None results, and fix other objects
		#numpy ndarrays and FixAtoms instances aren't JSON serializable, but we need to convert our data to JSON
		none_keys = []
		for key in ase_dict.keys():
			if 'numpy' in str(type(ase_dict[key])).lower():
				ase_dict[key] = ase_dict[key].tolist()
			if ase_dict[key] is None:
				none_keys.append(key)
				none_count += 1
#			elif ase_dict[key] != ase_dict[key]:
#				none_keys.append(key)
#				none_count += 1
			#elif 'fixatoms' in str(type(ase_dict[key])).lower():
			#	ase_dict[key] = ase_dict[key].get_indices().tolist()
			elif type(ase_dict[key]) is list:
				new_list = []
				for elem in ase_dict[key]:
					#if 'numpy' in str(type(elem)).lower():
					#	new_elem = elem.tolist()
					if 'fixatoms' in str(elem).lower():
						new_elem = elem.get_indices().tolist()
#					elif elem != elem:
#						new_elem = None
					else:
						new_elem = elem
					if new_elem:
						new_list.append(new_elem)
				if new_list:
					ase_dict[key] = new_list
				else:
					none_keys.append(key)
		'''
		#Remove None results
		none_keys = []
		for key, value in ase_dict.items():
			if value is None:
				#ase_dict.pop(key)
				none_keys.append(key)
				none_count += 1
			elif value != value:
				none_keys.append(key)
				none_count += 1
			elif type(value) is list:
				
			
			elif type(value) is list and value.all() != value.all(): #NaN
				none_keys.append(key)
			elif type(value) is dict and value.values().all() != value.values().all(): #NaN
				none_keys.append(key)
			else:
				try:
					if value != value:
						none_keys.append(key)
				except ValueError: #Swallow issues with direct comparison
					pass
		'''
		for key in none_keys:
			ase_dict.pop(key)
		
		ase_list.append(ase_dict)

	#Print results
	if verbose:
		print("Processed " + str(total_count) + " items.")
		print("There were " + str(success_count) + " successes and " + str(failure_count) + " failures.")
		print("No data existed for " + str(none_count - failure_count) + " items.") #none_count includes items that are None because they failed, don't want to report failures twice
	
	if output_file:
		with open(output_file, 'w') as out_file:
			dumps(ase_dict, out_file)
		if verbose:
			print(str(len(ase_dict)) + " valid items written to '" + output_file + "'.")
	else:
		if verbose:
			print(str(len(ase_dict)) + " valid items saved.")

	if data_format == "vasp":
		return {"frames" : ase_list}
	elif data_format == "cif":
		return ase_list[0]


#Finds all directories containing a specified type of file and returns list of dicts with path to files and data gleaned from folder names
#root specifies the path to the first dir to start with. Default is current working directory.
#file_match is a string containing the file name to search for. Default is None, which matches all files.
#keep_dir_name_depth is how many layers of dir, counting from the base file up, contain data to save. -1 saves everything in the path. Default is 0, which disables saving anything.
#max_files is the maximum number of results to return. Default -1, which returns all results.
#uncompress_archives, if True, will uncompress archives found before checking against the file_pattern. This is *SLOW*, and does not guarantee archives that expand into directories will be searched. Default False, which leaves archives alone.
def find_files(root=None, file_pattern=None, keep_dir_name_depth=0, max_files=-1, uncompress_archives=False, verbose=False):
	if not root:
		root = os.getcwd()
	dir_list = []
	for path, dirs, files in tqdm(os.walk(root), desc="Finding files", disable= not verbose):

		if uncompress_archives:
			for single_file in files:
				abs_path = os.path.join(path, single_file)
				if tarfile.is_tarfile(abs_path):
					tar = tarfile.open(abs_path)
					tar.extractall()
					tar.close()
				elif zipfile.is_zipfile(abs_path):
					z = zipfile.open(abs_path)
					z.extractall()
					z.close()
				else:
					try:
						with gzip.open(abs_path) as gz:
							file_data = gz.read()
							with open(abs_path.rsplit('.', 1)[0], 'w') as newfile: #Opens the absolute path, including filename, for writing, but does not include the extension (should be .gz or similar)
								newfile.write(file_data)
					except IOError: #This will occur at gz.read() if the file is not a gzip. After it has been opened. I don't know why it doesn't have any format check before this.
						pass
			all_files = os.listdir(path)
		else:
			all_files = files

		for one_file in all_files:
			if not file_pattern or re.search(file_pattern, one_file): #Only care about dirs with desired data
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
#	data_exception_log: File to log exceptions caught when processing the data. If a filename is listed here, such exceptions will be logged but otherwise ignored. Default None, which causes exceptions to terminate the program.
#
#	uri_adds: A list of things to add to the end of 'uri' for each record. Options are 'dir' for the directory structure, 'filename' for the name of the file, and 'ext' for the file extension. Other elements in the list will be added as string literals.
#		Choose any combination, including none (empty list). The URI will be appended with the values specified in the order they are specified. Default 'filename'.
#	max_records: Maximum number of records to return (actual return value may be less due to failed files). Default -1, which returns all (valid) records.
#	archived: Bool, if some desirable files are in archives and should be extracted. This slows down file discovery. Can be turned off after first run, because the archived files will already be extracted. Default False.
#	feedsack_size: Number of records to save in the "feedsack," the smaller feedstock file for testing. Any int <= 0 disables feedsacks. Default -1.
#	feedsack_file: Filename for feedsack output. Ignored if feedsack_size <= 0. Default "feedsack_$SIZE.json" in local directory.
#
def process_data(arg_dict):
	root = arg_dict.get("root", os.getcwd())
	keep_dir_name_depth = arg_dict.get("keep_dir_name_depth", 0)
	verbose  = arg_dict.get("verbose", False)
	file_pattern = arg_dict.get("file_pattern", None)
	uri_adds = arg_dict.get("uri_adds", ['filename'])
	max_records = arg_dict.get("max_records", -1)
	archived = arg_dict.get("archived", False)
	feedsack_size = arg_dict.get("feedsack_size", -1)
	feedsack_file = arg_dict.get("feedsack_file", "feedsack_" + str(feedsack_size) + ".json") if feedsack_size > 0 else None
	if arg_dict.get("file_format", "NONE") not in supported_formats:
		print("Error: file_format '" + arg_dict.get("file_format", "NONE") + "' is not supported.")
		return None

	if arg_dict.get("data_exception_log", None):
		err_log = open(arg_dict["data_exception_log"], 'w')
	else:
		err_log = None
	output_file = arg_dict.get("output_file", None)
#	if verbose:
#		print("Finding files")
	dir_list = find_files(root=root, file_pattern=file_pattern, keep_dir_name_depth=keep_dir_name_depth, max_files=max_records, uncompress_archives=archived, verbose=verbose)
	if verbose:
		print("Converting file data to JSON")
#	all_data_list = []
	good_count = 0
	all_count = 0
	if output_file:
		out_open = open(output_file, 'w')
	if feedsack_size > 0:
		feed_out = open(feedsack_file, 'w')
	for dir_data in tqdm(dir_list, desc="Processing data files", disable= not verbose):
		all_count += 1
		formatted_data = {}
		uri = arg_dict.get("uri", "")
		full_path = os.path.join(dir_data["path"], dir_data["filename"] + dir_data["extension"])
		with warnings.catch_warnings():
			warnings.simplefilter("ignore") #Either it fails (and is logged in convert_to_json) or it's fine.
			file_data = convert_to_json(file_path=full_path, data_format=arg_dict["file_format"], output_file=None, error_log=err_log, verbose=False) #Status messages spam with large datasets
		if file_data:
			file_data["dirs"] = dir_data["dirs"]
			file_data["filename"] = dir_data["filename"]
			file_data["ext"] = dir_data["extension"]

			for addition in uri_adds:
				if addition == 'dir':
					for dir_name in file_data["dirs"]:
						uri = os.path.join(uri, dir_name)
				elif addition == 'filename':
					uri = os.path.join(uri, file_data["filename"])
				elif addition == 'ext':
					uri += file_data["ext"]
				else:
					uri += addition

#			formatted_data["uri"] = uri
#			for key, value in arg_dict.get("metadata", {}).iteritems():
#				formatted_data[key] = value
			
			formatted_data = file_data
			formatted_data["uri"] = uri
	#		all_data_list.append(formatted_data)
			dump(formatted_data, out_open)
			out_open.write("\n")
			if good_count < feedsack_size:
				dump(formatted_data, feed_out)
				feed_out.write("\n")
			good_count += 1
	if feedsack_size > 0:
		feed_out.close()
		print("Feedsack written to", feedsack_file)
	if output_file:
		out_open.close()
		if verbose:
			print("Data written to", output_file)
	if err_log:
		err_log.close()
	if verbose:
		print(str(good_count) + "/" + str(all_count) + " data files successfully processed")

#	if arg_dict.get("output_file", None):
#		if verbose:
#			print "Writing output to " + arg_dict["output_file"]
#		with open(arg_dict["output_file"], 'w') as out_file:
#			dump(all_data_list, out_file)
#		if verbose:
#			print "Data written"
	if verbose:
		print("Processing complete")
	return True #all_data_list


if __name__ == "__main__":
	print("\nBEGIN")
	
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
			"root" : paths.datasets + "dane_morgan/data",
			"file_pattern" : "^OUTCAR$",
			"file_format" : "vasp",
			"verbose" : True,
			"output_file" : paths.raw_feed + "danemorgan_all.json",
			"data_exception_log" : paths.datasets + "dane_morgan/dane_errors.txt",
			"uri_adds" : ["dir"],
			"max_records" : -1,
			"archived" : False,
			"feedsack_size" : dane_feedsack if feedsack else -1,
			"feedsack_file" : paths.sack_feed + "danemorgan_" + str(dane_feedsack) + ".json"
			}
		if dane_args["verbose"]:
			print("DANE PROCESSING")
		dane = process_data(dane_args)
		if dane_args["verbose"]:
			print("DONE\n")
		'''
		if feedsack:
			
			print "Creating JSON files"
			print "Dane Morgan All"
			with open("dane_morgan/danemorgan_all.json", 'w') as fd1:
				json.dump(dane, fd1)
			
			if dane_args["verbose"]:
				print "Making Dane Morgan feedsack (" + str(dane_feedsack) + ")"
			with open(paths.sack_feed + "danemorgan_" + str(dane_feedsack) + ".json", 'w') as fd2:
				dump(dane[:dane_feedsack], fd2)
			if dane_args["verbose"]:
				print "Done\n"
		'''


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
			"root" : paths.datasets + "khazana/polymer_scientific_data_confirmed",
			"file_pattern" : None,
			"file_format" : "cif",
			"verbose" : True,
			"output_file" : paths.raw_feed + "khazana_polymer_all.json",
			"data_exception_log" : paths.datasets + "khazana/khazana_polymer_errors.txt",
			"uri_adds" : ["filename"],
			"max_records" : -1,
			"archived" : False,
			"feedsack_size" : khaz_p_feedsack if feedsack else -1,
			"feedsack_file" : paths.sack_feed + "khazana_polymer_" + str(khaz_p_feedsack) + ".json"
			}
		if khazana_polymer_args["verbose"]:
			print("KHAZANA POLYMER PROCESSING")
		khaz_p = process_data(khazana_polymer_args)
		if khazana_polymer_args["verbose"]:
			print("DONE\n")
		'''
		if feedsack:
		
			print "Creating JSON files"
			print "Khazana Polymer All"
			with open("khazana/khazana_polymer_all.json", 'w') as fk1:
				json.dump(khaz_p, fk1)
			
			if khazana_polymer_args["verbose"]:
				print "Making Khazana Polymer feedsack (" + str(khaz_p_feedsack) + ")"
			with open(paths.sack_feed + "khazana_polymer_" + str(khaz_p_feedsack) + ".json", 'w') as fk2:
				dump(khaz_p[:khaz_p_feedsack], fk2)
			if khazana_polymer_args["verbose"]:
				print "Done\n"
		'''

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
			"root" : paths.datasets + "khazana/OUTCARS",
			"file_pattern" : "^OUTCAR",
			"file_format" : "vasp",
			"verbose" : True,
			"output_file" : paths.raw_feed + "khazana_vasp_all.json",		
			"data_exception_log" : paths.datasets + "khazana/khazana_vasp_errors.txt",
			"uri_adds" : ["filename"],
			"max_records" : -1,
			"archived" : False,
			"feedsack_size" : khaz_v_feedsack if feedsack else -1,
			"feedsack_file" : paths.sack_feed + "khazana_vasp_" + str(khaz_v_feedsack) + ".json",
			}
		if khazana_vasp_args["verbose"]:
			print("KHAZANA VASP PROCESSING")
		khaz_v = process_data(khazana_vasp_args)
		if khazana_vasp_args["verbose"]:
			print("DONE\n")
		'''
		if feedsack:
			
			print "Creating JSON files"
			print "Khazana VASP All"
			with open("khazana/khazana_vasp_all.json", 'w') as fk1:
				json.dump(khaz_v, fk1)
			
			if khazana_vasp_args["verbose"]:
				print "Making Khazana VASP feedsack (" + str(khaz_v_feedsack) + ")"
			with open(paths.sack_feed + "khazana_vasp_" + str(khaz_v_feedsack) + ".json", 'w') as fk2:
				dump(khaz_v[:khaz_v_feedsack], fk2)
			if khazana_vasp_args["verbose"]:
				print "Done\n"
		'''

	#Crystallography Open Database
	if "cod" in datasets_to_process:
		cod_args = {
			"uri" : "http://www.crystallography.net/cod",
			"keep_dir_name_depth" : 0,
			"root" : paths.datasets + "cod/open-cod",
			"file_pattern" : "\.cif$",
			"file_format" : "cif",
			"verbose" : True,
			"output_file" : paths.raw_feed + "cod_all.json",
			"data_exception_log" : paths.datasets + "cod/cod_errors.txt",
			"uri_adds" : ["filename", ".html"],
			"max_records" : -1,
			"archived" : False,
			"feedsack_size" : cod_feedsack if feedsack else -1,
			"feedsack_file" : paths.sack_feed + "cod_" + str(cod_feedsack) + ".json"
			}
		if cod_args["verbose"]:
			print("COD PROCESSING")
		cod = process_data(cod_args)
		if cod_args["verbose"]:
			print("DONE\n")
		'''
		if feedsack:
			if cod_args["verbose"]:
				print "Making COD feedsack (" + str(cod_feedsack) + ")"
			with open(paths.sack_feed + "cod_" + str(cod_feedsack) + ".json", 'w') as fc:
				dump(cod[:cod_feedsack], fc)
			if cod_args["verbose"]:
				print "Done\n"
		'''

	#Sluschi
	if "sluschi" in datasets_to_process:
		sluschi_args = {
			"uri" : "globus:mostly_melted_snow",
			"keep_dir_name_depth" : -1,
			"root" : paths.datasets + "sluschi/sluschi",
			"file_pattern" : "^OUTCAR$",
			"file_format" : "vasp",
			"verbose" : True,
			"output_file" : paths.raw_feed + "sluschi_all.json",
			"data_exception_log" : paths.datasets + "sluschi/sluschi_errors.txt",
			"uri_adds" : ["dir"],
			"max_records" : -1,
			"archived" : False, #Should be True for first run, afterwards extracted data should be fine
			"feedsack_size" : sluschi_feedsack if feedsack else -1,
			"feedsack_file" : paths.sack_feed + "sluschi_" + str(sluschi_feedsack) + ".json"
			}
		if sluschi_args["verbose"]:
			print("SLUSCHI PROCESSING")
		sluschi = process_data(sluschi_args)
		if sluschi_args["verbose"]:
			print("DONE\n")
		'''
		if feedsack:
			if sluschi_args["verbose"]:
				print "Making sluschi feedsack (" + str(sluschi_feedsack) + ")"
			with open(paths.sack_feed + "sluschi_" + str(sluschi_feedsack) + ".json", 'w') as fc:
				dump(sluschi[:sluschi_feedsack], fc)
			if sluschi_args["verbose"]:
				print "Done\n"
		'''

	
	print("END")

