import os
from tqdm import tqdm
import ase.io

'''
from ase.io import read

def __read_vasp(filename='CONTCAR'):
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


def __read_vasp_out(filename=None, index=slice(0), force_consistent=False):
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
        constr = __read_vasp(os.path.join(data_path, 'CONTCAR')).constraints
    except Exception:
        try:
            constr = __read_vasp(os.path.join(data_path, 'POSCAR')).constraints
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
        try:
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
        except Exception:
            pass
        if 'POSITION          ' in line:
            try:
                forces = []
                positions = []
                for iatom in range(natoms):
                    temp = data[n + 2 + iatom].split()
                    try:
                        new_atom = Atom(symbols[iatom],
                                      [float(temp[0]), float(temp[1]), float(temp[2])])
                    except Exception:
                        new_atom = Atom(symbols[iatom])
                    atoms += new_atom
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
            except Exception:
                pass

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
'''

#Parser for data in ASE-readable formats
#Arguments:
#   file_path: Path to the data file (or directory for VASP). Required
#   data_format: Type of data found at the end of the path. If None, ASE will attempt to guess the format. Default None
#   verbose: Print status messages? Default False
def parse_ase(file_path, data_format=None, verbose=False):
    ase_template = {
#       "constraints" : None,
#        "all_distances" : None, # Causes performance issues with large numbers of atoms
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
#       "forces_raw" : None,
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
#       "potential_energy_raw" : None,
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

    result = ase.io.read(file_path, format=data_format)

    ase_dict = ase_template.copy()
    total_count = 0
    failure_count = 0
    success_count = 0
    none_count = 0
    for key in ase_dict.keys():
        if verbose:
            print("Fetching", key)
        total_count += 1
        try:
            ase_dict[key] = eval("result.get_" + key + "()")
            success_count += 1
        except Exception: #(NotImplementedError, AttributeError): More exception types here than can reasonably be found, just catching everything
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
        ase_dict["potential_energy_raw"] = result.get_potential_energy(apply_constraint=False)
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
#           elif ase_dict[key] != ase_dict[key]:
#               none_keys.append(key)
#               none_count += 1
        #elif 'fixatoms' in str(type(ase_dict[key])).lower():
        #   ase_dict[key] = ase_dict[key].get_indices().tolist()
        elif type(ase_dict[key]) is list:
            new_list = []
            for elem in ase_dict[key]:
                #if 'numpy' in str(type(elem)).lower():
                #   new_elem = elem.tolist()
                if 'fixatoms' in str(elem).lower():
                    new_elem = elem.get_indices().tolist()
#                   elif elem != elem:
#                       new_elem = None
                else:
                    new_elem = elem
                if new_elem:
                    new_list.append(new_elem)
            if new_list:
                ase_dict[key] = new_list
            else:
                none_keys.append(key)

    for key in none_keys:
        ase_dict.pop(key)

    #Print results
    if verbose:
        print("Processed " + str(total_count) + " items.")
        print("There were " + str(success_count) + " successes and " + str(failure_count) + " failures.")
        print("No data existed for " + str(none_count - failure_count) + " items.") #none_count includes items that are None because they failed, don't want to report failures twice
    
        print(str(len(ase_dict)) + " valid items returned.")

    return ase_dict

if __name__ == "__main__":
    import json
    ase_template = {
#       "constraints" : None,
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
#       "forces_raw" : None,
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
#       "potential_energy_raw" : None,
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
    print("\nThis is the parser for ASE-readable data.")
    print("USAGE:\n\nparse_ase(file_path, data_format=None, verbose=False)")
    print("Arguments:\n\tfile_path: Path to the data file (or directory for VASP)")
    print("\tdata_format: Type of data found at the end of the path. If None, ASE will attempt to guess the format. Default None")
    print("\tverbose: Print status messages? Default False\n")
    print("\nReturns:\n\tIf VASP format data: {'frames': a_list_of_ase_dicts}")
    print("\tIf another format: A single ASE dict.")
    print("\tAn ASE dictionary can contain the following keys (but is not guaranteed to):")
    print(json.dumps(list(ase_template.keys()), sort_keys=True, indent=4, separators=(',', ': ')))

