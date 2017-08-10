#Evan Pike dep78@cornell.edu
#Mol to sdf conversion
import subprocess
import os
from file_utils import find_files
from multiprocessing.pool import Pool

number_of_processes = 4


def mol_to_sdf(path, filename):
    # Code to turn one mol into sdf
    bash_command = "babel -imol {} -osdf {}".format(path + filename + ".mol", path + filename + ".sdf")
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    
def del_mol(path, filename):
    os.remove(os.path.join(path, filename + ".mol"))

def del_xyz(path, filename):
    os.remove(os.path.join(path, filename + ".xyz"))

def del_sdf(path, filename):
    os.remove(os.path.join(path, filename + ".sdf"))

def mol2_to_sdf(path, filename):
    bash_command = "babel -imol2 {} -osdf {}".format(path + "/" + filename + ".mol2", path + "/" + filename + ".sdf")
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()

def main(ext):
    # arg_list is a list of tuples, each tuple is one call to mol_to_sdf
    #arg_list = [ (i["path"], i["filename"][:-4]) for i in find_files("/".join(os.path.abspath("").split("/")[:-1]) + "/datasets/dss_tox/DSSToxAll_20151019/ToxAll/", "mol$") ]
    arg_list = [ (i["path"], i["filename"][:-(len(ext)+1)]) for i in find_files("/".join(os.path.abspath("").split("/")[:-1]) + "/datasets/activity_cliffs/", ext) ]
    mp = Pool(number_of_processes)
    #mp.starmap(mol_to_sdf, arg_list, chunksize=1)
    #mp.starmap(del_mol, arg_list, chunksize=1)
    #mp.starmap(del_xyz, arg_list, chunksize=1)
    mp.starmap(mol2_to_sdf, arg_list, chunksize=1)
    #mp.starmap(del_sdf, arg_list, chunksize=1)
    mp.close()
    mp.join()


if __name__ == '__main__':
    main("mol2")
