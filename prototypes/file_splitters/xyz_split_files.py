#Evan Pike dep78@cornell.edu
#Functions to split compact files
#Functions assume there is only one large file and
#no subfolders
#i.e. no folders are made... only records
import os
from tqdm import tqdm

#Use for file seperated by new lines
"""Code in triple quotes was used. Code for the current split
   line xyz function should be correct, but has not been tested"""
def split_line_xyz(in_dir, out_dir, verbose=False):
    
    if verbose:
        print("Creating files")
        
    with open(in_dir, 'r') as raw_in:
        data = raw_in.read()
    lst_data = data.split("\n\n")
    
    i=1
    for record in lst_data:
        new_dir = out_dir + "record-" + str(i) + ".xyz"
        f = open(new_dir, 'w')
        f.write(record)
        f.close()
        i+=1

    """with open(in_dir) as xyz_in:
        lst = xyz_in.readlines()
    if verbose:
        print("Creating files")
        
    record_lst = []
    for line in tqdm(lst, desc="Making files", disable=not verbose):
        try:
            if line == "\n":
                new_dir = out_dir + "record-" + str(i)
                f = open(new_dir+".xyz", 'w')
                record = "".join(record_lst)
                f.write(record)
                record_lst = []
                i+=1
                f.close()
        except:
            record_lst.append(line)"""
    
    if verbose:
        print("File Creation Complete")


#Use for file seperated by ints displaying how many lines follow
def index_split_xyz(in_dir, out_dir, verbose=False):
    
    with open(in_dir, errors="ignore") as xyz_in:
        lst = xyz_in.readlines()
    if verbose:
        print("Creating files")
        
    i=1
    for indx in tqdm(range(len(lst)), desc="Making files", disable=not verbose):
        try:
            num = int(lst[indx])
            record_lst = lst[indx: indx+num+2]
            new_dir = out_dir + "ic7b00006_si_record-" + str(i) + ".xyz"
            f = open(new_dir, 'w')
            record = "".join(record_lst)
            f.write(record)
            i+=1
            f.close()
        except:
            pass 
    
    if verbose:
        print("File Creation Complete")

#Use for cif seperate by #===END\n
def split_cif(in_dir, out_dir, verbose=False):

    if verbose:
        print("Creating Files")

    with open(in_dir, 'r') as raw_in:
        data = raw_in.read()

    i = 1
    for cif in tqdm(data.split("#===END\n")[:-1], desc="Making Files"):
        end_dir = os.path.join(out_dir, "cif_ic7b00006_si_record-" + str(i) + ".cif")
        f = open(end_dir, 'w')
        f.write(cif)
        f.close()
        i+=1

    if verbose:
        print("Files Created")


if __name__ == '__main__':
    split_cif(os.path.join(os.getcwd(), "ic7b00006_si_003.cif"), os.path.join(os.getcwd(), "cif_files"), True)