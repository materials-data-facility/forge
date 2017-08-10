#Evan Pike dep78@cornell.edu
#Used to create formatted files for the ionic_radii_curie_temp dataset
#Function was changed several times due to errors, so may not be exactly
#what was used to format ionic_radii_curie_temp
import os
from mdf_indexers.utils.file_utils import find_files

def make_files(in_dir):

    print("Creating Files")

    #for data_file in find_files(in_dir, "(f1.csv|f3.csv)"):
        #with open(os.path.join(data_file["path"], data_file["filename"]), 'r') as raw_in:
    with open(in_dir + "f3.csv", 'r') as raw_in:
        headers = raw_in.readline().split()
        print(headers)
        data = raw_in.readlines()[2:]
    #if "f" in data_file["filename"]:
    stop = 8
    increment = 2
    i=0
    while i < stop:
        if stop == 8:
            indx = int(i/2)
        else:
            indx = int(i/5)
        name = headers[indx]
        record_data = "T,k,DSC signal, mW/mg\n"
        for line in data:
            record = line.split()
            if stop == 8:
                try:
                    record_data += record[i] + ";" + record[i+1] + "\n"
                except:
                    pass
        #end_dir = in_dir + name + "_" + data_file["filename"][:-4] + ".txt"
        end_dir = in_dir + name + "_f3.txt"
        f = open(end_dir, 'w')
        f.write(record_data)
        f.close()
        i+=increment






    print("File Creation Complete")

if __name__ == '__main__':
    #make_files(os.path.abspath("") + "/mdf_indexers/datasets/ionic_radii_curie_temp/")
    for data_file in find_files(os.path.abspath("") + "/mdf_indexers/datasets/ionic_radii_curie_temp/", "txt"):
        with open(os.path.join(data_file["path"], data_file["filename"]), 'r') as raw_in:
            data = raw_in.read()
        new_data = data.replace(",", ":")
        final_data = new_data.replace(";", ",")
        f = open(os.path.join(data_file["path"], data_file["filename"]), 'w')
        f.write(final_data)
        f.close()





