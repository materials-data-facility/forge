#Evan Pike dep78@cornell.edu
#Split SDF Files
import os
from tqdm import tqdm
from mdf_indexers.utils.file_utils import find_files

def split_sdf_files(in_dir, out_dir, verbose=False):

    if verbose:
        print("Creating Files")

    for sdf_file in tqdm(find_files(in_dir, "sdf"), desc="Processing Files", disable=not verbose):
        with open(os.path.join(sdf_file["path"], sdf_file["filename"]), 'r') as raw_in:
            sdf_data = raw_in.read()
        split_sdf_data = sdf_data.split("$$$$\n")
        path = os.path.join(sdf_file["path"], sdf_file["filename"].split(".")[0])
        if not os.path.exists(path):
            os.mkdir(path)
        i=1
        for sdf_record in split_sdf_data[:-1]:
            filename = sdf_file["filename"].split(".")[0]
            end_dir = sdf_file["path"] + filename + "/" + "record-" + str(i) + ".sdf"
            f = open(end_dir, 'w')
            f.write(sdf_record)
            i+=1
            f.close()

    if verbose:
        print("File Creation Complete")

if __name__ == '__main__':
    split_sdf_files(os.path.abspath("") + "/mdf_indexers/datasets/qsar_molecular_diversity/", os.path.abspath("") + "/mdf_indexers/datasets/qsar_molecular_diversity/", True)

