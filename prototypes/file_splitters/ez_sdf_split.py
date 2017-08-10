#Evan Pike dep78@cornell.edu
#file maker sdf
import os

def ez_sdf_split(in_dir, out_dir, verbose=False):
    if verbose:
        print("Creating Files")

    with open(in_dir, 'r') as raw_in:
        data = raw_in.read()
    record_lst = data.split("$$$$\n")
    i=1
    for record in record_lst[:-1]:
        end_dir = out_dir + "carbolenes_record-" + str(i) + ".sdf"
        f = open(end_dir, 'w')
        f.write(record)
        i+=1
        f.close()

    if verbose:
        print("File Creation Complete")

if __name__ == '__main__':
    ez_sdf_split(os.path.abspath("") + "/mdf_indexers/datasets/silverman_qsar_comma/carbolenes.sdf", os.path.abspath("") + "/mdf_indexers/datasets/silverman_qsar_comma/carbolenes/", True)

