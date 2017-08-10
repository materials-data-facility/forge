#Evan Pike dep78@cornell.edu
#Split dilute_mg_alloys_dft into several seperate records
import os

def split_diffusion_data(in_dir, out_dir, verbose=False):

    if verbose:
        print("Creating Files")
    
    with open(in_dir, 'r') as raw_in:
        elements = raw_in.readline()[3:-3].split(",,,")
        lst_data = raw_in.readlines()[2:]
    headers = ["T/K", "1000/T", "D_basal (D⊥)", "D_normal (D||)", "ratio"]
    start = 2
    end = 5
    while end <= 147:
        record_data = "\t".join(headers) + "\n"
        for line in lst_data:
            data = line.split(",")
            record_data += data[0] + "\t" + data[1] + "\t" + "\t".join(data[start:end]) + "\n"
        index = int((start-2)/3)
        element = elements[index]
        end_dir = out_dir + "diffusion_data_dft/" + element + "_diffusion.txt"
        f = open(end_dir, 'w')
        f.write(record_data)
        start+=3
        end+=3
        f.close()

    if verbose:
        print("File Creation Complete")




def split_e_v_fitting_data(in_dir, out_dir, verbose=False):

    if verbose:
        print("Creating Files")
        
    with open(in_dir, 'r') as raw_in:
        lst_data = raw_in.readlines()[1:]
    structures = ["Mg63X,Mg95X", "Basal_initial_structure", "Basal_transition_state_structure", "Normal_initial_structure", "Normal_transition_state_structure"]

    start = 1
    end = 7
    while end <= 36:
        record_data = ""
        for line in lst_data:
            data = line.split(",")
            record_data += data[0] + "\t" + "\t".join(data[start:end]) + "\n"
        index = int((start-1)/7)
        structure = structures[index]
        end_dir = out_dir + "e_v_fitting_data_dft/" + structure + "_e_v_fitting.txt"
        f = open(end_dir, 'w')
        f.write(record_data)
        start+=7
        end+=7

        
    if verbose:
        print("File Creation Complete")

if __name__ == '__main__':
    final_path = "/mdf_indexers/datasets/dilute_mg_alloys_dft/"
    split_e_v_fitting_data(os.path.abspath("") + final_path + "e_v_fitting_results.csv", os.path.abspath("") + final_path, True)