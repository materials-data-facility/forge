#Evan Pike dep78@cornell.edu
#Used to seperate files for ti64_acoustic_loss so that each temperature has its own file
import os
from mdf_indexers.parsers.tab_parser import parse_tab
from tqdm import tqdm

def make_ti64_files(in_dir, out_dir, verbose=False):
    
    if verbose:
        print("Creating Files")
        
    with open(os.path.join(in_dir, "ti64-rawdata.txt"), 'r') as raw_in:
        headers = raw_in.readline().split()
        all_data = raw_in.read()
    temp_list = []
    for head in headers[1:]:
        head_split = head.split('>')
        temp_list.append(head_split[1][:-1])
    for temperature in temp_list:
        final_record = "freq" + "\t" + "ti64<{}>".format(temperature) + "\n"
        for record in tqdm(parse_tab(all_data, headers=headers, sep="\t"), desc="Processing records", disable=not verbose):
            final_record += record["freq"] + "\t" + record["'<ti64>" + temperature + "'"] + "\n"
        end_dir = out_dir + "ti64<{}>.txt".format(temperature)
        f = open(end_dir, 'w')
        f.write(final_record)
        f.close()
        
    if verbose:
        print("File Creation Complete")

if __name__ == '__main__':
    make_ti64_files(os.path.abspath("") + "/mdf_indexers/datasets/ti64_acoustic_loss/", os.path.abspath("") + "/mdf_indexers/datasets/ti64_acoustic_loss/", True)

