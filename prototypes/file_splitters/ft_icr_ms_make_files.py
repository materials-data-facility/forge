#Evan Pike dep78@cornell.edu
#Used to create formatted files for the ft-icr-ms dataset
import os
from mdf_indexers.utils.file_utils import find_files
from mdf_indexers.parsers.tab_parser import parse_tab
from tqdm import tqdm

def make_ft_icr_ms_file(in_dir, verbose=False):
    if verbose:
        print("Creating Files")

    final_record = ""
    for data_file in tqdm(find_files(in_dir, ".csv"), desc="Processing Files", disable=not verbose):
        technique = data_file["filename"].split("_")[1][:-4]
        location = data_file["filename"].split("_")[0]
        with open(os.path.join(in_dir, data_file["filename"])) as raw_in:
            if final_record == "":
                headers = raw_in.readline().split(",")
                headers = headers[1][1:] + ";" + ";".join(headers[2:8]) + "; technique; location" + "\n"
                headers = ";".join(headers.split("; "))
                final_record += headers
            all_data = raw_in.readlines()[2:]

        for line in all_data:
            if line[0] == "," or line[0] == "O":
                pass
            elif line[:2] == "No":
                break
            else:
                rdata = line.split(",")
                if rdata[13] == "13C":
                    comp = "".join(rdata[7:13]) + " 13C1"
                else:
                    comp = "".join(rdata[7:][:-1])
                final_record += ";".join(rdata[1:7]) + ";" + comp + ";" + technique + ";" + location + "\n"
    end_dir = in_dir + "ft-icr-ms_data" + ".txt"
    f = open(end_dir, 'w')
    f.write(final_record)
    f.close()


    if verbose:
        print("File Creation Complete")


if __name__ == '__main__':
    make_ft_icr_ms_file(os.path.abspath("") + "/mdf_indexers/datasets/ft-icr-ms/", True)