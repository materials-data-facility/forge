#!/usr/bin/env python

import sys
import os
import json


def main(argv):
    path = len(argv) == 2 and os.path.dirname(argv[0]) or "/Users/mo/Documents/MDF"
    filename = len(argv) == 2 and os.path.basename(argv[0]) or "NREL_polymer_export.txt"
    output = len(argv) == 2 and argv[1] or "NREL_polymer_output.txt"

    # print filename
    records = process_file(path, filename)

    # data output
    gw = open(output, "w")
    try:
        for record in records:
            print(json.dumps(record))
            gw.write(json.dumps(record) + "\n")
    except IOError:
        print("A list Records is empty!")

    gw.close()
    print("FINISH")


def run_extraction(filename):
    '''
    Parse the NREL file. Get the data defined by the header
    'gp_extrapolated' -> Extrapolated Gap
    'delta_homo' -> Highest Occupied Molecular Orbital difference
    'sum_f_osc' -> Sum of OSC
    'delta_optical_lumo' -> Optical Lowest Unoccupied Molecular Orbital difference
    'delta_lumo' -> Lowest Unoccupied Molecular Orbital difference
    'spectral_overlap' -> Spectral overlap
    'functional/basis' -> Functional basis
    'homo_extrapolated' -> Extrapolated Highest Occupied Molecular Orbital
    'common_tag' -> Common tag
    'optical_lumo_extrapolated' -> Extrapolated Optical Lowest Unoccupied Molecular Orbital
    'lumo_extrapolated' -> Extrapolated Lowest Unoccupied Molecular Orbital
    :param filename: Filename of NREL data file
    :return: List of Dictionaries containing data
    '''
    # parse line by line and create list of dictionaries with key headers
    try:
        f = open(filename)

        heads = list()
        records = list()
        line_index = 0
        for line in f:
            line = line.rstrip('\n')
            # print line
            if line_index == 0:
                heads = line.split()
            else:
                elements = line.split()
                if len(heads) == len(elements):
                    records.append(dict(zip(heads, elements)))
                else:
                    print("Wrong number of records: " + str(len(elements)) + " instead of " + str(len(heads)))
            line_index += 1
        f.close()

        return records

    except IOError:
        print("File " + filename + " does not exist!")


def process_file(path, file):
    # file exists
    if path[-1] == "/":
        filename = path + file
    else:
        filename = path + "/" + file

    if os.path.isfile(filename):
        return run_extraction(filename)
    else:
        print("Exiting! File " + file + " does not exist.")
        exit(0)


if __name__ == "__main__":
    main(sys.argv[1:])
