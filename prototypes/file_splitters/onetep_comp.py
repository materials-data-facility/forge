#Evan Pike dep78@cornell.edu
#Read ONETEP input files for composition
import os

def get_elements(data):

    """with open(in_file, 'r') as raw_in:
        data = raw_in.read()

    start = data.find("%block species\n")
    end = data.find("%endblock species\n")
    element_data = data[start+len("%block species")+1:end-1].split("\n")
    
    elements = {}   #Keys are the 1st column (Essentially whatever they name it) and Values are 2nd column (The element)
    for line in element_data:
        given_name = line.split()[0]
        if given_name not in elements:
            elements[given_name] = line.split()[1]

    return(elements)"""
    line = data.readline().strip()
    elements = {}
    while "%endblock" not in line.lower():
        given_name = line.split()[0]
        if given_name not in elements:
            elements[given_name] = line.split()[1]

    return(elements)


def get_comp(data, element_dict):

    """with open(in_file, 'r') as raw_in:
        data = raw_in.read()
        
    start = data.find("%block positions_abs")
    end = data.find("%endblock positions_abs")
    comp_data = data[start+len("%block position_abs")+2:end-1].split("\n")
    
    element_dict = get_elements(in_file)"""
    element_dict = element_dict
    line = data.readline().strip()
    composition = {}
    while "%endblock position_abs" not in line.lower():
        if line == "ang":
            continue
        given_element = line.split()[0]
        element = element_dict[given_element]
        if element in composition:
            composition[element] += 1
        else:
            composition[element] = 1
            
    comp = ""
    for element in composition:
        comp += element + str(composition[element])
    
    return(comp)

def find_comp(in_file):

    data = open(in_file, 'r')
    line = data.readline()

    while line:
        lower_line = line.strip().lower()
        if "%block species" in lower_line:
            element_dict = get_elements()
        if "%block positions_abs" in lower_line:
            comp = get_comp(data, element_dict)
            return(comp)


if __name__ == '__main__':
    find_comp(os.getcwd() + "/datasets/pjw/convtest_NCPP/PTCDA_400_6.out")