#!/usr/bin/env python

import sys
import xlrd
import os
import json


def main(argv):
    if not len(argv) == 2:
        print("Exiting! Use with two arguments: script.py input_file output_file.")
        exit(0)

    path = len(argv) == 2 and os.path.dirname(argv[0])
    filename = len(argv) == 2 and os.path.basename(argv[0])
    output = len(argv) == 2 and argv[1]

    # print filename
    records = process_file(path, filename)

    # data output
    gw = open(output, "w")
    try:
        for record in records:
            print(json.dumps(record, indent=4, sort_keys=True))
            gw.write(json.dumps(record, indent=4) + "\n")
    except IOError:
        print("A list Records is empty!")

    gw.close()
    print("FINISH")


def get_value(ws, wb, row, column):
    '''
    :param ws: sheet
    :param wb: workbook
    :param row: row
    :param column: column
    :return: string with the cell content
    '''
    # returns formatted string based on the Excel cell type and column
    cell_type = ws.cell_type(row, column)

    cell_value = ws.cell_value(row, column)
    # xlrd.XL_CELL_EMPTY = 0
    if cell_type == xlrd.XL_CELL_EMPTY:
        return ""
    # xlrd.XL_CELL_TEXT = 1
    elif cell_type == xlrd.XL_CELL_TEXT and len(cell_value.strip()) > 0:
        return cell_value
    # xlrd.XL_CELL_NUMBER = 2
    elif cell_type == xlrd.XL_CELL_NUMBER and cell_value != "":
        if int(cell_value) == cell_value:
            # integer
            return "{:d}".format(int(cell_value))
        else:
            if column == 13:
                # omega
                return "{:.3f}".format(cell_value)
            else:
                return "{:.2f}".format(cell_value)
    # xlrd.XL_CELL_DATE = 3
    elif cell_type == xlrd.XL_CELL_DATE:
        try:
            dt_tuple = xlrd.xldate_as_tuple(cell_value, wb.datemode)

            return "{:04d}".format(dt_tuple[0]) + "-" + "{:02d}".format(dt_tuple[1]) + "-" + "{:02d}".format(
                dt_tuple[2]) + " " + "{:02d}".format(dt_tuple[3]) + ":" + "{:02d}".format(
                dt_tuple[4]) + ":" + "{:02d}".format(dt_tuple[5])
        except ValueError:
            return ""

    # xlrd.XL_CELL_BOOLEAN = 4
    elif cell_type == xlrd.XL_CELL_BOOLEAN:
        return cell_value
    # xlrd.XL_CELL_ERROR = 5
    elif cell_type == xlrd.XL_CELL_ERROR:
        return ""
    # xlrd.XL_CELL_BLANK = 6
    elif cell_type == xlrd.XL_CELL_BLANK:
        return ""
    else:
        return ""


def get_methods(wb):
    '''
    Parse the Excel sheet with Method/Technique abbreviations.
    :param wb: workbook 
    :return: Dictionary of all methods, key is the abbr
    '''
    ws = wb.sheet_by_name('Method abbr.')
    # print ws.name, ws.nrows, ws.ncols
    methods = {}

    row_index = 0
    while row_index < ws.nrows:
        key_method = get_value(ws, wb, row_index, 0)
        value_method = get_value(ws, wb, row_index, 1)
        methods[key_method] = value_method
        row_index += 1

    return methods


def get_references(wb):
    '''
    Parse the Excel sheet with References.
    :param wb: workbook
    :return: Dictionary of all references, key is the ref number
    '''
    ws = wb.sheet_by_name('References SA95')
    # print ws.name, ws.nrows, ws.ncols
    references = {}

    row_index = 0
    while row_index < ws.nrows:
        # the first column is always an int
        cell_value = ws.cell_value(row_index, 0)
        key_ref = str(int(cell_value))
        value_ref = get_value(ws, wb, row_index, 1)
        references[key_ref] = value_ref
        row_index += 1

    return references


def run_extraction(filename):
    '''
    Parse the Surface Diffusion file. Get the data defined by the header
    'system' -> Full system of a dissusing specie on a substrate
    'diffusing_species' -> Diffusing species
    'substrate' -> Substrate
    'system_note' -> System note
    'ambient' -> Environment such as vacuum, liquid etc.
    'technique' -> Experimental technique
    'coverage_theta' -> Coverage of the diffusing specie
    'diffusion_coefficient' -> Pre-exponential factor of the diffusion coefficient (cm2s-1)
    'diffusion_coefficient_note' -> Pre-exponential factor of the diffusion coefficient note
    'activation_energy' -> Activation energy (kcal/mol)
    'activation_energy_note' -> Activation energy note
    'desorption_energy' -> Desorption energy (kcal/mol)
    'desorption_energy_note' -> Desorption energy note
    'corrugation_omega' -> Corrugation ratio Omega = Ediff/Edes
    'corrugation_omega_note' -> Corrugation ratio note
    'energy_alpha' -> Activation energy alpha= Ediff/kTm
    'energy_alpha_note' -> Activation energy alpha note
    'temperature_range' -> Temperature range relative to the substrate melting point, T/Tm
    'temperature_range_note' -> Temperature range note
    'reference1' -> Reference 1
    'reference2' -> Reference 2
    'reference3' -> Reference 3
    :param filename: Filename of Surface Diffusion data file
    :return: List of Dictionaries containing data
    '''
    # parse excel spreadsheet and create list of dictionaries with key headers
    heads = ['system', 'diffusing_species', 'substrate', 'system_note', 'ambient', 'technique', 'coverage_theta',
             'diffusion_coefficient', 'diffusion_coefficient_note', 'activation_energy', 'activation_energy_note',
             'desorption_energy', 'desorption_energy_note', 'corrugation_omega', 'corrugation_omega_note', 'energy_alpha', 'energy_alpha_note',
             'temperature_range', 'temperature_range_note', 'reference1', 'reference2', 'reference3']
    records = list()

    try:
        wb = xlrd.open_workbook(filename)
        print("The number of worksheets is", wb.nsheets)
        print("Worksheet name(s):", wb.sheet_names())

        methods = get_methods(wb)
        references = get_references(wb)

        for sheetx in range(0, wb.nsheets - 2):
            ws = wb.sheet_by_index(sheetx)
            # print ws.name, ws.nrows, ws.ncols

            row_index = 0
            while row_index < ws.nrows:
                elements = list()

                column_index = 0
                while column_index < 20:
                    value = get_value(ws, wb, row_index, column_index)
                    elements.append(value)
                    column_index += 1

                if elements[0] != 'System' and elements[0] != '' and elements[1] != '' and elements[2] != '' and \
                                elements[19] != '':
                    elements[5] = methods[elements[5]]
                    refs = elements[19].strip().split(",")
                    elements[19] = references['0']
                    if len(refs) == 1:
                        elements.append(references[refs[0].strip()])
                        elements.append("")
                    elif len(refs) > 1:
                        elements.append(references[refs[0].strip()])
                        elements.append(references[refs[1].strip()])

                    if len(heads) == len(elements):
                        records.append(dict(zip(heads, elements)))
                    else:
                        print("Wrong number of records: " + str(len(elements)) + " instead of " + str(len(heads)))

                row_index += 1
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