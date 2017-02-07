from json import load, dump
from tqdm import tqdm
import os
import paths
import json_converter
from utils import find_files

def fix_table(table):
	headers = []
	for head in table["headers"]["column"]:
		try:
			headers.append(head["#text"])
		except KeyError: #Some tables don't have headers
			headers.append("UNKNOWN")
	data = []
	for row in table["rows"]["row"]:
		if type(row) is not list: #Some rows are lists, some are not
			row_list = [row] #Now all rows are lists
		else:
			row_list = row
		subdata = []
		for row_elem in row_list:
			for column in row_elem["column"]:
				try:
					subdata.append(float(column["#text"]))
				except KeyError: #And some tables don't have data?
					subdata.append(float("nan")) #Fix it with NaNs
				except ValueError: #Some people like to put headers in the data portion of tables
					headers.append(column["#text"])
				
		data.append(subdata)
	return { "headers":headers, "data":data}


#Recursive parser, calls fix_table for only change in data
def rec_parse(data):
	if type(data) is list:
		new_data = []
		for elem in data:
			new_data.append(rec_parse(elem))
	elif type(data) is dict:
		if "headers" in data.keys() and "rows" in data.keys():
			try:
				new_data = fix_table(data)
			except Exception as err:
				print(err)
				new_data = None
		else:
			new_data = {}
			for key, value in data.items():
				new_data[key] = rec_parse(value)
	else:
		new_data = data
	return new_data


#Take JSON file and rebuild with sensible data structures
def convert_nanomine(dir_path, out_file, sack_size=0, sack_file=None, verbose=False):
	with open(out_file, 'w') as output_f:
		if sack_size > 0:
			sack = open(sack_file, 'w')
		count = 0
		for in_file_data in tqdm(find_files(dir_path, verbose=verbose), desc="Processing files", disable= not verbose):
			in_file = os.path.join(in_file_data["path"], in_file_data["filename"] + in_file_data["extension"])
#			print(in_file)
			with open(in_file, 'r') as input_f:
				in_data = load(input_f)
			converted = rec_parse(in_data)
#		with open(out_file, 'w') as output_f:
#			if sack_size > 0:
#				sack = open(sack_file, 'w')
			if type(converted) is list:
#				count = 0
				for record in converted:
					record["uri"] = in_file_data["filename"]
					dump(record, output_f)
					output_f.write('\n')
					if count < sack_size:
						dump(record, sack)
						sack.write('\n')
					count += 1
			else:
				converted['uri'] = in_file_data["filename"]
				dump(converted, output_f)
				output_f.write('\n')
				if count < sack_size:
					dump(converted, sack)
					sack.write('\n')
				count += 1
		if sack_size > 0:
			sack.close()
		if verbose:
			print("Processed", count, "records.")

if __name__ == "__main__":
	convert_nanomine(paths.datasets + "nanomine/nanomine_results", paths.raw_feed + "nanomine_all.json", 10, paths.sack_feed + "nanomine_10.json", verbose=True)

