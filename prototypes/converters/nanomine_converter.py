from json import load, dump
from tqdm import tqdm
import os
import paths
import re
#import bson
#import bsonjs
#import json_converter
#from utils import find_files

def fix_table(table):
	headers = []
	for head in table["headers"]["column"]:
		try:
			headers.append(head["#text"])
		except KeyError: #Some tables don't have headers
			headers.append("UNKNOWN")
		except TypeError: #Some headers aren't dicts
			headers.append(head)
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
				except KeyError: #Some tables don't have data?
					subdata.append(float("nan")) #Fix it with NaNs
				except ValueError: #Some people like to put headers in the data portion of tables
					headers.append(column["#text"])
				except TypeError: #Some columns aren't dicts
					subdata.append(column)
				
		data.append(subdata)
	return {"headers":headers, "data":data}


#Recursive parser, calls fix_table for only change in data
def recursive_parse(data):
	if type(data) is list:
		new_data = []
		for elem in data:
			new_data.append(recursive_parse(elem))
	elif type(data) is dict:
		if "headers" in data.keys() and "rows" in data.keys():
			try:
				new_data = fix_table(data)
			except Exception as err: #There's only one record that causes unknown formatting issues. Ignoring.
				#print(type(err), err)
				new_data = None
		else:
			new_data = {}
			for key, value in data.items():
				new_data[key] = recursive_parse(value)
	else:
		new_data = data
	return new_data


#Take JSON file and rebuild with sensible data structures
def convert_nanomine(in_file, out_file, uri_prefix="", sack_size=0, sack_file=None, verbose=False):
	all_uri = []
	with open(out_file, 'w') as output_f:
		if sack_size > 0:
			sack = open(sack_file, 'w')
		count = 0
#		for in_file_data in tqdm(find_files(dir_path, verbose=verbose), desc="Processing files", disable= not verbose):
#			in_file = os.path.join(in_file_data["path"], in_file_data["filename"] + in_file_data["extension"])
#			print(in_file)
#			with open(in_file, 'r') as input_f:
#				in_data = load(input_f)
#			converted = rec_parse(in_data)
#		with open(out_file, 'w') as output_f:
#			if sack_size > 0:
#				sack = open(sack_file, 'w')
		with open(in_file, 'r') as input_f:
			for str_record in tqdm(input_f, desc="Processing files", disable= not verbose):
				#Replace BSON terms with Python-friendly ones
				clean_record = str_record.replace("null", 'float("nan")').replace("false", "False")
				record = eval(clean_record) #String version of dict converted to actual dict
				converted = recursive_parse(record)
#				print(converted)
				if converted and type(converted) is dict: #Type should be dict, but check anyway
					converted['uri'] = uri_prefix + converted["_id"]["$oid"]
					dump(converted, output_f)
					output_f.write('\n')
					if count < sack_size:
						dump(converted, sack)
						sack.write('\n')
					count += 1
				else:
					raise ValueError("Single record must be dict")
#				elif type(converted) is list:
#					for subrecord in converted:
#						subrecord["uri"] = in_file_data["filename"]
#						dump(subrecord, output_f)
#						output_f.write('\n')
#						if count < sack_size:
#							dump(subrecord, sack)
#							sack.write('\n')
#						count += 1
		if sack_size > 0:
			sack.close()
	if verbose:
		print("Processed", count, "records.")
	duplicates = [x for x in all_uri if all_uri.count(x) > 1]
	if duplicates:
		print("Warning: Duplicate URIs found:\n", set(duplicates))

if __name__ == "__main__":
#	convert_nanomine(paths.datasets + "nanomine/nanomine_results", paths.raw_feed + "nanomine_all.json", 10, paths.sack_feed + "nanomine_10.json", verbose=True)
	convert_nanomine(paths.datasets + "nanomine/nanomine.dump", paths.raw_feed + "nanomine_all.json", uri_prefix="http://nanomine.northwestern.edu:8000/explore/detail_result_keyword?id=", sack_size=10, sack_file=paths.sack_feed + "nanomine_10.json", verbose=True)

