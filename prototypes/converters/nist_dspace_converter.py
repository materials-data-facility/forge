import paths
import multi_converter
from json import load, loads, dump

ids = ["177", "184"]

def convert_nist_dspace(id_num, file_format, verbose=False):
	root = paths.datasets + "nist_dspace/" + id_num + "/"
	if file_format == "vasp":
		args = {
			"uri" : "https://materialsdata.nist.gov/dspace/rest/items/",
			"keep_dir_name_depth" : 1,
			"root" : paths.datasets + "nist_dspace/" + id_num + "/",
			"file_pattern" : "OUTCAR",
			"file_format" : "vasp",
			"verbose" : verbose,
			"output_file" : paths.raw_feed + "nist_dspace_" + id_num + "_all.json",
			"data_exception_log" : paths.datasets + "nist_dspace/" + id_num + "/errors.txt",
			"uri_adds" : [id_num, "#", "filename"],
			"max_records" : -1,
			"archived" : False,
			"feedsack_size" : 10,
			"feedsack_file" : paths.sack_feed + "nist_dspace_" + id_num + "_10.json"
			}
		if verbose:
			print("Processing #" + id_num)
		multi_converter.process_data(args)
		if verbose:
			print("Adding metadata")
	
		data = []
		with open(paths.raw_feed + "nist_dspace_" + id_num + "_all.json", 'r') as in_file:
			for line in in_file:
				data.append(loads(line))
		with open(paths.datasets + "nist_dspace/" + id_num + "/" + id_num + "_metadata.json", 'r') as meta_file:
			metadata = load(meta_file)
		for datum in data:
			for elem in metadata:
				key = elem["key"].replace(".", "_")
				if not datum.get(key, None): #Some fields are repeated
					datum[key] = []
				datum[key].append(elem["value"])
		with open(paths.raw_feed + "nist_dspace_" + id_num + "_all.json", 'w') as out_file:
			for datum in data:
				dump(datum, out_file)
				out_file.write("\n")

	else:
		print("Invalid format")


if __name__ == "__main__":
	for num in ids:
		convert_nist_dspace(num, "vasp", verbose=True)
	print("Done.")



