'''
Formatter
'''
from json import load, dump, loads
#import ujson as json
from sys import exit
from tqdm import tqdm
from copy import deepcopy

#Pick one or more to refine
to_refine = []

#to_refine.append("janaf")
#to_refine.append("khazana_polymer")
#to_refine.append("khazana_vasp")
#to_refine.append("danemorgan")
#to_refine.append("oqmd")
#to_refine.append("cod")
#to_refine.append("sluschi")


#Formats a record into the appropriate schema
#TODO: Actually come up with a schema. Until then, this does nothing useful.
def schema_format(raw_record):
	formatted = raw_record
	return formatted



#Refines/formats a raw feedstock file into a refined, ingestable file (with metadata)
#	raw_file: The unrefined feedstock location
#	refined_file: The output location for the refined feedstock
#	static_metadata: Dict of metadata for the whole feedstock (for example, {"globus_source" : "Globus Dataset 1"}). Default nothing.
#	dynamic_metadata: Dict containing metadata that needs to be computed. Values in the dict should be commands to be used in "eval()", where "data" is the name of the feedstock record (for example, {"title" : "'Big DB - ' + data['composition']" ). Default nothing.
def refine_feedstock(raw_file, refined_file, static_metadata={}, dynamic_metadata={}, verbose=False):
#	if verbose:
#		print("Loading raw feedstock from " + raw_file)
#	with open(raw_file, 'r') as raw:
		#rawtemp = raw.read()
		#print rawtemp
#		raw_stock = load(raw)
#		raw2 = load(raw)
	if verbose:
		print("Loading feedstock from:", raw_file)
		print("Refining feedstock to:", refined_file)
#	if type(raw_stock) is not list:
#		print("Error: Cannot process " + str(type(raw_stock)) + ". Must be list.")
#		exit(-1)
#	if len(raw_stock) == 0:
#		print("Error: No data found")
#		exit(-1)
#	refined_stock = []
	refined_template = {}
	for key, value in static_metadata.items(): 
		refined_template[key] = value
	total = 0
	success = 0
#	for raw_record in tqdm(raw_stock, desc="Refining", disable= not verbose):
	raw_stock = open(raw_file, 'r')
	ref_dump = open(refined_file, 'w')
	for raw_unloaded in tqdm(raw_stock, desc="Refining", disable= not verbose):
		total += 1
		raw_record = loads(raw_unloaded)
		if raw_record:
			refined_record = deepcopy(refined_template)
			for key, value in dynamic_metadata.items():
				refined_record[key] = eval(value.replace("data", "raw_record"))
			refined_record["data"] = schema_format(raw_record)
			#refined_stock.append(refined_record)
			dump(refined_record, ref_dump)
			ref_dump.write("\n")
			success += 1
	if verbose:
		print(str(success) + "/" + str(total) + " records refined.")
#	if refined_file:
#		if verbose:
#			print "Dumping refined feedstock into " + refined_file
#		with open(refined_file, 'w') as refined:
#			dump(refined_stock, refined)
#		if verbose:
#			print "Dumping complete"
	if verbose:
		print("Format of feedstock complete\n")



if __name__ == "__main__":
	raw_dir = "raw_feedstock/"
	ref_dir = "refined_feedstock/"
	verbose = True
	if verbose:
		print("BEGIN")

	#########################
#	refine_feedstock("../datasets/10.5061_dryad.dd56c/classical_interatomic_potentials.json", "../datasets/10.5061_dryad.dd56c/refined_cip.json", verbose=True)
	refine_feedstock(raw_dir + "cip_all.json", ref_dir + "cip_refined.json", verbose = True)


	if "janaf" in to_refine:
		janaf_static = {
			"__source_name" : "janaf",
			"globus_id" : "janaf",
			"globus_source" : "NIST-JANAF",
			"context" : {
				"janaf" : "http://kinetics.nist.gov/janaf/",
				"dc" : "http://dublincore.org/documents/dcmi-terms"
				}
			}
		janaf_dynamic = {
			"globus_subject" : "data['uri']"
			}
		refine_feedstock(raw_dir+"janaf_all.json", ref_dir+"janaf_refined.json", janaf_static, janaf_dynamic, verbose)

	if "khazana_polymer" in to_refine:
		khaz_poly_static = {
			"__source_name" : "khazana_polymer",
			"globus_source" : "Khazana (Polymer)"
			}
		khaz_poly_dynamic = {
			"globus_subject" : "data['uri']"
			}
		refine_feedstock(raw_dir+"khazana_polymer_all.json", ref_dir+"khazana_polymer_refined.json", khaz_poly_static, khaz_poly_dynamic, verbose)

	if "khazana_vasp" in to_refine:
		khaz_vasp_static = {
			"__source_name" : "khazana_dft",
			"globus_source" : "Khazana (DFT)"
			}
		khaz_vasp_dynamic = {
			"globus_subject" : "data['uri']"
			}
		refine_feedstock(raw_dir+"khazana_vasp_all.json", ref_dir+"khazana_vasp_refined.json", khaz_vasp_static, khaz_vasp_dynamic, verbose)

	if "danemorgan" in to_refine:
		danemorgan_static = {
			"__source_name" : "ab_initio_solute_database",
			"globus_source" : "High-throughput Ab-initio Dilute Solute Diffusion Database"
			}
		danemorgan_dynamic = {
			"globus_subject" : "data['uri']"
			}
		refine_feedstock(raw_dir+"danemorgan_all.json", ref_dir+"danemorgan_refined.json", danemorgan_static, danemorgan_dynamic, verbose)

	if "oqmd" in to_refine:
		oqmd_static = {
			"__source_name" : "oqmd",
			"globus_source" : "Open Quantum Materials Database",
			"context" : {
				"oqmd" : "http://www.oqmd.org/",
				"dc" : "http://dublincore.org/documents/dcmi-terms"
				}
			}
		oqmd_dynamic = {
			"globus_subject" : "data['uri']",
			"dc:title" : "'OQMD - ' + data['comp']"
			}
		refine_feedstock(raw_dir+"oqmd_all.json", ref_dir+"oqmd_refined.json", oqmd_static, oqmd_dynamic, verbose)

	if "cod" in to_refine:
		cod_static = {
			"__source_name" : "cod",
			"globus_source" : "Crystallography Open Database"
			}
		cod_dynamic = {
			"globus_subject" : "data['uri']"
			}
		refine_feedstock(raw_dir+"cod_all.json", ref_dir+"cod_refined.json", cod_static, cod_dynamic, verbose)

	if "sluschi" in to_refine:
		sluschi_static = {
			"__source_name" : "sluschi",
			"globus_source" : "Sluschi"
			}
		sluschi_dynamic = {
			"globus_subject" : "data['uri']"
			}
		refine_feedstock(raw_dir+"sluschi_all.json", ref_dir+"sluschi_refined.json", sluschi_static, sluschi_dynamic, verbose)

	if verbose:
		print("END")


