from json import load, dump
from sys import exit
from tqdm import tqdm
from copy import deepcopy

#Pick one or more to refine
to_refine = []
to_refine.append("janaf")
to_refine.append("khazana_polymer")
to_refine.append("khazana_vasp")
to_refine.append("danemorgan")
to_refine.append("oqmd")


#Formats a record into the appropriate schema
#TODO: Actually come up with a schema. Until then, this does nothing useful.
def schema_format(raw_record):
	formatted = raw_record
	return formatted



#Refines/formats a raw feedstock file into a refined, ingestable file (with metadata)
#	raw_file: The unrefined feedstock location
#	refined_file: The output location for the refined feedstock. Default None, which suppresses writing out to file.
#	static_metadata: Dict of metadata for the whole feedstock (for example, {"globus_source" : "Globus Dataset 1"}). Default nothing.
#	dynamic_metadata: Dict containing metadata that needs to be computed. Values in the dict should be commands to be used in "eval()", where "data" is the name of the feedstock record (for example, {"title" : "'Big DB - ' + data['composition']" ). Default nothing.
def refine_feedstock(raw_file, refined_file=None, static_metadata={}, dynamic_metadata={}, verbose=False):
	if verbose:
		print "Loading raw feedstock"
	with open(raw_file, 'r') as raw:
		raw_stock = load(raw)
	if verbose:
		print "Refining feedstock"
	if type(raw_stock) is not list:
		print "Error: Cannot process " + str(type(raw_stock)) + ". Must be list."
		exit(-1)
	if len(raw_stock) == 0:
		print "Error: No data found"
		exit(-1)
	refined_stock = []
	refined_template = {}
	for key, value in static_metadata.iteritems(): 
		refined_template[key] = value
	total = 0
	success = 0
	for raw_record in tqdm(raw_stock, desc="Refining", disable= not verbose):
		total += 1
		if raw_record:
			refined_record = deepcopy(refined_template)
			for key, value in dynamic_metadata.iteritems():
				refined_record[key] = eval(value.replace("data", "raw_record"))
			refined_record["data"] = schema_format(raw_record)
			refined_stock.append(refined_record)
			success += 1
	if verbose:
		print str(success) + "/" + str(total) + " records refined."
	if refined_file:
		if verbose:
			print "Dumping refined feedstock into " + refined_file
		with open(refined_file, 'w') as refined:
			dump(refined_stock, refined)
		if verbose:
			print "Dumping complete"
	if verbose:
		print "Format of feedstock from " + raw_file + " complete"



if __name__ == "__main__":
	raw_dir = "raw_feedstock/"
	ref_dir = "refined_feedstock/"

	if "janaf" in to_refine:
		janaf_static = {
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
		refine_feedstock(raw_dir+"janaf_all.json", ref_dir+"janaf_refined.json", janaf_static, janaf_dynamic, True)

	if "khazana_polymer" in to_refine:
		khaz_poly_static = {
			"globus_source" : "Khazana"
			}
		khaz_poly_dynamic = {
			"globus_subject" : "data['uri']"
			}
		refine_feedstock(raw_dir+"khazana_polymer_all.json", ref_dir+"khazana_polymer_refined.json", khaz_poly_static, khaz_poly_dynamic, True)

	if "khazana_vasp" in to_refine:
		khaz_vasp_static = {
			"globus_source" : "Khazana"
			}
		khaz_vasp_dynamic = {
			"globus_subject" : "data['uri']"
			}
		refine_feedstock(raw_dir+"khazana_vasp_all.json", ref_dir+"khazana_vasp_refined.json", khaz_vasp_static, khaz_vasp_dynamic, True)

	if "danemorgan" in to_refine:
		danemorgan_static = {
			"globus_source" : "High-throughput Ab-initio Dilute Solute Diffusion Database"
			}
		danemorgan_dynamic = {
			"globus_subject" : "data['uri']"
			}
		refine_feedstock(raw_dir+"danemorgan_all.json", ref_dir+"danemorgan_refined.json", danemorgan_static, danemorgan_dynamic, True)

	if "oqmd" in to_refine:
		oqmd_static = {
			"globus_source" : "Open Quantum Materials Database",
			"context" : {
				"oqmd" : "http://www.oqmd.org/",
				"dc" : "http://dublincore.org/documents/dcmi-terms"
				}
			}
		oqmd_dynamic = {
			"globus_subject" : "'http://oqmd.org/materials/entry/' + data['oqmd_id']",
			"dc:title" : "'OQMD - ' + data['comp']"
			}


