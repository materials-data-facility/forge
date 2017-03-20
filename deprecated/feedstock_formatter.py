'''
Formatter
'''
from json import load, dump, loads
#import ujson as json
from sys import exit
from tqdm import tqdm
from copy import deepcopy
from bson import ObjectId

#Pick one or more to refine
to_refine = []

#to_refine.append("janaf")
#to_refine.append("khazana_polymer")
#to_refine.append("khazana_vasp")
#to_refine.append("danemorgan")
#to_refine.append("oqmd")
#to_refine.append("cod")
#to_refine.append("sluschi")
#to_refine.append("hopv")
#to_refine.append("cip")
#to_refine.append("nanomine")
#to_refine.append("nist_ip")
#to_refine.append("nist_dspace")
#to_refine.append("metadata_matin")
#to_refine.append("metadata_cxidb")
to_refine.append("metadata_nist")
#to_refine.append("pppdb")
#to_refine.append("metadata_materials_commons")


#Formats a record into the appropriate schema and mints BSON ID
#TODO: Actually define a schema.
def schema_format(raw_record):
	formatted = raw_record
	formatted["mdf_id"] = str(ObjectId())
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
#	refine_feedstock(raw_dir + "cip_all.json", ref_dir + "cip_refined.json", verbose = True)


	if "janaf" in to_refine:#Moved
		janaf_static = {
			"mdf_source_name" : "janaf",
			"mdf_source_id" : 2,
			"globus_id" : "janaf",
			"globus_source" : "NIST-JANAF",
			"context" : {
				"janaf" : "http://kinetics.nist.gov/janaf/",
				"dc" : "http://dublincore.org/documents/dcmi-terms"
				},
			"acl" : ["public"]
			}
		janaf_dynamic = {
			"globus_subject" : "data['uri']"
			}
		refine_feedstock(raw_dir+"janaf_all.json", ref_dir+"janaf_refined.json", janaf_static, janaf_dynamic, verbose)

	if "khazana_polymer" in to_refine:#Moved
		khaz_poly_static = {
			"mdf_source_name" : "khazana_polymer",
			"mdf_source_id" : 4,
			"globus_source" : "Khazana (Polymer)",
			"acl" : ["public"]
			}
		khaz_poly_dynamic = {
			"globus_subject" : "data['uri']"
			}
		refine_feedstock(raw_dir+"khazana_polymer_all.json", ref_dir+"khazana_polymer_refined.json", khaz_poly_static, khaz_poly_dynamic, verbose)

	if "khazana_vasp" in to_refine:#Moved
		khaz_vasp_static = {
			"mdf_source_name" : "khazana_dft",
			"mdf_source_id" : 5,
			"globus_source" : "Khazana (DFT)",
			"acl" : ["public"]
			}
		khaz_vasp_dynamic = {
			"globus_subject" : "data['uri']"
			}
		refine_feedstock(raw_dir+"khazana_vasp_all.json", ref_dir+"khazana_vasp_refined.json", khaz_vasp_static, khaz_vasp_dynamic, verbose)

	if "danemorgan" in to_refine:#Moved
		danemorgan_static = {
			"mdf_source_name" : "ab_initio_solute_database",
			"mdf_source_id" : 3,
			"globus_source" : "High-throughput Ab-initio Dilute Solute Diffusion Database",
			"acl" : ["public"]
			}
		danemorgan_dynamic = {
			"globus_subject" : "data['uri']"
			}
		refine_feedstock(raw_dir+"danemorgan_all.json", ref_dir+"danemorgan_refined.json", danemorgan_static, danemorgan_dynamic, verbose)

	if "oqmd" in to_refine:#Moved
		oqmd_static = {
			"mdf_source_name" : "oqmd",
			"mdf_source_id" : 1,
			"globus_source" : "Open Quantum Materials Database",
			"context" : {
				"oqmd" : "http://www.oqmd.org/",
				"dc" : "http://dublincore.org/documents/dcmi-terms"
				},
			"acl" : ["public"]
			}
		oqmd_dynamic = {
			"globus_subject" : "data['uri']",
			"dc:title" : "'OQMD - ' + data['comp']"
			}
		refine_feedstock(raw_dir+"oqmd_all.json", ref_dir+"oqmd_refined.json", oqmd_static, oqmd_dynamic, verbose)

	if "cod" in to_refine:#Moved
		cod_static = {
			"mdf_source_name" : "cod",
			"mdf_source_id" : 6,
			"globus_source" : "Crystallography Open Database",
			"acl" : ["public"]
			}
		cod_dynamic = {
			"globus_subject" : "data['uri']"
			}
		refine_feedstock(raw_dir+"cod_all.json", ref_dir+"cod_refined.json", cod_static, cod_dynamic, verbose)

	if "sluschi" in to_refine:#Moved
		sluschi_static = {
			"mdf_source_name" : "sluschi",
			"mdf_source_id" : 7,
			"globus_source" : "Sluschi",
			"acl" : ["public"]
			}
		sluschi_dynamic = {
			"globus_subject" : "data['uri']"
			}
		refine_feedstock(raw_dir+"sluschi_all.json", ref_dir+"sluschi_refined.json", sluschi_static, sluschi_dynamic, verbose)
	
	if "hopv" in to_refine:#Moved
		hopv_static = {
			"mdf_source_name" : "hopv",
			"mdf_source_id" : 8,
			"globus_source" : "Harvard Open Photovoltaic Database",
			"acl" : ["public"]
			}
		hopv_dynamic = {
			"globus_subject" : "data['uri']"
		}
		refine_feedstock(raw_dir+"hopv_all.json", ref_dir+"hopv_refined.json", hopv_static, hopv_dynamic, verbose)

	if "cip" in to_refine:#Moved
		cip_static = {
			"mdf_source_name" : "cip",
			"mdf_source_id" : 9,
			"globus_source" : "Evaluation and comparison of classical interatomic potentials through a user-friendly interactive web-interface",
			"acl" : ["public"]
			}
		cip_dynamic = {
			"globus_subject" : "data['uri']"
			}
		refine_feedstock(raw_dir+"cip_all.json", ref_dir+"cip_refined.json", cip_static, cip_dynamic, verbose)
	
	if "nanomine" in to_refine:#Moved
		nanomine_static = {
			"mdf_source_name" : "nanomine",
			"mdf_source_id" : 10,
			"globus_source" : "Nanomine",
			"acl" : ["c8745ef4-d274-11e5-bee8-3b6845397ac9", "117e8833-68f5-4cb2-afb3-05b25db69be1"] #blaiszik, jgaff
			}
		nanomine_dynamic = {
			"globus_subject" : "data['uri']"
			}
		refine_feedstock(raw_dir+"nanomine_all.json", ref_dir+"nanomine_refined.json", nanomine_static, nanomine_dynamic, verbose)

	if "nist_ip" in to_refine:#Moved
		nist_ip_static = {
			"mdf_source_name" : "nist_ip",
			"mdf_source_id" : 11,
			"globus_source" : "NIST Interatomic Potentials",
			"acl" : ["public"]
			}
		nist_ip_dynamic = {
			"globus_subject" : "data['uri']"
			}
		refine_feedstock(raw_dir+"nist_ip_all.json", ref_dir+"nist_ip_refined.json", nist_ip_static, nist_ip_dynamic, verbose)

	if "nist_dspace" in to_refine:#Moved
		nist_dspace_static = {
			"mdf_source_name" : "nist_dspace",
			"mdf_source_id" : 12,
			"acl" : ["public"]
			}
		nist_dspace_dynamic = {
			"globus_source" : "data['dc_title'][0]",
			"globus_subject" : "data['uri']"
			}
		from utils import find_files
		for raw_file in find_files(root=raw_dir, file_pattern="^nist_dspace"):
			ref_file = raw_file["filename"].replace("all", "refined") + raw_file["extension"]
			refine_feedstock(raw_dir+raw_file["filename"]+raw_file["extension"], ref_dir+ref_file, nist_dspace_static, nist_dspace_dynamic, verbose)

	if "metadata_matin" in to_refine:#Moved
		metadata_matin_static = {
			"mdf_source_name" : "matin",
			"mdf_source_id" : 13,
			"globus_source" : "MATIN",
			"acl" : ["public"]
			}
		metadata_matin_dynamic = {
			"globus_subject" : "data['dc.identifier']"
			}
		refine_feedstock(raw_dir+"matin_metadata_all.json", ref_dir+"matin_metadata_refined.json", metadata_matin_static, metadata_matin_dynamic, verbose)

	if "metadata_cxidb" in to_refine:#Moved
		metadata_cxidb_static = {
			"mdf_source_name" : "cxidb",
			"mdf_source_id" : 14,
			"globus_source" : "CXIDB",
			"acl" : ["public"]
			}
		metadata_cxidb_dynamic = {
			"globus_subject" : "data['dc.identifier']"
			}
		refine_feedstock(raw_dir+"cxidb_metadata_all.json", ref_dir+"cxidb_metadata_refined.json", metadata_cxidb_static, metadata_cxidb_dynamic, verbose)

	if "metadata_nist" in to_refine:#Moved
		metadata_nist_static = {
			"mdf_source_name" : "nist_dspace",
			"mdf_source_id" : 12,
			"globus_source" : "NIST DSpace (metadata)",
			"acl" : ["public"]
			}
		metadata_nist_dynamic = {
			"globus_subject" : "data['dc.identifier']"
			}
		refine_feedstock(raw_dir+"nist_metadata_all.json", ref_dir+"nist_metadata_refined.json", metadata_nist_static, metadata_nist_dynamic, verbose)
	
	if "pppdb" in to_refine:#Moved
		pppdb_static = {
			"mdf_source_name" : "pppdb",
			"mdf_source_id" : 15,
			"globus_source" : "PPPDB",
			"acl" : ["public"]
			}
		pppdb_dynamic = {
			"globus_subject" : "data['dc.identifier']"
			}
		refine_feedstock(raw_dir+"pppdb_all.json", ref_dir+"pppdb_refined.json", pppdb_static, pppdb_dynamic, verbose)

	if "metadata_materials_commons" in to_refine:#Moved
		metadata_mc_static = {
			"mdf_source_name" : "materials_commons",
			"mdf_source_id" : 16,
			"globus_source" : "Materials Commons",
			"acl" : ["public"]
			}
		metadata_mc_dynamic = {
			"globus_subject" : "data['dc.identifier']"
			}
		refine_feedstock(raw_dir+"materials_commons_metadata_all.json", ref_dir+"materials_commons_metadata_refined.json", metadata_mc_static, metadata_mc_dynamic, verbose)




	if verbose:
		print("END")


