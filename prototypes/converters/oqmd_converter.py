'''
Converter (oqmd)
'''
from qmpy import *
#from pickle import dump
from json import dump
from six import print_
from tqdm import tqdm
from bson import ObjectId

import paths #Contains relative paths to data

feedstock = True

def printDataset(entries, filename, mdf_meta, verbose=False):
	all_uri = []
	if verbose:
		print_("Printing to: " + filename)
		print_("\tNumber of entries: %d"%(entries.count()))
#	print >>fp, "id comp energy_pa volume_pa magmom_pa bandgap delta_e stability"
	values = entries.values_list("entry__id", "composition__formula", "calculation__energy_pa", "calculation__output__volume_pa", "calculation__magmom_pa", "calculation__band_gap", "delta_e", "stability")
	count = 0
#	out_list = list()
	with open(filename, 'w') as fp:
		if feedstock:
			feed_file = open(paths.sack_feed + "oqmd_10000.json", 'w')
		for e in values:
			output = dict()
			if e and e[0] and e[1]:
				output["oqmd_id"] = str(e[0])
				output["comp"] = e[1].replace(" ", "")
				output["energy_pa"] = e[2]
				output["volume_pa"] = e[3]
				output["magmom_pa"] = e[4]
				output["bandgap"] = e[5]
				output["delta_e"] = e[6]
				output["stability"] = e[7]
#				output = str(e[0]) + ","
#				output += e[1].replace(" ", "")
#				for x in e[2:]:
#					output += "," + str(x)
#				print >>fp, output

#				output["testing"] = "oqmd_id_test"
				'''
				meta_out = {
					#"globus_subject" : "http://oqmd.org/materials/composition/" + output["comp:comp"],
					"globus_subject" : "http://oqmd.org/materials/entry/" + output["oqmd:id"],
					"globus_id" : "oqmd",
					"globus_source" : "Open Quantum Materials Database",
					"context" : {
						"oqmd" : "http://www.oqmd.org/",
						"dc" : "http://dublincore.org/documents/dcmi-terms",
						"comp" : "composition"
						},
					"data" : output
					}
				meta_out["data"]["dc:title"] = "OQMD - " + output["comp:comp"]
				'''
				output["uri"] = "http://oqmd.org/materials/entry/" + output["oqmd_id"]
				all_uri.append(output["uri"])
				#out_list.append(output)

				#Metadata
				feedstock_data = {}
				feedstock_data["mdf_id"] = str(ObjectId())
				feedstock_data["mdf_source_name"] = mdf_meta["mdf_source_name"]
				feedstock_data["mdf_source_id"] = mdf_meta["mdf_source_id"]
#				feedstock_data["globus_source"] = mdf_meta.get("globus_source", "")
				feedstock_data["mdf_datatype"] = mdf_meta["mdf_datatype"]
				feedstock_data["acl"] = mdf_meta["acl"]
				feedstock_data["globus_subject"] = output["uri"]
				feedstock_data["mdf-publish.publication.collection"] = mdf_meta["collection"]
				feedstock_data["mdf-base.material_composition"] = output["comp"]
				feedstock_data["data"] = output

				dump(feedstock_data, fp)
				fp.write('\n')

#				if count > 100000:
#					print str(output)
#				if count > 100100:
#					break


				if feedstock and count < 10000:
					dump(feedstock_data, feed_file)
					feed_file.write('\n')
				count+=1
		if feedstock:
			feed_file.close()
#	fp = open(filename, 'w')
#	dump(out_list, fp)
#	fp.close()
	if verbose:
		print_(str(count) + " records saved.")

	#duplicates = [x for x in all_uri if all_uri.count(x) > 1]
	if len(all_uri) != len(set(all_uri)): #duplicates:
		print_("Warning: Duplicate URIs found!")
		print_("This is due to duplicate IDs and can be ignored unless the dataset changes.")
	
	'''
	import json
	print "Dumping to JSON"
	print "Dumping all"
	with open("oqmd_all.json", 'w') as oq1:
		json.dump(out_list, oq1)
	'''
#	print "Dumping 10,000"
#	with open(paths.sack_feed + "oqmd_10000.json", 'w') as oq2:
#		dump(out_list[:10000], oq2)
#	print "Done"
	

if __name__ == "__main__":
	mdf_metadata = {
		"mdf_source_name" : "oqmd",
		"mdf_source_id" : 1,
#		"globus_source" : "Open Quantum Materials Database",
		"mdf_datatype" : "oqmd",
		"acl" : ["public"],
		"collection" : "Open Quantum Materials Database"
		}
	filename = paths.raw_feed + "oqmd_all.json"
	e = Formation.objects.filter(fit = "standard")
	printDataset(e, filename, mdf_metadata, verbose=True)

#everything = Entry.objects.all()

#printDataset(everything, "oqmd_everything.txt")

#f = e.filter(stability__lte = 0)

#printDataset(f, "oqmd_hull.txt")

#f = e.filter(calculation__path__contains = 'icsd')

#printDataset(f, "icsd_all.txt")
