from qmpy import *
from pickle import dump

def printDataset(entries, filename):
	print "Printing to: " + filename
	print "\tNumber of entries: %d"%(entries.count())
#	print >>fp, "id comp energy_pa volume_pa magmom_pa bandgap delta_e stability"
	values = entries.values_list("id", "composition__formula", "calculation__energy_pa", "calculation__output__volume_pa", "calculation__magmom_pa", "calculation__band_gap", "delta_e", "stability")
	count = 0
	out_list = list()

	for e in values:
		output = dict()
		if e and e[0] and e[1]:
			output["oqmd:id"] = str(e[0])
			output["comp:comp"] = e[1].replace(" ", "")
			output["oqmd:energy_pa"] = e[2]
			output["oqmd:volume_pa"] = e[3]
			output["oqmd:magmom_pa"] = e[4]
			output["oqmd:bandgap"] = e[5]
			output["oqmd:delta_e"] = e[6]
			output["oqmd:stability"] = e[7]
#			output = str(e[0]) + ","
#			output += e[1].replace(" ", "")
#			for x in e[2:]:
#				output += "," + str(x)
#			print >>fp, output

#			output["testing"] = "example_1000_ingest"

			meta_out = {
				"globus_subject" : "http://oqmd.org/materials/composition/" + output["comp:comp"],
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
			out_list.append(meta_out)
			count+=1
	fp = open(filename, 'w')
	dump(out_list, fp)
	fp.close()
	print str(count) + " records saved."

e = Formation.objects.filter(fit = "standard")

printDataset(e, "oqmd_all_v2.pickle")

#everything = Entry.objects.all()

#printDataset(everything, "oqmd_everything.txt")

#f = e.filter(stability__lte = 0)

#printDataset(f, "oqmd_hull.txt")

#f = e.filter(calculation__path__contains = 'icsd')

#printDataset(f, "icsd_all.txt")
