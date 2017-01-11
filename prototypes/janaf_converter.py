import json
from qmpy import parse_comp
import os
import shutil
from numpy import sum
from pickle import dump

def parse_janaf_file(filename):
        '''
        Parse the JANAF file. Get the following data

        'comp' -> Composition
        'cas' -> CAS registry number
        'T' -> Measurement temps
        'Cp' -> Heat capacity
        'dH' -> Enthalpy of formation
        'dG' -> Gibbs energy of formation
        'dS' -> Entropy of formation
        'state' -> Sample state
        :param filename: Filename of JANAF data file
        :return: Dictionary containing data
        '''

        # Parse file
        try:
                data = json.load(open(filename, 'r'))
        except:
                return None

        # Check that file contains data
        if not 'data' in data.keys():
                return None

                # Get the number of atoms per formula unit
        comp = data['identifiers']['chemical formula']
        if len(comp) < 1: return None
        try:
            n_atoms = sum(parse_comp(comp).values())
        except:
            return None
#        print comp, n_atoms

        # Load in data
        T = []
        dH = []
        dG = []
        Cp = []
        for row in data['data']:
                try:
                        t = row['T']
                        cp = row['values'][u'Cp\xb0']
                        dh = row['values'][u'delta-f H\xb0']
                        dg = row['values'][u'delta-f G\xb0']
                except:
                        continue
                T.append(float(t))
                Cp.append(float(cp))
                dH.append(float(dh))
                dG.append(float(dg))

                # Normalize by number of atoms in the formula unit
        dH = [ x / n_atoms for x in dH ]
        dG = [ x / n_atoms for x in dG ]

        # Prepare output
	output = {}
        output['janaf:file'] = filename
        output['comp:comp'] = data['identifiers']['chemical formula']
        output['janaf:state'] = data['identifiers']['state']
        output['janaf:cas'] = data['identifiers']['cas registry number']
 	
 	output['janaf:T'] = T
        output['janaf:dH'] = dH
        output['janaf:dG'] = dG
        output['janaf:dS'] = [ (h - g) / t if t > 0 else 0 for h,g,t in zip(dH,dG,T)]
        output['janaf:Cp'] = Cp
	
#	output["testing"] = "janaftest4"
#	output["janaf:comp"] = "1234"

	meta_out = {
		"globus_subject" : "http://kinetics.nist.gov/janaf/" + output["comp:comp"] + "_" + output["janaf:state"],
		"globus_id" : "janaf",
		"globus_source" : "NIST-JANAF",
		"context" : {
			"janaf" : "http://kinetics.nist.gov/janaf/",
			"dc" : "http://dublincore.org/documents/dcmi-terms",
			"comp" : "composition"
			},
		"data" : output
		}
	meta_out["data"]["dc:title"] = "JANAF - " + output["comp:comp"] + " - " + output["janaf:state"]

        return meta_out


if __name__ == "__main__":
	data = []
	data_dir = "srd13_janaf"
	out_filename = "janaf_json.pickle"
	count = 0
	full_count = 0
	for f in os.listdir(data_dir):
		entry = parse_janaf_file(os.path.join(data_dir, f))
		full_count +=1
		if entry is not None:
			data.append(entry)
			count +=1
#	print json.dumps(data[0], sort_keys=True, indent=4, separators=(',', ': '))
	out_file = open(out_filename, 'w')
#	print >>out_file, data
	dump(data, out_file)
	out_file.close()
	print str(count) + "/" + str(full_count) + " converted."




