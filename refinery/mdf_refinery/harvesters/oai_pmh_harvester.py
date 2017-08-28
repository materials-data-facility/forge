import requests
from sys import exit
from json import dump, loads
import xmltodict
import os
import os.path
from shutil import rmtree
from tqdm import tqdm

#List of resources to harvest
to_harvest = []
#to_harvest.append("matin")
to_harvest.append("nist_mrr")


#Collects available data from a resource using OAI-PMH and saves to the given directory
#out_dir: The path to the directory (which will be created) for the data files
#base_url: The URL to the OAI-PMH resource
#metadata_prefixes: List of metadataPrefix values to use in fetching records. Requires at least one. Default ["oai_dc"].
#resource_types: List of setSpec resource types to harvest. If empty, will harvest all. Default empty.
#existing_dir:
#       -1: Remove out_dir if it exists
#        0: Error if out_dir exists (Default)
#        1: Overwrite files in out_dir if there are path collisions
#verbose: Print status messages? Default False
def harvest(out_dir, base_url, metadata_prefixes=["oai_dc"], resource_types=[], existing_dir=0, verbose=False):
    if os.path.exists(out_dir):
        if existing_dir == 0:
            exit("Directory '" + out_dir + "' exists")
        elif not os.path.isdir(out_dir):
            exit("Error: '" + out_dir + "' is not a directory")
        elif existing_dir == -1:
            rmtree(out_dir)
            os.mkdir(out_dir)
    else:
        os.mkdir(out_dir)

    #Fetch list of records
    records = []
    for prefix in metadata_prefixes:
        record_res = requests.get(base_url + "?verb=ListRecords&metadataPrefix=" + prefix)
        if record_res.status_code != 200:
            exit("Records GET failure: " + str(record_res.status_code) + " error")
        result = xmltodict.parse(record_res.content)
#       print(result)
        try:
            new_records = result["OAI-PMH"]["ListRecords"]["record"]
            records.append(new_records)
        except KeyError: #No results
            if verbose:
                print("No results for", prefix)

    for record_entry in tqdm(records, desc="Fetching records", disable= not verbose):
        for record in (record_entry if type(record_entry) is list else [record_entry]):
            if not resource_types or record["header"]["setSpec"] in resource_types: #Only grab what is desired
#               print(len(record))
                resource_num = record["header"]["identifier"].rsplit("/", 1)[1] #identifier is `URL/id_num`
                with open(os.path.join(out_dir, resource_num + "_metadata.json"), 'w') as out_file:
                    dump(record, out_file)


if __name__ == "__main__":
    nist_mrr_url = "http://matsci.registry.nationaldataservice.org/oai_pmh/server/"
    matin_url = "https://matin.gatech.edu/oaipmh/"

    if "matin" in to_harvest:
        oai_pmh_harvest(out_dir=paths.datasets + "matin_metadata", base_url=matin_url, metadata_prefixes=["oai_dc"], existing_dir=1, verbose=True)
    if "nist_mrr" in to_harvest:
        oai_pmh_harvest(out_dir=paths.datasets + "nist_mrr_metadata", base_url=nist_mrr_url, metadata_prefixes=["oai_dc", "oai_all", "oai_software", "oai_service", "oai_org"], existing_dir=1, verbose=True)\




