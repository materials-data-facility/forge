import requests
import json
import xmltodict
import os
import os.path
from tqdm import tqdm

from mdf_refinery.config import PATH_FEEDSTOCK

url = "http://mrr.materialsdatafacility.org/oai_pmh/server/"
prefixes = [
    "oai_database",
    "oai_datacol",
    "oai_dataset"
    ]

# Harvest MDF MRR OAI-PMH
def harvest_proto(out_dir=PATH_FEEDSTOCK, base_url=url, metadata_prefixes=prefixes, resource_types=[], verbose=False):
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

    count = 0
    with open(os.path.join(out_dir, "mdf_mrr_all.json"), 'w') as feed_file:
        for record_entry in tqdm(records, desc="Fetching records", disable= not verbose):
            for record in (record_entry if type(record_entry) is list else [record_entry]):
                if not resource_types or record["header"]["setSpec"] in resource_types: #Only grab what is desired
#                  print(len(record))
#                   resource_num = record["header"]["identifier"].rsplit("/", 1)[1] #identifier is `URL/id_num`
                    # Add mdf data
                    record["mdf"] = {
                        "acl": ["public"],
                        "source_name": "mdf_mrr",
                        "links": {
                            "landing_page": "http://mrr.materialsdatafacility.org/#" + str(count)
                            }
                        }
                    json.dump(record, feed_file)
                    feed_file.write("\n")
                    count += 1

    if verbose:
        print("Finished")


if __name__ == "__main__":
    harvest_proto(verbose=True)


