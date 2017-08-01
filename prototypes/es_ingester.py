import sys
import json
import os
import importlib

from tqdm import tqdm
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError
from elasticsearch import helpers

from mdf_forge.toolbox import format_gmeta
from mdf_refinery.config import get_path

PATH_FEEDSTOCK = get_path("feedstock")


def es_ingest(mdf_source_names, batch_size=100, delete_index=False, verbose=False):
    client = Elasticsearch(timeout=120)
    try:
        client.indices.create(index="mdf")
    except RequestError:
        if delete_index:
            client.indices.delete(index="mdf")
            client.indices.create(index="mdf")

    if "all" in mdf_source_names:
        mdf_source_names = [feed.replace("_all.json", "") for feed in os.listdir(PATH_FEEDSTOCK) if feed.endswith("_all.json")]

    if verbose:
        print("\nStarting ingest of:\n", mdf_source_names, "\nBatch size:", batch_size, "\n")

    if type(mdf_source_names) is str:
        mdf_source_names = [mdf_source_names]

    for source_name in mdf_source_names:
        list_ingestables = []
        count_ingestables = 0
        count_batches = 0
        with open(os.path.join(PATH_FEEDSTOCK, source_name+"_all.json"), 'r') as feedstock:
            for json_record in tqdm(feedstock, desc="Ingesting " + source_name, disable= not verbose):
                record = format_gmeta(json.loads(json_record))
                list_ingestables.append(record)
                count_ingestables += 1

                if batch_size > 0 and len(list_ingestables) >= batch_size:
                    es_client_ingest(client, format_gmeta(list_ingestables))
                    list_ingestables.clear()
                    count_batches += 1

        # Check for partial batch to ingest
        if list_ingestables:
            es_client_ingest(client, format_gmeta(list_ingestables))
            list_ingestables.clear()
            count_batches += 1

        if verbose:
            print("Ingested", count_ingestables, "records in", count_batches, "batches, from", source_name, "\n")


def es_client_ingest(client, data):
#    print(data, flush=True)
#    raise Exception
    ingest = ({
        "_index": "mdf",
        "_type" : "record",
#        "_id": entry['content']['mdf_id'],
        "_id": entry['content']['mdf']['mdf-id'],
        "_source": entry['content'],
        } for entry in data["ingest_data"]["gmeta"])
    res = helpers.bulk(client, ingest)
    return res


if __name__ == "__main__":
    sys.argv.pop(0)
    if  "--delete-index" in sys.argv:
        delete_index = True
        sys.argv.remove("--delete-index")
    else:
        delete_index=False
    if sys.argv[0] == "--batch-size":
        sys.argv.pop(0)
        batch_size = int(sys.argv.pop(0))
    else:
        batch_size = 100

    es_ingest(sys.argv, batch_size=batch_size, delete_index=delete_index, verbose=True)

