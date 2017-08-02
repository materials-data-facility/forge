import sys
import json
import os

from tqdm import tqdm

from mdf_forge.toolbox import format_gmeta, confidential_login
from mdf_refinery.config import PATH_FEEDSTOCK, PATH_CREDENTIALS


def ingest(mdf_source_names, globus_index, batch_size=100, verbose=False):
    ''' Ingests feedstock from file.
        Arguments:
            mdf_source_names (str or list of str): Dataset name(s) to ingest.
                Special value "all" will ingest all feedstock in the feedstock directory.
            batch_size (int): Max size of a single ingest operation. -1 for unlimited. Default 100.
            verbose (bool): Print status messages? Default False.
        '''

    if "all" in mdf_source_names:
        mdf_source_names = [feed.replace("_all.json", "") for feed in os.listdir(PATH_FEEDSTOCK) if feed.endswith("_all.json")]

    if verbose:
        print("\nStarting ingest of:\n", mdf_source_names, "\nIndex:", globus_index, "\nBatch size:", batch_size, "\n")

    ingest_client = confidential_login(credentials=os.path.join(PATH_CREDENTIALS, "ingester_login.json"))["search_ingest"]

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
                    ingest_client.ingest(format_gmeta(list_ingestables))                           
                    list_ingestables.clear()
                    count_batches += 1

        # Check for partial batch to ingest
        if list_ingestables:
            ingest_client.ingest(format_gmeta(list_ingestables))
            list_ingestables.clear()
            count_batches += 1

        if verbose:
            print("Ingested", count_ingestables, "records in", count_batches, "batches, from", source_name, "\n")
