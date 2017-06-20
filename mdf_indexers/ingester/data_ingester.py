import sys
import json
import os

from tqdm import tqdm

from . import ingest_client
from ..utils import paths
from ..utils.gmeta_utils import format_gmeta, add_namespace

PATH_FEEDSTOCK = paths.get_path(__file__, "feedstock")
globus_url = "https://search.api.globus.org/"


def ingest(mdf_source_names, globus_index, batch_size=100, verbose=False):
    ''' Ingests feedstock from file.
        Arguments:
            mdf_source_names (str or list of str): Dataset name(s) to ingest.
            batch_size (int): Max size of a single ingest operation. -1 for unlimited. Default 100.
            verbose (bool): Print status messages? Default False.
        '''
    if verbose:
        print("\nStarting ingest of:\n", mdf_source_names, "\nBatch size:", batch_size, "\n")

    globus_client = ingest_client.IngestClient(globus_url, globus_index)

    if type(mdf_source_names) is str:
        mdf_source_names = [mdf_source_names]

    for source_name in mdf_source_names:
        list_ingestables = []
        count_ingestables = 0
        count_batches = 0
        with open(os.path.join(PATH_FEEDSTOCK, source_name+"_all.json"), 'r') as feedstock:
            for json_record in tqdm(feedstock, desc="Ingesting " + source_name, disable= not verbose):
                record = format_gmeta(json.loads(json_record))
                record["content"] = add_namespace(record["content"])
                list_ingestables.append(record)
                count_ingestables += 1

                if batch_size > 0 and len(list_ingestables) >= batch_size:
                    globus_client.ingest(format_gmeta(list_ingestables))
                    list_ingestables.clear()
                    count_batches += 1

        # Check for partial batch to ingest
        if list_ingestables:
            globus_client.ingest(format_gmeta(list_ingestables))
            list_ingestables.clear()
            count_batches += 1

        if verbose:
            print("Ingested", count_ingestables, "records in", count_batches, "batches, from", source_name, "\n")
