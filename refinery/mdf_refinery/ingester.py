import sys
import json
import os
import multiprocessing
from queue import Empty

from tqdm import tqdm
from globus_sdk import GlobusAPIError

from mdf_forge.toolbox import format_gmeta, confidential_login
from mdf_refinery.config import PATH_FEEDSTOCK, PATH_CREDENTIALS


NUM_SUBMITTERS = 5

def ingest(mdf_source_names, globus_index, batch_size=100, verbose=False):
    ''' Ingests feedstock from file.
        Arguments:
            mdf_source_names (str or list of str): Dataset name(s) to ingest.
                Special value "all" will ingest all feedstock in the feedstock directory.
            batch_size (int): Max size of a single ingest operation. -1 for unlimited. Default 100.
            verbose (bool): Print status messages? Default False.
        '''
    if type(mdf_source_names) is str:
        mdf_source_names = [mdf_source_names]

    if "all" in mdf_source_names:
        mdf_source_names = [feed.replace("_all.json", "") for feed in os.listdir(PATH_FEEDSTOCK) if feed.endswith("_all.json")]

    if verbose:
        print("\nStarting ingest of:\n", mdf_source_names, "\nIndex:", globus_index, "\nBatch size:", batch_size, "\n")

    with open(os.path.join(PATH_CREDENTIALS, "ingester_login.json")) as cred_file:
        creds = json.load(cred_file)
        creds["index"] = globus_index
        ingest_client = confidential_login(credentials=creds)["search_ingest"]


    # Set up multiprocessing
    ingest_queue = multiprocessing.JoinableQueue()
    counter = multiprocessing.Value('i', 0)
    killswitch = multiprocessing.Value('i', 0)

    # One reader (can reduce performance on large datasets if multiple are submitted at once)
    reader = multiprocessing.Process(target=queue_ingests, args=(ingest_queue, mdf_source_names, batch_size))
    # As many submitters as is feasible
    submitters = [multiprocessing.Process(target=process_ingests, args=(ingest_queue, ingest_client, counter, killswitch)) for i in range(NUM_SUBMITTERS)]
    prog_bar = multiprocessing.Process(target=track_progress, args=(counter, killswitch))
    reader.start()
    [s.start() for s in submitters]
    if verbose:
        prog_bar.start()

    reader.join()
    ingest_queue.join()
    killswitch.value = 1
    [s.join() for s in submitters]
    if prog_bar.is_alive():
        prog_bar.join()

    if verbose:
        print("Ingesting complete")


def queue_ingests(ingest_queue, sources, batch_size):
    for source_name in sources:
        list_ingestables = []
        with open(os.path.join(PATH_FEEDSTOCK, source_name+"_all.json"), 'r') as feedstock:
            for json_record in feedstock:
                record = format_gmeta(json.loads(json_record))
                list_ingestables.append(record)

                if batch_size > 0 and len(list_ingestables) >= batch_size:
                    full_ingest = format_gmeta(list_ingestables)
                    ingest_queue.put(json.dumps(full_ingest))
                    list_ingestables.clear()

        # Check for partial batch to ingest
        if list_ingestables:
            full_ingest = format_gmeta(list_ingestables)
            ingest_queue.put(json.dumps(full_ingest))
            list_ingestables.clear()


def process_ingests(ingest_queue, ingest_client, counter, killswitch):
    while killswitch.value == 0:
        try:
            ingestable = json.loads(ingest_queue.get(timeout=10))
        except Empty:
            continue
        try:
            res = ingest_client.ingest(ingestable)
            if not res["success"]:
                raise ValueError("Ingest failed: " + str(res))
            elif res["num_documents_ingested"] <= 0:
                raise ValueError("No documents ingested: " + str(res))
        except GlobusAPIError as e:
            print("\nA Globus API Error has occurred. Details:\n", e.raw_json, "\n")
            continue
        with counter.get_lock():
            counter.value += 1
        ingest_queue.task_done()


def track_progress(counter, killswitch):
    with tqdm(desc="Ingesting feedstock batches") as prog:
        old_counter = 0
        while killswitch.value == 0:
            # Update tqdm with difference in all counters
            new_counter = counter.value
            prog.update(new_counter - old_counter)
            old_counter = new_counter

