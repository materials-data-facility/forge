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

    if "all" in mdf_source_names:
        mdf_source_names = [feed.replace("_all.json", "") for feed in os.listdir(PATH_FEEDSTOCK) if feed.endswith("_all.json")]

    if verbose:
        print("\nStarting ingest of:\n", mdf_source_names, "\nIndex:", globus_index, "\nBatch size:", batch_size, "\n")

    ingest_client = confidential_login(credentials=os.path.join(PATH_CREDENTIALS, "ingester_login.json"))["search_ingest"]

    if type(mdf_source_names) is str:
        mdf_source_names = [mdf_source_names]

    # Set up multiprocessing
    ingest_queue = multiprocessing.JoinableQueue()
    counter = multiprocessing.Value('i', 0)
    killswitch = multiprocessing.Value('i', 0)
    # One reader per feedstock to ingest
    readers = [multiprocessing.Process(target=queue_ingests, args=(ingest_queue, sn, batch_size)) for sn in mdf_source_names]
    # As many submitters as you want
    submitters = [multiprocessing.Process(target=process_ingests, args=(ingest_queue, ingest_client, counter, killswitch)) for i in range(NUM_SUBMITTERS)]
    prog_bar = multiprocessing.Process(target=track_progress, args=(counter, killswitch))
    [r.start() for r in readers]
    [s.start() for s in submitters]
    if verbose:
        prog_bar.start()

    [r.join() for r in readers]
    ingest_queue.join()
    killswitch.value = 1
    [s.join() for s in submitters]
    if prog_bar.is_alive():
        prog_bar.join()

    if verbose:
        print("Ingesting complete")


def queue_ingests(ingest_queue, source_name, batch_size):
    list_ingestables = []
    with open(os.path.join(PATH_FEEDSTOCK, source_name+"_all.json"), 'r') as feedstock:
        for json_record in feedstock:
            record = format_gmeta(json.loads(json_record))
            list_ingestables.append(record)

            if batch_size > 0 and len(list_ingestables) >= batch_size:
                ingest_queue.put(format_gmeta(list_ingestables))
                list_ingestables.clear()

    # Check for partial batch to ingest
    if list_ingestables:
        ingest_queue.put(format_gmeta(list_ingestables))
        list_ingestables.clear()


def process_ingests(ingest_queue, ingest_client, counter, killswitch):
    while killswitch.value == 0:
        try:
            ingestable = ingest_queue.get(timeout=10)
        except Empty:
            continue
        try:
            ingest_client.ingest(ingestable)
        except GlobusAPIError as e:
            print("\nA Globus API Error has occurred. Details:\n", e.raw_json, "\n")
            return
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

