import json
import sys
import os
import multiprocessing
from queue import Empty
from time import sleep

from tqdm import tqdm

from mdf_refinery.validator import Validator

NUM_PROCESSORS = 2
#NUM_WRITERS = 1

# VERSION 0.3.0

# This is the converter for the OQMD, using multiple processes for speed.
# Arguments:
#   input_path (string): The file or directory where the data resides.
#       NOTE: Do not hard-code the path to the data in the converter. The converter should be portable.
#   metadata (string or dict): The path to the JSON dataset metadata file, a dict or json.dumps string containing the dataset metadata, or None to specify the metadata here. Default None.
#   verbose (bool): Should the script print status messages to standard output? Default False.
#       NOTE: The converter should have NO output if verbose is False, unless there is an error.
def convert(input_path, metadata=None, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    if not metadata:
        dataset_metadata = {
        "mdf": {
            "title": "The Open Quantum Materials Database",
            "acl": ["public"],
            "source_name": "oqmd",
            "citation": ['Saal, J. E., Kirklin, S., Aykol, M., Meredig, B., and Wolverton, C. "Materials Design and Discovery with High-Throughput Density Functional Theory: The Open Quantum Materials Database (OQMD)", JOM 65, 1501-1509 (2013). doi:10.1007/s11837-013-0755-4', 'Kirklin, S., Saal, J.E., Meredig, B., Thompson, A., Doak, J.W., Aykol, M., Rühl, S. and Wolverton, C. "The Open Quantum Materials Database (OQMD): assessing the accuracy of DFT formation energies", npj Computational Materials 1, 15010 (2015). doi:10.1038/npjcompumats.2015.10'],
            "data_contact": {

                "given_name": "Chris",
                "family_name": "Wolverton",

                "email": "oqmd.questions@gmail.com",
                "institution": "Northwestern University",
                },

            "author": [
                {
                "given_name": "Chris",
                "family_name": "Wolverton",
                "institution": "Northwestern University"
                },
                {
                "given_name": "Scott",
                "family_name": "Kirklin",
                "institution": "Northwestern University"
                },
                {
                "given_name": "Vinay",
                "family_name": "Hegde",
                "institution": "Northwestern University"
                },
                {
                "given_name": "Logan",
                "family_name": "Ward",
                "institution": "Northwestern University",
                "orcid": "https://orcid.org/0000-0002-1323-5939"
                }
                ],

#            "license": ,

            "collection": "OQMD",
            "tags": ["dft"],

            "description": "The OQMD is a database of DFT-calculated thermodynamic and structural properties.",
            "year": 2013,

            "links": {

                "landing_page": "http://oqmd.org/",

                "publication": ["http://dx.doi.org/10.1007/s11837-013-0755-4", "http://dx.doi.org/10.1038/npjcompumats.2015.10"],
#                "dataset_doi": ,

#                "related_id": ,

                # data links: {

                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #}
                },

#            "mrr": ,

            "data_contributor": [{
                "given_name": "Jonathon",
                "family_name": "Gaff",
                "email": "jgaff@uchicago.edu",
                "institution": "The University of Chicago",
                "github": "jgaff"
                }]
            }
        }
    elif type(metadata) is str:
        try:
            dataset_metadata = json.loads(metadata)
        except Exception:
            try:
                with open(metadata, 'r') as metadata_file:
                    dataset_metadata = json.load(metadata_file)
            except Exception as e:
                sys.exit("Error: Unable to read metadata: " + repr(e))
    elif type(metadata) is dict:
        dataset_metadata = metadata
    else:
        sys.exit("Error: Invalid metadata parameter")


    dataset_validator = Validator(dataset_metadata)


    # Get the data
    with open(os.path.join(input_path, "metadata_lookup.csv"), 'r') as lookup_file:
        # Discard header line
        lookup_file.readline()
        lookup = {}
        for line in tqdm(lookup_file, desc="Preprocessing data", disable= not verbose):
            pair = line.split(",")
            if len(pair) > 1:
                lookup[pair[0]] = pair[1]

    # Set up multiprocessing
    md_files = multiprocessing.JoinableQueue()
    rc_out = multiprocessing.JoinableQueue()
    counter = multiprocessing.Value('i', 0)
    killswitch = multiprocessing.Value('i', 0)
    # Process to add data into input queue
    adder = multiprocessing.Process(target=(lambda in_path: [ md_files.put(os.path.join(in_path, "metadata-files", f)) for f in os.listdir(os.path.join(in_path, "metadata-files")) ]), args=(input_path,))
    # Processes to process records from input queue to output queue
    processors = [multiprocessing.Process(target=process_oqmd, args=(md_files, rc_out, lookup, killswitch)) for i in range(NUM_PROCESSORS)]
    # Processes to write data from output queue
#    writers = [multiprocessing.Process(target=do_validation, args=(rc_out, dataset_validator, counter, killswitch)) for i in range(NUM_WRITERS)]
    w = multiprocessing.Process(target=do_validation, args=(rc_out, dataset_validator, counter, killswitch))
    # Process to manage progress bar
    prog_bar = multiprocessing.Process(target=track_progress, args=(len(os.listdir(os.path.join(input_path, "metadata-files"))), counter, killswitch))

    # Start queue adder, start processors when queue has some data
    adder.start()
    while md_files.empty():
        sleep(1)
    [p.start() for p in processors]
#    [w.start() for w in writers]
    w.start()
    prog_bar.start()

    adder.join()
    md_files.join()
    rc_out.join()
    killswitch.value = 1
    [p.join() for p in processors]
#    [w.join() for w in writers]
    w.join()
    prog_bar.join()

    if verbose:
        print("Finished converting")


# Function to deal with updating tqdm in a multiprocess env
def track_progress(total, counter, killswitch):
    with tqdm(total=total) as prog:
        old_counter = 0
        while killswitch.value == 0:
            new_counter = counter.value
            prog.update(new_counter - old_counter)
            old_counter = new_counter

# Write out results from processing into thread-unsafe validator
def do_validation(q_metadata, dataset_validator, counter, killswitch):
    while killswitch.value == 0:
        try:
            record = q_metadata.get(timeout=10)
#            with counter.get_lock():
            result = dataset_validator.write_record(record)
            counter.value += 1
            if result["success"] is not True:
                print("Error:", result["message"])
            q_metadata.task_done()
        except Empty:
            pass


# Record processing, ready for multiprocessing
def process_oqmd(q_paths, q_metadata, lookup, killswitch):
    while killswitch.value == 0:
        try:
            full_path = q_paths.get(timeout=10)
        except Empty:
            continue
        filename = os.path.basename(full_path)
        if os.path.isfile(full_path):
            with open(full_path, 'r') as data_file:
                record = json.load(data_file)
            metadata_path = lookup.get(filename.split(".")[0], "None").replace("//", "/").strip()
            outcar_path = os.path.join(os.path.dirname(metadata_path), "OUTCAR.gz") if metadata_path != "None" else "Unavailable"

            record_metadata = {
            "mdf": {
                "title": "OQMD - " + record["composition"],
                "acl": ["public"],
                "composition": record["composition"],
                "links": {
                    "landing_page": record["url"],
                    "metadata": {

                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",

                        "path": metadata_path,
                        },
                    "outcar": {
                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",

                        "path": outcar_path,
                        }
                    },

    #                "citation": ,
    #                "data_contact": {

    #                    "given_name": ,
    #                    "family_name": ,

    #                    "email": ,
    #                    "institution":,

                    # IDs
    #                },

    #                "author": ,

    #                "license": ,
    #                "collection": ,
    #                "data_format": ,
    #                "data_type": ,
    #                "year": ,

    #                "mrr":

    #            "processing": ,
    #            "structure":,
                },
                "oqmd": {
                    "band_gap": record["band_gap"],
                    "delta_e": record.get("stability_data", {}).get("formation_enthalpy", {}),
                    "volume": record["volume_pa"],
                    "stability": record.get("stability_data", {}).get("stability", {}),
                    "spacegroup": record.get("spacegroup", None),
                    "configuration": record["configuration"],
                    "converged": record["converged"],
                    "crossreference": record["entry"]["crossreference"],
                    "magnetic_moment": record.get("magnetic_moment", None),
                    "total_energy": record.get("total_energy", None)
                }
            }

            q_metadata.put(record_metadata)
            q_paths.task_done()

