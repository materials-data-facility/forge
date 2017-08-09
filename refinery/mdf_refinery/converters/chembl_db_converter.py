import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

#Multiprocessing imports
import warnings
import multiprocessing
from queue import Empty
from time import sleep

NUM_PROCESSORS = 4
#NUM_WRITERS = 1

# VERSION 0.3.0
# This is the converter for: ChEMBL Database, running on multiprocessing for speed
# Arguments:
#   input_path (string): The file or directory where the data resides.
#       NOTE: Do not hard-code the path to the data in the converter (the filename can be hard-coded, though). The converter should be portable.
#   metadata (string or dict): The path to the JSON dataset metadata file, a dict or json.dumps string containing the dataset metadata, or None to specify the metadata here. Default None.
#   verbose (bool): Should the script print status messages to standard output? Default False.
#       NOTE: The converter should have NO output if verbose is False, unless there is an error.
def convert(input_path, metadata=None, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    # NOTE: For fields that represent people (e.g. mdf-data_contact), other IDs can be added (ex. "github": "jgaff").
    #    It is recommended that all people listed in mdf-data_contributor have a github username listed.
    #
    # If there are other useful fields not covered here, another block (dictionary at the same level as "mdf") can be created for those fields.
    # The block must be called the same thing as the source_name for the dataset.
    if not metadata:
        ## Metadata:dataset
        dataset_metadata = {
            "mdf": {

                "title": "ChEMBL Database",
                "acl": ["public"],
                "source_name": "chembl_db",

                "data_contact": {

                    "given_name": "John P.",
                    "family_name": "Overington",
                    "email": "jpo@ebi.ac.uk",
                    "institution": "European Molecular Biology Laboratory European Bioinformatics Institute",

                },

                "data_contributor": [{

                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78",

                }],

                "citation": ["A.P. Bento, A. Gaulton, A. Hersey, L.J. Bellis, J. Chambers, M. Davies, F.A. Krüger, Y. Light, L. Mak, S. McGlinchey, M. Nowotka, G. Papadatos, R. Santos and J.P. Overington (2014) 'The ChEMBL bioactivity database: an update.' Nucleic Acids Res., 42 1083-1090. DOI: 10.1093/nar/gkt1031 PMID: 24214965", "M. Davies, M. Nowotka, G. Papadatos, F. Atkinson, G.J.P. van Westen, N Dedman, R. Ochoa and J.P. Overington  (2014) 'myChEMBL: A Virtual Platform for Distributing Cheminformatics Tools and Open Data' Challenges 5 (334-337) DOI: 10.3390/challe5020334", "S. Jupp, J. Malone, J. Bolleman, M. Brandizi, M. Davies, L. Garcia, A. Gaulton, S. Gehant, C. Laibe, N. Redaschi, S.M Wimalaratne, M. Martin, N. Le Novère, H. Parkinson, E. Birney and A.M Jenkinson (2014) The EBI RDF Platform: Linked Open Data for the Life Sciences Bioinformatics 30 1338-1339 DOI: 10.1093/bioinformatics/btt765 PMID: 24413672"],

                #"author": [],

                "license": "https://creativecommons.org/licenses/by-sa/3.0/",
                "collection": "ChEMBL db",
                "tags": ["SAR"],
                "description": "ChEMBL is a database of bioactive drug-like small molecules, it contains 2-D structures, calculated properties (e.g. logP, Molecular Weight, Lipinski Parameters, etc.) and abstracted bioactivities (e.g. binding constants, pharmacology and ADMET data).vThe data is abstracted and curated from the primary scientific literature, and cover a significant fraction of the SAR and discovery of modern drugs We attempt to normalise the bioactivities into a uniform set of end-points and units where possible, and also to tag the links between a molecular target and a published assay with a set of varying confidence levels. Additional data on clinical progress of compounds is being integrated into ChEMBL at the current time.",
                "year": 2017,

                "links": {

                    "landing_page": "https://www.ebi.ac.uk/chembl/downloads",
                    #"publication": [""],
                    "data_doi": "ftp://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/",
                    #"related_id": "",

                    #"data_link": {

                        #"globus_endpoint": ,
                        #"http_host": "",

                        #"path": "",

                    #},

                },

            },

            #"mrr": {

            #},

            #"dc": {

            #},


        }
        ## End metadata
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



    # Make a Validator to help write the feedstock
    # You must pass the metadata to the constructor
    # Each Validator instance can only be used for a single dataset
    # If the metadata is incorrect, the constructor will throw an exception and the program will exit
    dataset_validator = Validator(dataset_metadata)


    # Get the data
    #    Each record should be exactly one dictionary
    #    You must write your records using the Validator one at a time
    #    It is recommended that you use a parser to help with this process if one is available for your datatype
    #    Each record also needs its own metadata



# Set up multiprocessing
    md_files = multiprocessing.JoinableQueue()
    rc_out = multiprocessing.JoinableQueue()
    counter = multiprocessing.Value('i', 0)
    err_counter = multiprocessing.Value('i', 0)
    killswitch = multiprocessing.Value('i', 0)
    # Find all the sdf files
    sdf_list = [ os.path.join(sdf["path"], sdf["filename"]) for sdf in tqdm(find_files(input_path, "sdf$"), desc="Finding files", disable= not verbose) ]
    # Process to add data into queue
    adder = multiprocessing.Process(target=(lambda sdf_list: [ md_files.put(sdf) for sdf in sdf_list ]), args=(sdf_list,))
    # Processes to process records from input queue to output queue
    processors = [multiprocessing.Process(target=process_chembl_db, args=(md_files, rc_out, err_counter, killswitch)) for i in range(NUM_PROCESSORS)]
    # Processes to write data from output queue
#    writers = [multiprocessing.Process(target=do_validation, args=(rc_out, dataset_validator, counter, killswitch)) for i in range(NUM_WRITERS)]
    w = multiprocessing.Process(target=do_validation, args=(rc_out, dataset_validator, counter, killswitch))
    # Process to manage progress bar
    prog_bar = multiprocessing.Process(target=track_progress, args=(len(sdf_list), counter, err_counter, killswitch))

    # Start adder
    adder.start()
    # Start processors, writers, and progress bar after data is in queue
    while md_files.empty():
       sleep(1) 
    [p.start() for p in processors]
#    [w.start() for w in writers]
    w.start()
    if verbose:
        prog_bar.start()

    # Wait on adder to finish
    adder.join()
    #print("ADDER JOINED")
    # Wait on both queues to be complete
    md_files.join()
    #print("MD_FILES JOINED")
    rc_out.join()
    #print("RC_OUT JOINED")
    # Trigger remote termination of processes without purpose
    killswitch.value = 1
    #print("KILLSWITCH AT 1")
    # Wait on all the processes to terminate
    [p.join() for p in processors]
    #print("PROCESSORS JOINED")
#    [w.join() for w in writers]
    w.join()
    #print("W JOINED")
    if prog_bar.is_alive():
        prog_bar.join()

    if verbose:
        print("Finished converting")
        print("There were", err_counter.value, "errors")


# Function to deal with updating tqdm in a multiprocess env
def track_progress(total, counter, err_counter, killswitch):
    with tqdm(total=total, desc="Processing files") as prog:
        old_counter = 0
        while killswitch.value == 0:
            # Update tqdm with difference in all counters
            new_counter = counter.value + err_counter.value
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
    dataset_validator.flush()




# Process records in parallel
def process_chembl_db(in_q, out_q, err_counter, killswitch):
    while killswitch.value == 0:
        try:
            full_path = in_q.get(timeout=10)
        except Empty:
            continue
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                sdf = parse_ase(full_path, data_format="sdf", verbose=False)
        except Exception as e:
            with err_counter.get_lock():
                err_counter.value += 1
            #in_q.task_done()
            #continue
        ## Metadata:record
        end_path = full_path.split("datasets/chembl_db/")[-1]
        record_metadata = {
            "mdf": {

                "title": "ChEMBL db - " + sdf["chemical_formula"],
                "acl": ["public"],
                "composition": sdf["chemical_formula"],

                #"tags": ,
                #"description": ,
                #"raw": ,

                "links": {

                    #"landing_page": ,
                    #"publication": ,
                    #"data_doi": ,
                    #"related_id": ,

                    "sdf": {

                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",

                        "path": "/collections/chembl_db/" + end_path,

                    },

                },

                #"citation": ,

                #"data_contact": {

                    #"given_name": ,
                    #"family_name": ,
                    #"email": ,
                    #"institution": ,

                #},

                #"author": [{

                    #"given_name": ,
                    #"family_name": ,
                    #"email": ,
                    #"institution": ,

                #}],

                #"year": ,

            },

            #"dc": {

            #},


        }
        ## End metadata

        out_q.put(record_metadata)
        in_q.task_done()
