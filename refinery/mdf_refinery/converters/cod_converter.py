import json
import sys
import os
import warnings
import multiprocessing
from queue import Empty
from time import sleep

from tqdm import tqdm

from mdf_refinery.validator import Validator
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase

NUM_PROCESSORS = 2
#NUM_WRITERS = 1

# VERSION 0.3.0

# This is the converter for the Crystallography Open Database
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
    # Fields can be:
    #    REQ (Required, must be present)
    #    RCM (Recommended, should be present if possible)
    #    OPT (Optional, can be present if useful)
    # NOTE: For fields that represent people (e.g. mdf-data_contact), other IDs can be added (ex. "github": "jgaff").
    #    It is recommended that all people listed in mdf-data_contributor have a github username listed.
    #
    # If there are other useful fields not covered here, another block (dictionary at the same level as "mdf") can be created for those fields.
    # The block must be called the same thing as the source_name for the dataset.
    if not metadata:
        ## Metadata:dataset
        dataset_metadata = {
            # REQ dictionary: MDF-format dataset metadata
            "mdf": {

                # REQ string: The title of the dataset
                "title": "Crystallography Open Database",

                # REQ list of strings: The UUIDs allowed to view this metadata, or 'public'
                "acl": ["public"],

                # REQ string: A short version of the dataset name, for quick reference. Spaces and dashes will be replaced with underscores, and other non-alphanumeric characters will be removed.
                "source_name": "cod",

                # REQ dictionary: The contact person/steward/custodian for the dataset
                "data_contact": {

                    # REQ string: The person's given (or first) name
                    "given_name": "Daniel",

                    # REQ string: The person's family (or last) name
                    "family_name": "Chateigner",

                    # REQ string: The person's email address
                    "email": "cod-bugs@ibt.lt",

                    # RCM string: The primary affiliation for the person
#                    "institution": ,

                },

                # REQ list of dictionaries: The person/people contributing the tools (harvester, this converter) to ingest the dataset (i.e. you)
                "data_contributor": [{

                    # REQ string: The person's given (or first) name
                    "given_name": "Jonathon",

                    # REQ string: The person's family (or last) name
                    "family_name": "Gaff",

                    # REQ string: The person's email address
                    "email": "jgaff@uchicago.edu",

                    # RCM string: The primary affiliation for the person
                    "institution": "The University of Chicago",

                    # RCM string: The person's GitHub username
                    "github": "jgaff",


                }],

                # RCM list of strings: The full bibliographic citation(s) for the dataset
                "citation": ['Merkys, A., Vaitkus, A., Butkus, J., Okulič-Kazarinas, M., Kairys, V. & Gražulis, S. (2016) "COD::CIF::Parser: an error-correcting CIF parser for the Perl language". Journal of Applied Crystallography 49.', 'Gražulis, S., Merkys, A., Vaitkus, A. & Okulič-Kazarinas, M. (2015) "Computing stoichiometric molecular composition from crystal structures". Journal of Applied Crystallography 48, 85-91.', 'Gražulis, S., Daškevič, A., Merkys, A., Chateigner, D., Lutterotti, L., Quirós, M., Serebryanaya, N. R., Moeck, P., Downs, R. T. & LeBail, A. (2012) "Crystallography Open Database (COD): an open-access collection of crystal structures and platform for world-wide collaboration". Nucleic Acids Research 40, D420-D427.', 'Grazulis, S., Chateigner, D., Downs, R. T., Yokochi, A. T., Quiros, M., Lutterotti, L., Manakova, E., Butkus, J., Moeck, P. & Le Bail, A. (2009) "Crystallography Open Database – an open-access collection of crystal structures". J. Appl. Cryst. 42, 726-729.', 'Downs, R. T. & Hall-Wallace, M. (2003) "The American Mineralogist Crystal Structure Database". American Mineralogist 88, 247-250.'],
                
                # RCM list of dictionaries: A list of the authors of this dataset
#                "author": [{

                    # REQ string: The person's given (or first) name
#                    "given_name": ,

                    # REQ string: The person's family (or last) name
#                    "family_name": ,

                    # RCM string: The person's email address
#                    "email": ,

                    # RCM string: The primary affiliation for the person
#                    "institution": ,


#                }],
                
                # RCM string: A link to the license for distribution of the dataset
                "license": "Public Domain",

                # RCM string: The collection for the dataset, commonly a portion of the title
                "collection": "COD",

                # RCM list of strings: Tags, keywords, or other general descriptors for the dataset
                "tags": ["Crystallography"],

                # RCM string: A description of the dataset
                "description": "Open-access collection of crystal structures of organic, inorganic, metal-organic compounds and minerals, excluding biopolymers.",

                # RCM integer: The year of dataset creation
                "year": 2003,

                # REQ dictionary: Links relating to the dataset
                "links": {

                    # REQ string: The human-friendly landing page for the dataset
                    "landing_page": "http://www.crystallography.net/cod/",

                    # RCM list of strings: The DOI(s) (in link form, ex. 'https://dx.doi.org/10.12345') for publications connected to the dataset
#                    "publication": ,

                    # RCM string: The DOI of the dataset itself (in link form)
#                    "data_doi": ,

                    # OPT list of strings: The mdf-id(s) of related entries, not including records from this dataset
#                    "related_id": ,

                    # RCM dictionary: Links to raw data files from the dataset (multiple allowed, field name should be data type)
#                    "data_link": {

                        # RCM string: The ID of the Globus Endpoint hosting the file
#                        "globus_endpoint": ,

                        # RCM string: The fully-qualified HTTP hostname, including protocol, but without the path (for example, 'https://data.materialsdatafacility.org')
#                        "http_host": ,

                        # REQ string: The full path to the data file on the host
#                        "path": ,

#                    },

                },

            },

            # OPT dictionary: MRR-format metadata
            "mrr": {

            },

            # OPT dictionary: DataCite-format metadata
            "dc": {

            },


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
    # Find all the cif files
    cif_list = [ os.path.join(cif["path"], cif["filename"]) for cif in tqdm(find_files(input_path, "cif$"), desc="Finding files", disable= not verbose) ]
    # Process to add data into queue
    adder = multiprocessing.Process(target=(lambda cif_list: [ md_files.put(cif) for cif in cif_list ]), args=(cif_list,))
    # Processes to process records from input queue to output queue
    processors = [multiprocessing.Process(target=process_cod, args=(md_files, rc_out, err_counter, killswitch)) for i in range(NUM_PROCESSORS)]
    # Processes to write data from output queue
#    writers = [multiprocessing.Process(target=do_validation, args=(rc_out, dataset_validator, counter, killswitch)) for i in range(NUM_WRITERS)]
    w = multiprocessing.Process(target=do_validation, args=(rc_out, dataset_validator, counter, killswitch))
    # Process to manage progress bar
    prog_bar = multiprocessing.Process(target=track_progress, args=(len(cif_list), counter, err_counter, killswitch))

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
    if verbose:
        print("Adder has completed.")
    # Wait on both queues to be complete
    md_files.join()
    if verbose:
        print("Input Queue is empty.")
    rc_out.join()
    if verbose:
        print("Output Queue is empty.")
    # Trigger remote termination of processes without purpose
    killswitch.value = 1
    if verbose:
        print("Terminating remaining processes.")
    # Wait on all the processes to terminate
    [p.join() for p in processors]
    if verbose:
        print("All processors terminated.")
#    [w.join() for w in writers]
    w.join()
    if verbose:
        print("Writer terminated")
    if prog_bar.is_alive():
        prog_bar.join()
        print("Progress bar terminated.")

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
def process_cod(in_q, out_q, err_counter, killswitch):
    while killswitch.value == 0:
        try:
            full_path = in_q.get(timeout=10)
        except Empty:
            continue
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                cif = parse_ase(full_path, data_format="cif", verbose=False)
        except Exception as e:
            with err_counter.get_lock():
                err_counter.value += 1
            in_q.task_done()
            continue
        # Fields can be:
        #    REQ (Required, must be present)
        #    RCM (Recommended, should be present if possible)
        #    OPT (Optional, can be present if useful)
        ## Metadata:record
        filename = full_path.rsplit("/", 1)[1]
        record_metadata = {
            # REQ dictionary: MDF-format record metadata
            "mdf": {

                # REQ string: The title of the record
                "title": "COD - " + cif["chemical_formula"],

                # RCM list of strings: The UUIDs allowed to view this metadata, or 'public' (defaults to the dataset ACL)
                "acl": ["public"],

                # RCM string: Subject material composition, expressed in a chemical formula (ex. Bi2S3)
                "composition": cif["chemical_formula"],

                # RCM list of strings: Tags, keywords, or other general descriptors for the record
#                "tags": ,

                # RCM string: A description of the record
#                "description": ,

                # RCM string: The record as a JSON string (see json.dumps())
#                "raw": ,

                # REQ dictionary: Links relating to the record
                "links": {

                    # RCM string: The human-friendly landing page for the record (defaults to the dataset landing page)
                    "landing_page": "http://www.crystallography.net/cod/" + filename.replace("cif", "html"),

                    # RCM list of strings: The DOI(s) (in link form, ex. 'https://dx.doi.org/10.12345') for publications specific to this record
#                    "publication": ,

                    # RCM string: The DOI of the record itself (in link form)
#                    "data_doi": ,

                    # OPT list of strings: The mdf-id(s) of related entries, not including the dataset entry
#                    "related_id": ,

                    # RCM dictionary: Links to raw data files from the dataset (multiple allowed, field name should be data type)
                    "cif": {

                        # RCM string: The ID of the Globus Endpoint hosting the file
#                        "globus_endpoint": ,

                        # RCM string: The fully-qualified HTTP hostname, including protocol, but without the path (for example, 'https://data.materialsdatafacility.org')
                        "http_host": "http://www.crystallography.net/cod",

                        # REQ string: The full path to the data file on the host
                        "path": "/cod/" + filename,

                    },

                },

                # OPT list of strings: The full bibliographic citation(s) for the record, if different from the dataset
#                "citation": ,

                # OPT dictionary: The contact person/steward/custodian for the record, if different from the dataset
#                "data_contact": {

                    # REQ string: The person's given (or first) name
#                    "given_name": ,

                    # REQ string: The person's family (or last) name
#                    "family_name": ,

                    # REQ string: The person's email address
#                    "email": ,

                    # RCM string: The primary affiliation for the person
#                    "institution": ,

#                },

                # OPT list of dictionaries: A list of the authors of this record, if different from the dataset
#                "author": [{

                    # REQ string: The person's given (or first) name
#                    "given_name": ,

                    # REQ string: The person's family (or last) name
#                    "family_name": ,

                    # RCM string: The person's email address
#                    "email": ,

                    # RCM string: The primary affiliation for the person
#                    "institution": ,


#                }],

                # OPT integer: The year of dataset creation, if different from the dataset
#                "year": ,

            },

            # OPT dictionary: DataCite-format metadata
            "dc": {

            },


        }
        ## End metadata

        out_q.put(record_metadata)
        in_q.task_done()

