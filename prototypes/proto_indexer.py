import os
import sys
import json
import argparse
import random
from importlib import import_module

DEFAULT_GEN_CONV = "metadata_only"


def autoindex(dataset_metadata, data_path, submit_feedstock=True, review_feedstock=True, default_converter=DEFAULT_GEN_CONV, verbose=False):
    converter_dir = os.path.join("mdf_indexers", "converters")
    feedstock_dir = os.path.join("mdf_indexers", "feedstock")
    submitter_path = os.path.join("mdf_indexers", "submitter", "feedstock_submitter")
    if verbose:
        print("Indexing dataset")
    if type(dataset_metadata) is dict:
        metadata = dataset_metadata
    elif type(dataset_metadata) is str:
        try:
            metadata = json.loads(dataset_metadata)
        except Exception:
            try:
                with open(dataset_metadata, 'r') as metadata_file:
                    metadata = json.load(metadata_file)
            except Exception as e:
                sys.exit("Error: Unable to read metadata: " + repr(e))
    else:
        sys.exit("Error: Invalid metadata parameter")

    # Autodetect data type if necessary and able
    if not metadata.get("mdf-data_type", None):
        data_type = detect_data_type(data_path)
        if data_type:
            metadata["mdf-data_type"] = data_type
        elif verbose:
            print("Unable to detect data type")

    # Gather list of available converters
    all_converters = [c.replace("_converter.py", "") for c in os.listdir(converter_dir) if c.endswith("_converter.py")]
    # Choose converter
    # mdf-source_name
    if metadata.get("mdf-source_name", "") in all_converters:
        selected_converter = metadata.get("mdf-source_name", "")
    # mdf-data_type
    elif metadata.get("mdf-data_type", "") in all_converters:
        selected_converter = metadata.get("mdf-data_type", "")
    else:
        selected_converter = default_converter

    if verbose:
        print("Using converter '", selected_converter, "'", sep="")

    # Call converter
    # If converter_dir is absolute path, add to sys.path
    if converter_dir.startswith("/"):
        sys.path.append(converter_dir)
        converter = import_module(selected_converter + "_converter")
    # If converter_dir is relative path, use import_module directly
    else:
        converter_path = os.path.join(converter_dir, selected_converter + "_converter").strip(".")
        # Transform: '../part/of/path' => '..part.of.path'
        converter = import_module(converter_path.replace("..", ".").replace(os.sep, "."))

    if verbose:
        print("Converting dataset")

    converter.convert(data_path, metadata=metadata, verbose=verbose)

    if verbose:
        print("Dataset converted")

    feedstock_path = os.path.join(feedstock_dir, metadata.get("mdf-source_name", "") + "_all.json")

    # If user should review feedstock, print dataset entry and random record entry, then require signoff
    if review_feedstock:
        with open(feedstock_path) as feedstock:
            num_records = len(feedstock.readlines()) - 1  # -1 for dataset entry
            feedstock.seek(0)
            ds_entry = json.loads(feedstock.readline())
            if num_records:  # If num_records is 0, cannot print any records
                # Discard random number of record entries
                [feedstock.readline() for i in range(random.randint(0, num_records-1))]
                # Save next record entry to display
                rc_entry = json.loads(feedstock.readline())
            else:
                rc_entry = None

        # Prompt user
        print("Please review the following feedstock to ensure that the dataset was converted correctly:")
        print("*"*50)
        print("DATASET entry:")
        print(json.dumps(ds_entry, sort_keys=True, indent=4, separators=(',', ': ')))
        if rc_entry:
            print("*"*50)
            print("\nRECORD entry:")
            print(json.dumps(rc_entry, sort_keys=True, indent=4, separators=(',', ': ')))
        print("Please review the above feedstock to ensure that the dataset was converted correctly.")

        correct = input("Is the feedstock correct?\n")
        while not correct.strip():
            print("Please enter a response.")
            correct = input("Is the feedstock correct?\n")
        yes_inputs = ["y", "yes", "true"]
        if correct.strip().lower() not in yes_inputs:
            print("The feedstock will not be submitted. Please correct any errors and try again, or contact MDF for help.")
            submit_feedstock = False
        print("Thank you for reviewing the feedstock.\n")

    # If submission should proceed, call submitter
    if submit_feedstock:
        if verbose:
            print("Submitting feedstock to MDF")
        submitter = import_module(submitter_path.replace("..", ".").replace(os.sep, "."))
        submitter.submit_feedstock(feedstock_path, verbose)
        if verbose:
            print("Feedstock submitted to MDF")

    if verbose:
        print("Finished processing dataset '", metadata.get("mdf-source_name", ""), "'", sep="")



def cli():
    parser = argparse.ArgumentParser(description="This tool will automatically index a dataset, given the metadata and data location, and submit it to MDF.")
    parser.add_argument("-m", "--md", "--metadata-path", help="The path to the metadata file.", required=True)
    parser.add_argument("-d", "--data-path", help="The path to the data.", required=True)
    parser.add_argument("-f", "--no-submit", "--feedstock-only", "--local", action="store_true", help="Do not submit the feedstock to MDF; only save it locally.")
    parser.add_argument("-s", "--no-review", "--no-check", action="store_true", help="Do not ask for a review of the feedstock. CAUTION: If --no-submit is not provided, this option will submit whatever feedstock is produced to MDF without further user input.")
    parser.add_argument("--default-converter", help="The converter to use if no specific alternative is found.", default=DEFAULT_GEN_CONV)
    parser.add_argument("-v", "--verbose", action="store_true", help="Print status messages.")
    args = parser.parse_args()

    autoindex(dataset_metadata=args.md, data_path=args.data_path, submit_feedstock=not args.no_submit, review_feedstock=not args.no_review, default_converter=args.default_converter, verbose=args.verbose)


if __name__ == "__main__":
    cli()


