import os
import sys
import json
import argparse
import random
from importlib import import_module

DEFAULT_GEN_CONV = "metadata_only"


def autoindex(dataset_metadata, data_path, submit_feedstock=True, review_feedstock=True, default_converter=DEFAULT_GEN_CONV, verbose=False):
    converter_dir = "mdf_indexers/converters"
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
        converter = import_module(converter_path.replace("..", ".").replace("/", "."))

    if verbose:
        print("Converting dataset")

    converter.convert(data_path, metadata=metadata, verbose=verbose)

    if verbose:
        print("Dataset converted")

    feedstock_path = 

    if review_feedstock:
        print("Please review the following to ensure that the dataset was converted correctly:")
        


def cli():
    parser = argparse.ArgumentParser(description="This tool will automatically index a dataset, given the metadata and data location, and submit it to MDF.")
    parser.add_argument("-m", "--md", "--metadata-path", help="The path to the metadata file", required=True)
    parser.add_argument("-d", "--data-path", help="The path to the data", required=True)
    submit_group = parser.add_mutually_exclusive_group()
    submit_group.add_argument("-f", "--no-submit", "--feedstock-only", "--local", action="store_true", help="Do not submit the feedstock to MDF; only save it locally")
    submit_group.add_argument("-s", "--auto-submit", "--autosubmit", "--no-check", "--no-review", action="store_true", help="Submit the feedstock to MDF without reviewing it first")
    parser.add_argument("--default-converter", help="The converter to use if no specific alternative is found", default=DEFAULT_GEN_CONV)
    parser.add_argument("-v", "--verbose", action="store_true", help="Print status messages")
    args = parser.parse_args()

    autoindex(dataset_metadata=parser.md, data_path=parser.data_path, submit_feedstock=not parser.no_submit, review_feedstock=not parser.auto_submit, default_converter=parser.default_converter, verbose=parser.verbose)


if __name__ == "__main__":
    cli()


