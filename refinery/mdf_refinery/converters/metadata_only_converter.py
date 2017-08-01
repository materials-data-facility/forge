import json
import sys

from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for datasets that cannot be meaningfully deeply indexed.
# Arguments:
#   metadata (string or dict): The path to the JSON dataset metadata file, a dict or json.dumps string containing the dataset metadata.
#   verbose (bool): Should the script print status messages to standard output? Default False.
#       NOTE: The converter should have NO output if verbose is False, unless there is an error.
def convert(metadata, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    if type(metadata) is str:
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

    Validator(dataset_metadata)

    if verbose:
        print("Finished converting")
