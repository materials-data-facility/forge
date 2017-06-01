import json
import sys
from validator import Validator

# VERSION 0.1.0

# This is the converter for datasets that cannot be meaningfully deeply indexed. 
# Arguments:
#   metadata (string or dict): The path to the JSON dataset metadata file, a dict containing the dataset metadata, or None to specify the metadata here. Default None.
#   verbose (bool): Should the script print status messages to standard output? Default False.
#       NOTE: The converter should have NO output if verbose is False, unless there is an error.
def convert(metadata=None, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    if not metadata:
        sys.exit("Error: Dataset metadata is required.")
    elif type(metadata) is str:
        try:
            with open(metadata, 'r') as metadata_file:
                dataset_metadata = json.load(metadata_file)
        except Exception as e:
            sys.exit("Error: Unable to read metadata: " + repr(e))
    elif type(metadata) is dict:
        dataset_metadata = metadata
    else:
        sys.exit("Error: Invalid metadata parameter")

    dataset_validator = Validator(dataset_metadata, strict=True)

    if verbose:
        print("Finished converting")

if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print("ARGS:")
        print("--cmd: If this is the first argument, the rest of the arguments will be parsed as the metadata.")
        print("Otherwise, the first argument must be the name of the JSON file to read.")
    elif sys.argv[1] == "--cmd":
        metadata = {}
        key = ""
        for arg in sys.argv[2:]:
            if not key:
                key = arg.replace("--", "")
            else:
                metadata[key] = arg
                key = ""
        convert(metadata, verbose=True)
    else:
        convert(sys.argv[1], verbose=True)
