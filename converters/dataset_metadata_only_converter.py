import json
import sys
import os
from tqdm import tqdm
from parsers.utils import find_files
from validator import Validator
import paths

# VERSION 0.1.0

# This is the converter for datasets that cannot be meaningfully deeply indexed.
# Arguments:
#   metadata (string or dict): The path to the JSON dataset metadata file, or a dict or json.dumps string containing the dataset metadata.
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
            dataset_metadata = json.loads(metadata)
        except Exception:
            try:
                with open(metadata, 'r') as metadata_file:
                    dataset_metadata = json.load(metadata_file)
            except FileNotFoundError:
                try:
                    with open(os.path.join(paths.datasets, "metadata_only", metadata), 'r') as metadata_file:
                        dataset_metadata = json.load(metadata_file)
                except Exception as e:
                    sys.exit("Error: Unable to read metadata: " + repr(e))
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
        print("--all: If this is the first argument, all .json files in " + os.path.join(paths.datasets, "metadata_only") + " will be converted.")
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
    elif sys.argv[1] == "--all":
        for md_file in tqdm(find_files(os.path.join(paths.datasets, "metadata_only"), ".json"), desc="Processing metadata"):
            convert(os.path.join(md_file["path"], md_file["filename"]))
    else:
        convert(sys.argv[1], verbose=True)
