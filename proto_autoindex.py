import os
import sys
from importlib import import_module


def autoindex(dataset_metadata, data_path, converter_dir="mdf_indexers/converters", default_converter="metadata_only", verbose=False):
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
        converter = import_module(converter_path.replace("..", ".").replace("/", ".")

    if verbose:
        print("Converting dataset")

    converter.convert(data_path, metadata=metadata, verbose=verbose)

    if verbose:
        print("Dataset converted")





