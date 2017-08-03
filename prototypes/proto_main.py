import os
from importlib import import_module
from mdf_refinery.config import PATH_DATASETS

VERBOSE = True
harvesters_import = "mdf_refinery.harvesters."
converters_import = "mdf_refinery.converters."
ingester_import = "mdf_refinery.ingester"


def call_harvester(source_name, existing_dir=-1, verbose=VERBOSE, **kwargs):
    if verbose:
        print("HARVESTING", source_name)
    harvester = import_module(harvesters_import + source_name + "_harvester")
    output_path = os.path.join(PATH_DATASETS, source_name + "/")
    harvester.harvest(out_dir=output_path, existing_dir=existing_dir,  verbose=verbose, **kwargs)
    if verbose:
        print("HARVESTING COMPLETE")


def call_converter(sources, input_path=None, metadata=None, verbose=VERBOSE):
    if type(sources) is not list:
        sources = [sources]
    if verbose:
        print("CONVERTING THE FOLLOWING DATASETS:", sources)
    for source_name in sources:
        if verbose:
            print("\nCONVERTER FOR", source_name, "\n")
        converter = import_module(converters_import + source_name + "_converter")
        if not input_path:
            # Relative path is from calling function, not sub-function: paths.datasets will be wrong
            # Use "mdf_refinery/datasets/X" instead
            input_path = os.path.join(PATH_DATASETS, source_name + "/")
        converter.convert(input_path=input_path, metadata=metadata, verbose=verbose)
    if verbose:
        print("\nALL CONVERTING COMPLETE")


def call_ingester(sources, globus_index="mdf", batch_size=100, verbose=VERBOSE):
    ingester = import_module(ingester_import)
    ingester.ingest(sources, globus_index=globus_index, batch_size=batch_size, verbose=verbose)


def call_md_only_converter(source_name, verbose=VERBOSE):
    if type(source_name) is not list:
        source_name = [source_name]
    for src_nm in source_name:
        converter = import_module(converters_import + "metadata_only_converter")
        md_path = PATH_DATASETS + "/metadata_only/" + src_nm + ".json"
        converter.convert(md_path, verbose)


if __name__ == "__main__":
    import sys
    harvest = ["h", "harvester", "harvest"]
    convert = ["c", "converter", "convert"]
    ingest = ["i", "ingester", "ingest"]
    accept = ["a", "acceptor", "accept"]
    md_only = ["m", "md", "metadata-only"]
    if len(sys.argv) < 2:
        print("Usage statement coming soon")

    elif sys.argv[1].strip(" -").lower() in harvest:
        call_harvester(sys.argv[2])

    elif sys.argv[1].strip(" -").lower() in convert:
        call_converter(sys.argv[2:])

    elif sys.argv[1].strip(" -").lower() in ingest:
        if sys.argv[2] == "--index" or sys.argv[2] == "--globus-index":
            sys.argv.pop(2)
            globus_index = sys.argv.pop(2)
        else:
            globus_index = "mdf"

        if sys.argv[2] == "--batch-size":
            sys.argv.pop(2)
            batch_size = int(sys.argv.pop(2))
        else:
            batch_size = 100
        call_ingester(sources=sys.argv[2:], globus_index=globus_index, batch_size=batch_size)

    elif sys.argv[1].strip(" -").lower() in accept:
        if len(sys.argv) > 2 and sys.argv[2] == "--remove_old":
            remove_old = True
        else:
            remove_old = False
        call_acceptor(sys.argv[3:] or "all", remove_old=remove_old)

    elif sys.argv[1].strip(" -").lower() in md_only:
        call_md_only_converter(sys.argv[2:])

    else:
        print("Invalid option")

