from importlib import import_module


VERBOSE = True


def call_harvester(source_name, verbose=VERBOSE, **kwargs):
    if verbose:
        print("HARVESTING", source_name)
    harvester = import_module("mdf_indexers.harvesters." + source_name + "_harvester")
    harvester.harvest(verbose=verbose, **kwargs)
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
        converter = import_module("mdf_indexers.converters." + source_name + "_converter")
        if not input_path:
            # Relative path is from calling function, not sub-function: paths.datasets will be wrong
            # Use "mdf_indexers/datasets/X" instead
            input_path = "mdf_indexers/datasets/" + source_name + "/"
        converter.convert(input_path=input_path, metadata=metadata, verbose=verbose)
    if verbose:
        print("\nALL CONVERTING COMPLETE")


def call_ingester(sources, globus_index="mdf", batch_size=100, verbose=VERBOSE):
    ingester = import_module("mdf_indexers.ingester.data_ingester")
    ingester.ingest(sources, globus_index=globus_index, batch_size=batch_size, verbose=verbose)


def call_acceptor(sources="all", strict=True, remove_old=True, verbose=VERBOSE):
    acceptor = import_module("mdf_indexers.acceptor.feedstock_acceptor")
    if "all" in sources:
        acceptor.accept_all(strict=strict, remove_old=remove_old, verbose=verbose)
    else:
        if type(sources) is not list:
            sources = [sources]
        for src in sources:
            acceptor.accept_feedstock("mdf_indexers/acceptor/acceptor_inbox/" + src + "all.json", strict=strict, remove_old=remove_old, verbose=verbose)


if __name__ == "__main__":
    import sys
    harvest = ["h", "harvester", "harvest"]
    convert = ["c", "converter", "convert"]
    ingest = ["i", "ingester", "ingest"]
    accept = ["a", "acceptor", "accept"]
    if len(sys.argv) < 2:
        print("Usage statement coming soon")

    elif sys.argv[1].strip(" -").lower() in harvest:
        call_harvester(sys.argv[2])

    elif sys.argv[1].strip(" -").lower() in convert:
        call_converter(*sys.argv[2:])

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
        if len(sys.argv) > 2 and sys.argv[2] == "--save_old":
            remove_old = False
        else:
            remove_old = True
        call_acceptor(sys.argv[3:] or "all", remove_old=remove_old)

    else:
        print("Invalid option")

