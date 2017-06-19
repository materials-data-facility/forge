from importlib import import_module


def call_harvester(source_name, **kwargs):
   harvester = import_module("mdf_indexers.harvesters." + source_name + "_harvester")
   harvester.harvest(**kwargs)


def call_converter(sources, input_path=None, metadata=None, verbose=False):
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


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        call_converter(*sys.argv[1:])
    else:
        call_converter()

