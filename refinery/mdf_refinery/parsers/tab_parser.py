def parse_tab(in_str, sep=',', headers=[], entry_sep="\n"):
    # Tabular data rows should be delineated by newline
    in_str = in_str.strip().split(entry_sep)
    # Infer headers from first line if not provided
    if not headers:
        headers = in_str.pop(0).split(sep)
    # Make {header: value} with all headers for each record
    while in_str:
        try:
            line = in_str.pop(0).split(sep)
        except IndexError:
            break  # Should never happen
        data = {}
        for head in headers:
            try:
                data[head] = line.pop(0)
            except IndexError:
                data[head] = None
        yield data


if __name__ == "__main__":
    print("\nThis is the parser for tabular data.")
    print("USAGE:\n\nparse_tab(in_str, sep=',', headers=[])")
    print("Arguments:\n\tin_str: String containing tabular data")
    print("\tsep: The separator between data elements. Default ','")
    print("\theaders: List of headers for the data. Default []")
    print("\t\tIf headers is empty, the headers will be taken from the first line of input.")
    print("\t\tIf there are not enough headers for the data, the extra data will be lost.")
    print("\tentry_sep: The separator between rows or lines of data. Default newline ('\\n')")
    print("Yields: Each record as a dictionary in the format {header1: value, ..., headerN: value}")
