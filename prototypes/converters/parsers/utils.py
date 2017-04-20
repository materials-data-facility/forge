import os
import re
from tqdm import tqdm


#Finds files inside a given directory (recursively) and returns path and filename info.
#Arguments:
#   root: Path to the first dir to start with. Required.
#   file_pattern: regex string to search for. Default is None, which matches all files.
#   verbose: Should the script print status messages? Default False.
def find_files(root, file_pattern=None, verbose=False):
    # Add separator to end of root if not already supplied
    root += os.sep if root[-1:] != os.sep else ""
    dir_list = []
    for path, dirs, files in tqdm(os.walk(root), desc="Finding files", disable= not verbose):
        for one_file in files:
            if not file_pattern or re.search(file_pattern, one_file):  # Only care about dirs with desired data
                dir_list.append({
                    "path": path,
                    "filename": one_file,
                    "no_root_path": path.replace(root, "")
                    })
    return dir_list


#Uncompresses all tar, zip, and gzip archives in a directory (searched recursively). Very slow.
def uncompress_tree(root, verbose=False):
    import tarfile
    import zipfile
    import gzip
    for path, dirs, files in tqdm(os.walk(root), desc="Uncompressing files", disable= not verbose):
        for single_file in files:
            abs_path = os.path.join(path, single_file)
            if tarfile.is_tarfile(abs_path):
                tar = tarfile.open(abs_path)
                tar.extractall()
                tar.close()
            elif zipfile.is_zipfile(abs_path):
                z = zipfile.open(abs_path)
                z.extractall()
                z.close()
            else:
                try:
                    with gzip.open(abs_path) as gz:
                        file_data = gz.read()
                        with open(abs_path.rsplit('.', 1)[0], 'w') as newfile: #Opens the absolute path, including filename, for writing, but does not include the extension (should be .gz or similar)
                            newfile.write(file_data)
                except IOError: #This will occur at gz.read() if the file is not a gzip.
                    pass


if __name__ == "__main__":
    print("\nThis is a module containing miscellaneous utility functions.")
    print("Functions:\n")
    print("find_files(root, file_pattern=None, verbose=False)")
    print("Finds files inside a given directory (recursively) and returns path and filename info")
    print("Arguments:\n\troot: Path to the first dir to start with. Required.")
    print("\tfile_pattern: regex string to search for. Default is None, which matches all files.")
    print("\tverbose: Should the script print status messages? Default False.")
    print("\nuncompress_tree(root, verbose=False)")
    print("Uncompresses all tar, zip, and gzip archives in a directory (searched recursively). Very slow.")
    print("Arguments:\n\troot: Path to the first dir to start with. Required.")
    print("\tverbose: Should the script print status messages? Default False.")
