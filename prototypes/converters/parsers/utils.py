import os
import re
from tqdm import tqdm
import tarfile
import zipfile
import gzip
#Finds all directories containing a specified type of file and returns list of dicts with path to files and data gleaned from folder names
#Arguments:
#   root: Path to the first dir to start with. Default is current working directory.
#   file_match: regex string containing the file name to search for. Default is None, which matches all files.
#   keep_dir_name_depth: How many layers of dir, counting from the base file up, contain data to save. -1 saves everything in the path. Default is 0, which disables saving anything.
#   max_files: Maximum number of results to return. Default -1, which returns all results.
#   uncompress_archives: If True, will uncompress archives found before checking against the file_pattern. This is *SLOW*, and does not guarantee archives that expand into directories will be searched. Default False, which leaves archives alone.
def find_files(root=None, file_pattern=None, keep_dir_name_depth=0, max_files=-1, uncompress_archives=False, verbose=False):
    if not root:
        root = os.getcwd()
    dir_list = []
    for path, dirs, files in tqdm(os.walk(root), desc="Finding files", disable= not verbose):

        if uncompress_archives:
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
                    except IOError: #This will occur at gz.read() if the file is not a gzip. After it has been opened. I don't know why it doesn't have any format check before this.
                        pass
            all_files = os.listdir(path)
        else:
            all_files = files

        for one_file in all_files:
            if not file_pattern or re.search(file_pattern, one_file): #Only care about dirs with desired data
                dir_names = []
                head, tail = os.path.split(path)
                dir_names.append(tail)
                while head and head != os.sep:
                    head, tail = os.path.split(head)
                    dir_names.append(tail)  
                if keep_dir_name_depth >= 0:
                    dir_names = dir_names[:keep_dir_name_depth]
                dir_names.reverse() #append leaves path elements in reverse order (/usr/xyz/stuff/ -> [stuff, xyz, usr]), should make right
                #Find and save extension
                name_and_ext = one_file.rsplit('.', 1)
                if name_and_ext[0]: #Hidden files on *nix cause issues ('.name') and are probably not part of data, so ignore them
                    dir_data = {
                        "path" : path,
                        "dirs" : dir_names,
                        "filename" : name_and_ext[0]
                        }
                    try:
                        dir_data["extension"] = '.' + name_and_ext[1]
                    except IndexError:
                        dir_data["extension"] = ''
                    dir_list.append(dir_data)
    if max_files >= 0:
        return dir_list[:max_files]
    else:
        return dir_list

dc_template = {
    "dc.title" : str(), #Required
    "dc.creator" : str(), #Required
    "dc.contributor.author" : list(),
    "dc.identifier" : str(), #Required
    "dc.subject" : list(),
    "dc.description" : str(),
    "dc.relatedidentifier" : str(),
    "dc.year" : int()
    }
#Takes dict of DataCite formatted metadata, checks if valid
#If invalid, returns valid=false and the invalid data
def dc_validate(dc_raw):
    dc_data = {}
    for key, value in dc_raw.items(): #Clear out empty values
        if value:
            dc_data[key] = value
    invalid = {}
    if type(dc_data.get("dc.title", None)) is not str:
        invalid["dc.title"] = dc_data.get("dc.title", None)
    if type(dc_data.get("dc.creator", None)) is not str:
        invalid["dc.creator"] = dc_data.get("dc.creator", None)
    if type(dc_data.get("dc.contributor.author", list())) is not list:
        invalid["dc.contributor.author"] = dc_data["dc.contributor.author"]
    if type(dc_data.get("dc.identifier", None)) is not str:
        invalid["dc.identifier"] = dc_data.get("dc.identifier", None)
    if type(dc_data.get("dc.subject", list())) is not list:
        invalid["dc.subject"] = dc_data["dc.subject"]
    if type(dc_data.get("dc.description", str())) is not str:
        invalid["dc.description"] = dc_data["dc.description"]
    if type(dc_data.get("dc.relatedidentifier", list())) is not list:
        invalid["dc.relatedidentifier"] = dc_data["dc.relatedidentifier"]
    if type(dc_data.get("dc.year", int())) is not int:
        invalid["dc.year"] = dc_data["dc.year"]
    
    if invalid:
        return {
            "valid" : False,
            "invalid_fields" : invalid
            }
    else:
        return {
            "valid" : True,
            "validated" : dc_data
            }

if __name__ == "__main__":
    print("\nThis is a module containing miscellaneous utility functions.")
    print("Functions:\n")
    print("find_files(root=None, file_pattern=None, keep_dir_name_depth=0, max_files=-1, uncompress_archives=False, verbose=False)")
    print("Finds all directories containing a specified type of file and returns list of dicts with path to files and data gleaned from folder names")
    print("Arguments:\n\troot: Path to the first dir to start with. Default is current working directory.")
    print("\tfile_match: regex string containing the file name to search for. Default is None, which matches all files.")
    print("\tkeep_dir_name_depth: How many layers of dir, counting from the base file up, contain data to save. -1 saves everything in the path. Default is 0, which disables saving anything.")
    print("\tmax_files: Maximum number of results to return. Default -1, which returns all results.")
    print("\tuncompress_archives: If True, will uncompress archives found before checking against the file_pattern. This is *SLOW*, and does not guarantee archives that expand into directories will be searched. Default False, which leaves archives alone.")

    print("\ndc_validate(dc_raw)")
    print("Checks data for valid DataCite (modified) metadata")
    print("If the metadata is invalid, returns valid=false and the invalid data")
    print("Arguments:\n\tdc_raw: Dictionary of metadata to be validated\n")


