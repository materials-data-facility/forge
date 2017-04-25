from json import load, loads, dump, dumps
from os.path import join
from tqdm import tqdm
#from ujson import dump
from bson import ObjectId
from parsers.utils import find_files, dc_validate
import paths


#Generator to run through a JSON record and yield the data
#Data is defined as the first layer that isn't a list
#Pseudo-code examples:
#   [ {1}, {2}, {3}] yields {1} then {2} then {3}
#   [ [ {1}, {2} ], [ {[3]}, {4} ] ] yields {1} then {2} then {[3]} then {4}
#   {1} yields {1}
def find_data(record):
    if type(record) is list:
        for elem in record:
            for result in find_data(elem):
                yield result
    else: #Not list, treat as data
        yield record

#Takes a JSON file and converts it into formatter-compatible JSON
#If feed_size > 0, feed_name is required
def convert_json_to_json(in_name, out_name, uri_loc, mdf_meta, feed_size=0, feed_name=None, verbose=False):
    all_uri = []
    if verbose:
        print("Converting JSON, dumping results to", out_name)
    if type(in_name) is str:
        in_name = [in_name]
    list_of_data = []
    for one_file in tqdm(in_name, desc="Processing", disable= not verbose):
        #print(one_file)
        with open(one_file, 'r') as in_file:
            #if verbose:
            #   print("Processing")
            try: #If input JSON is human-formatted (with newlines), this will fail
                for line in in_file:
                    line_data = loads(line)
                    for result in find_data(line_data):
                        list_of_data.append(result)
                        
            except Exception as err: #Fall back to reading the whole thing at once
                #if list_of_data: #If some lines were already processed, is error in JSON
                    #print("Possible error in JSON on file:", one_file, ":", err)
                    #list_of_data.clear() #Reset and try again
                #if verbose:
                #   print("Line reading failed, falling back to whole-file processing")
                in_file.seek(0) #Reset file to start
                data = load(in_file)
                for result in find_data(data):
                    list_of_data.append(result)

    with open(out_name, 'w') as out_file:       
        if list_of_data:
            if feed_size > 0:
                feed_file = open(feed_name, 'w')
            count = 0
            for entry in list_of_data:
                datum = entry["interatomic-potential"] #All data is in interatomic-potential, don't need extra nest layer
#               datum["uri"] = eval("datum['" + uri_loc + "']")
#               all_uri.append(datum["uri"])

                #Metadata
                feedstock_data = {}
                #Parse out the important bits
                url_list = []
                link_texts = []
                url_base = ""
                for artifact in datum["implementation"]:
                    for web_link in artifact["artifact"]:
                        url = web_link.get("web-link", {}).get("URL", None)
                        if url:
                            url_list.append(url)
                            if not url_base:
                                url_base = web_link["web-link"]["URL"].rsplit("/", 1)[0]
                            elif web_link["web-link"]["URL"].rsplit("/", 1)[0] != url_base:
                                print("Mismatch base_url:", web_link["web-link"]["URL"].rsplit("/", 1)[0], "vs.", url_base)
                                #exit("ERROR: Web link URL base mismatch:\n\n" + str(entry) + "\n\n")
                                pass
                        link_text = web_link.get("web-link", {}).get("link-text", None)
                        if link_text:
                            link_texts.append(link_text)

                datum["uri"] = url_base
                all_uri.append(datum["uri"])

                title = "NIST Interatomic Potential - "
                for text in link_texts:
                    title += text + ", "
                feedstock_data["dc.title"] = title
                feedstock_data["dc.creator"] = "NIST Interatomic Potentials Repository Project"
#               feedstock_data["dc.contributor.author"] = []
                feedstock_data["dc.identifier"] = datum["uri"] if datum["uri"] else datum["id"]
                feedstock_data["dc.subject"] = ["interatomic potential", "forcefield"]

                description = ""
                for note in datum["description"]["notes"]:
                    description += note["text"] + "; "
                feedstock_data["dc.description"] = description

                related_list = url_list
                for citation in datum["description"]["citation"]:
                    doi = citation.get("DOI", None)
                    if doi:
                        related_list.append("http://dx.doi.org/" + doi)
                feedstock_data["dc.relatedidentifier"] = related_list
                feedstock_data["dc.year"] = int(datum["id"][:4])

                composition = ""
                for elem in datum["element"]:
                    composition += elem + " "
                feedstock_data["mdf-base.material_composition"] = composition

                feedstock_data["mdf_id"] = str(ObjectId())
                feedstock_data["mdf_source_name"] = mdf_meta["mdf_source_name"]
                feedstock_data["mdf_source_id"] = mdf_meta["mdf_source_id"]
#               feedstock_data["globus_source"] = mdf_meta.get("globus_source", "")
                feedstock_data["mdf_datatype"] = mdf_meta["mdf_datatype"]
                feedstock_data["acl"] = mdf_meta["acl"]
                feedstock_data["globus_subject"] = feedstock_data["dc.identifier"]
                feedstock_data["mdf-publish.publication.collection"] = mdf_meta["collection"]
                feedstock_data["data_links"] = url_list
                feedstock_data["data"] = { "raw" : dumps(entry) } #Only thing to keep is the raw data

                dc_validation = dc_validate(feedstock_data)
                if not dc_validation["valid"]:
                    exit("ERROR: Invalid fields: " + str(dc_validation["invalid_fields"]))

                dump(feedstock_data, out_file)
                out_file.write('\n')
                if count < feed_size:
                    dump(feedstock_data, feed_file)
                    feed_file.write('\n')
                count += 1
            print("Data written successfully")
        else:
            print("Error: No data recovered from file")
    duplicates = [x for x in all_uri if all_uri.count(x) > 1]
    if duplicates:
        print("Warning: Duplicate URIs found:\n", set(duplicates))

if __name__ == "__main__":
    verbose = True
    nist_ip_mdf = {
        "mdf_source_name" : "nist_ip",
        "mdf_source_id" : 11,
#       "globus_source" : "NIST Interatomic Potentials",
        "mdf_datatype" : "json",
        "acl" : ["public"],
        "collection" : "NIST Interatomic Potentials"
        }
    nist_ip_in = paths.datasets + "nist_ip/interchange"
    nist_ip_out = paths.raw_feed + "nist_ip_all.json"
    nist_ip_uri = "id"
    nist_ip_sack_size = 10
    nist_ip_feed = paths.sack_feed + "nist_ip_10.json"
    nist_ip_file_list = []
    for file_data in find_files(nist_ip_in, ".*\.json$"):
        nist_ip_file_list.append(join(file_data["path"], file_data["filename"] + file_data["extension"]))
    convert_json_to_json(in_name=nist_ip_file_list, out_name=nist_ip_out, uri_loc=nist_ip_uri, mdf_meta=nist_ip_mdf, feed_size=nist_ip_sack_size, feed_name=nist_ip_feed, verbose=verbose)
        


