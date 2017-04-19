from json import dump
from bson import ObjectId
import os
import paths

#Validator class holds data about a dataset while writing to feedstock
class Validator:
    #init takes dataset metadata to start processing and save another function call
    def __init__(self, metadata):
        self.__feedstock = None
        self.dataset_id = None
        self.__mdf_source_name = None
        self.__uris = []

        res = self.__write_metadata(metadata)
        if not res["success"]:
            raise ValueError("Invalid metadata: '" + res["message"] + "' " + str(res.get("invalid_metadata", "")))

    #del attempts cleanup
    def __del__(self):
        try:
            self.__feedstock.close()
        except AttributeError: #Feedstock wasn't opened
            pass

    #Sets metadata for dataset
    def __write_metadata(self, metadata):
        if self.__feedstock or self.dataset_id or self.__mdf_source_name: #Metadata already set; cannot change
            return {
                "success": False,
                "message": "Metadata already written for this dataset"
                }

        md_val = validate_metadata(metadata)
        if not md_val["success"]:
            return {
                "success": False,
                "message": md_val["message"],
                "invalid_metadata": md_val.get("invalid_metadata", "")
                }

        metadata["mdf_source_name"] = metadata["mdf_source_name"].lower().replace(" ", "_")
        self.__mdf_source_name = metadata["mdf_source_name"]

        #Open feedstock file for the first time and write metadata entry
        feedstock_path = paths.feedstock + metadata["mdf_source_name"] + "_all.json"
        metadata["mdf_id"] = str(ObjectId())
        metadata["mdf_node_type"] = "dataset"
        try:
            self.__feedstock = open(feedstock_path, 'w')
            dump(metadata, self.__feedstock)
            self.__feedstock.write("\n")
            
            self.dataset_id = metadata["mdf_id"]

            return {"success": True}

        except:
            return {
                "success": False,
                "message": "Error: Bad metadata"
                }

    #Output single record to feedstock
    def write_record(self, record):
        if (not self.__feedstock) or (not self.dataset_id) or (not self.__mdf_source_name): #Metadata not set
            return {
                "success": False,
                "message": "Metadata not written for this dataset"
                }
        rec_val = validate_record(record)
        if not rec_val["success"]:
            return {
                "success": False,
                "message": rec_val["message"],
                "invalid_metadata": rec_val["invalid_metadata"]
                }

        # Check for duplicate URIs
        if record["globus_subject"] in self.__uris:
            return {
                "success": False,
                "message": "'globus_subject' duplicate found:" + record["globus_subject"]
                }
        else:
            self.__uris.append(record["globus_subject"])

        record["mdf_id"] = str(ObjectId())
        record["parent_id"] = self.dataset_id
        record["mdf_node_type"] = "record"
        record["mdf_source_name"] = self.__mdf_source_name

        if record.get("mdf-base.material_composition", None):
            str_of_elem = ""
            for char in list(record["mdf-base.material_composition"]):
                if char.isupper(): #Uppercase indicates start of new element symbol
                    str_of_elem += " " + char
                elif char.islower(): #Lowercase indicates continuation of symbol
                    str_of_elem += char
                #Anything else is not useful (numbers, whitespace, etc.)
            record["mdf-base.elements"] = list(set(str_of_elem.split())) #split elements in string (on whitespace), make unique, make JSON-serializable


        new_data = {
            "files" : record["data"].pop("files")
            }
        if "raw" in record["data"].keys():
            new_data["raw"] = record["data"].pop("raw")
        for key, value in record["data"].items():
            new_data[self.__mdf_source_name + ":" + key] = value
        record["data"] = new_data

        #Write new record to feedstock
        try:
            dump(record, self.__feedstock)
            self.__feedstock.write("\n")
            return {"success" : True}
        except:
            return {
                "success": False,
                "message": "Error: Bad record"
                }

    #Output whole dataset to feedstock
    #all_records must be a list of all the dataset records
    def write_dataset(self, all_records):
        if (not self.__feedstock) or (not self.dataset_id): #Metadata not set
            return {
                "success": False,
                "message": "Metadata not written for this dataset"
                }
        #Write all records to feedstock
        for record in all_records:
            result = self.write_record(record)
            if not result["success"]:
                print("Error on record: ", record)
        return {"success" : True}


#Function to validate metadata fields
def validate_metadata(metadata):
    valid_list = [
        "globus_subject",
        "acl",
        "mdf_source_name",
        "mdf-publish.publication.collection",
        "dc.title",
        "dc.creator",
        "dc.identifier",
        "dc.contributor.author",
        "dc.subject",
        "dc.description",
        "dc.relatedidentifier",
        "dc.year"
        ]
    invalid_list = []

    #globus_subject must exist, and be a non-empty string
    if type(metadata.get("globus_subject", None)) is not str or not metadata.get("globus_subject"):
        invalid_list.append({
            "field" : "globus_subject",
            "value" : metadata.get("globus_subject", None),
            "reason" : "globus_subject is required and must be a string"
            })

    #acl must exist, and be a list
    if type(metadata.get("acl", None)) is not list:
        invalid_list.append({
            "field" : "acl",
            "value" : metadata.get("acl", None),
            "reason" : "acl is required and must be a list"
            })
    #acl must contain only non-empty strings
    elif not all( [ (type(elem) is str and elem) for elem in metadata.get("acl") ] ):
        invalid_list.append({
            "field" : "acl",
            "value" : metadata.get("acl", None),
            "reason" : "acl must contain only non-empty strings"
            })
    
    #mdf_source_name must exist, and be a non-empty string
    if type(metadata.get("mdf_source_name", None)) is not str or not metadata.get("mdf_source_name"):
        invalid_list.append({
            "field" : "mdf_source_name",
            "value" : metadata.get("mdf_source_name", None),
            "reason" : "mdf_source_name is required and must be a string"
            })

    #mdf-publish.publication.collection, if it exists, must be a non-empty string
    if type(metadata.get("mdf-publish.publication.collection", "")) is not str or not metadata.get("mdf-publish.publication.collection", True):
        invalid_list.append({
            "field" : "mdf-publish.publication.collection",
            "value" : metadata.get("mdf-publish.publication.collection", None),
            "reason" : "mdf-publish.publication.collection must be a non-empty string"
            })

    #dc.title must exist, and be a non-empty string
    if type(metadata.get("dc.title", None)) is not str or not metadata.get("dc.title"):
        invalid_list.append({
            "field" : "dc.title",
            "value" : metadata.get("dc.title", None),
            "reason" : "dc.title is required and must be a string"
            })
    
    #dc.creator must exist, and be a non-empty string
    if type(metadata.get("dc.creator", None)) is not str or not metadata.get("dc.creator"):
        invalid_list.append({
            "field" : "dc.creator",
            "value" : metadata.get("dc.creator", None),
            "reason" : "dc.creator is required and must be a string"
            })

    #dc.identifier must exist, and be a non-empty string
    if type(metadata.get("dc.identifier", None)) is not str or not metadata.get("dc.identifier"):
        invalid_list.append({
            "field" : "dc.identifier",
            "value" : metadata.get("dc.identifier", None),
            "reason" : "dc.identifier is required and must be a string"
            })

    #dc.contributor.author, if it exists, must be a list
    if type(metadata.get("dc.contributor.author", [])) is not list:
        invalid_list.append({
            "field" : "dc.contributor.author",
            "value" : metadata.get("dc.contributor.author", None),
            "reason" : "dc.contributor.author must be a list"
            })
    #dc.contributor.author, if it exists, must contain only non-empty strings
    elif not all( [ (type(elem) is str and elem) for elem in metadata.get("dc.contributor.author", []) ] ):
        invalid_list.append({
            "field" : "dc.contributor.author",
            "value" : metadata.get("dc.contributor.author", None),
            "reason" : "dc.contributor.author must contain only non-empty strings"
            })

    #dc.subject, if it exists, must be a list
    if type(metadata.get("dc.subject", [])) is not list:
        invalid_list.append({
            "field" : "dc.subject",
            "value" : metadata.get("dc.subject", None),
            "reason" : "dc.subject must be a list"
            })
    #dc.subject, if it exists, must contain only non-empty strings
    elif not all( [ (type(elem) is str and elem) for elem in metadata.get("dc.subject", []) ] ):
        invalid_list.append({
            "field" : "dc.subject",
            "value" : metadata.get("dc.subject", None),
            "reason" : "dc.subject must contain only non-empty strings"
            })

    #dc.description, if it exists, must be a non-empty string
    if type(metadata.get("dc.description", "")) is not str or not metadata.get("dc.identifier", True):
        invalid_list.append({
            "field" : "dc.description",
            "value" : metadata.get("dc.description", None),
            "reason" : "dc.description must be a non-empty string"
            })

    #dc.relatedidentifier, if it exists, must be a list
    if type(metadata.get("dc.relatedidentifier", [])) is not list:
        invalid_list.append({
            "field" : "dc.relatedidentifier",
            "value" : metadata.get("dc.relatedidentifier", None),
            "reason" : "dc.relatedidentifier must be a list"
            })
    #dc.relatedidentifier, if it exists, must contain only non-empty strings
    elif not all( [ (type(elem) is str and elem) for elem in metadata.get("dc.relatedidentifier", []) ] ):
        invalid_list.append({
            "field" : "dc.relatedidentifier",
            "value" : metadata.get("dc.relatedidentifier", None),
            "reason" : "dc.relatedidentifier must contain only non-empty strings"
            })

    #dc.year, if it exists, must be an int
    if type(metadata.get("dc.year", 0)) is not int:
        invalid_list.append({
            "field" : "dc.year",
            "value" : metadata.get("dc.year", None),
            "reason" : "dc.year must be an integer"
            })

    #NIST_MRR fields
    #Not implemented

    #mdf_credits
    #Not implemented

    #No other metadata is allowed
    disallowed_list = [x for x in metadata.keys() if x not in valid_list]
    for key in disallowed_list:
        invalid_list.append({
            "field" : key,
            "value" : metadata.get(key, None),
            "reason" : key + " is not a valid metadata field"
            })

    if not invalid_list:
        return {"success": True}
    else:
        return {
            "success": False,
            "invalid_metadata": invalid_list,
            "message": "Invalid dataset metadata"
            }


#Function to validate record fields
def validate_record(metadata):
    valid_list = [
        "globus_subject",
        "acl",
        #"mdf_source_name", 
        "mdf-publish.publication.collection",
        "mdf_data_class",
        "mdf-base.material_composition",
        "dc.title",
        "dc.creator",
        "dc.identifier",
        "dc.contributor.author",
        "dc.subject",
        "dc.description",
        "dc.relatedidentifier",
        "dc.year",
        "data"
        ]
    invalid_list = []

    #globus_subject must exist, and be a non-empty string
    if type(metadata.get("globus_subject", None)) is not str or not metadata.get("globus_subject"):
        invalid_list.append({
            "field" : "globus_subject",
            "value" : metadata.get("globus_subject", None),
            "reason" : "globus_subject is required and must be a string"
            })

    #acl must exist, and be a list
    if type(metadata.get("acl", None)) is not list:
        invalid_list.append({
            "field" : "acl",
            "value" : metadata.get("acl", None),
            "reason" : "acl is required and must be a list"
            })
    #acl must contain only non-empty strings
    elif not all( [ (type(elem) is str and elem) for elem in metadata.get("acl") ] ):
        invalid_list.append({
            "field" : "acl",
            "value" : metadata.get("acl", None),
            "reason" : "acl must contain only non-empty strings"
            })

# Requirement removed
#   #mdf_source_name must exist, and be a non-empty string
#   if type(metadata.get("mdf_source_name", None)) is not str or not metadata.get("mdf_source_name"):
#       invalid_list.append({
#           "field" : "mdf_source_name",
#           "value" : metadata.get("mdf_source_name", None),
#           "reason" : "mdf_source_name is required and must be a string"
#           })

    #mdf-publish.publication.collection, if it exists, must be a non-empty string
    if type(metadata.get("mdf-publish.publication.collection", "")) is not str or not metadata.get("mdf-publish.publication.collection", True):
        invalid_list.append({
            "field" : "mdf-publish.publication.collection",
            "value" : metadata.get("mdf-publish.publication.collection", None),
            "reason" : "mdf-publish.publication.collection must be a non-empty string"
            })

    #mdf_data_class, if it exists, must be a non-empty string
    if type(metadata.get("mdf_data_class", "")) is not str or not metadata.get("mdf_data_class", True):
        invalid_list.append({
            "field" : "mdf_data_class",
            "value" : metadata.get("mdf_data_class", None),
            "reason" : "mdf_data_class must be a non-empty string"
            })

    #mdf-base.material_composition, if it exists, must be a non-empty string
    if type(metadata.get("mdf-base.material_composition", "")) is not str or not metadata.get("mdf-base.material_composition", True):
        invalid_list.append({
            "field" : "mdf-base.material_composition",
            "value" : metadata.get("mdf-base.material_composition", None),
            "reason" : "mdf-base.material_composition must be a non-empty string"
            })

    #dc.title must exist, and be a non-empty string
    if type(metadata.get("dc.title", None)) is not str or not metadata.get("dc.title"):
        invalid_list.append({
            "field" : "dc.title",
            "value" : metadata.get("dc.title", None),
            "reason" : "dc.title is required and must be a string"
            })
    
    #dc.creator, if it exists, must be a non-empty string
    if type(metadata.get("dc.creator", "")) is not str or not metadata.get("dc.creator", True):
        invalid_list.append({
            "field" : "dc.creator",
            "value" : metadata.get("dc.creator", None),
            "reason" : "dc.creator must be a string"
            })

    #dc.identifier, if it exists, must be a non-empty string
    if type(metadata.get("dc.identifier", "")) is not str or not metadata.get("dc.identifier", True):
        invalid_list.append({
            "field" : "dc.identifier",
            "value" : metadata.get("dc.identifier", None),
            "reason" : "dc.identifier must be a string"
            })

    #dc.contributor.author, if it exists, must be a list
    if type(metadata.get("dc.contributor.author", [])) is not list:
        invalid_list.append({
            "field" : "dc.contributor.author",
            "value" : metadata.get("dc.contributor.author", None),
            "reason" : "dc.contributor.author must be a list"
            })
    #dc.contributor.author, if it exists, must contain only non-empty strings
    elif not all( [ (type(elem) is str and elem) for elem in metadata.get("dc.contributor.author", []) ] ):
        invalid_list.append({
            "field" : "dc.contributor.author",
            "value" : metadata.get("dc.contributor.author", None),
            "reason" : "dc.contributor.author must contain only non-empty strings"
            })

    #dc.subject, if it exists, must be a list
    if type(metadata.get("dc.subject", [])) is not list:
        invalid_list.append({
            "field" : "dc.subject",
            "value" : metadata.get("dc.subject", None),
            "reason" : "dc.subject must be a list"
            })
    #dc.subject, if it exists, must contain only non-empty strings
    elif not all( [ (type(elem) is str and elem) for elem in metadata.get("dc.subject", []) ] ):
        invalid_list.append({
            "field" : "dc.subject",
            "value" : metadata.get("dc.subject", None),
            "reason" : "dc.subject must contain only non-empty strings"
            })

    #dc.description, if it exists, must be a non-empty string
    if type(metadata.get("dc.description", "")) is not str or not metadata.get("dc.identifier", True):
        invalid_list.append({
            "field" : "dc.description",
            "value" : metadata.get("dc.description", None),
            "reason" : "dc.description must be a non-empty string"
            })

    #dc.relatedidentifier, if it exists, must be a list
    if type(metadata.get("dc.relatedidentifier", [])) is not list:
        invalid_list.append({
            "field" : "dc.relatedidentifier",
            "value" : metadata.get("dc.relatedidentifier", None),
            "reason" : "dc.relatedidentifier must be a list"
            })
    #dc.relatedidentifier, if it exists, must contain only non-empty strings
    elif not all( [ (type(elem) is str and elem) for elem in metadata.get("dc.relatedidentifier", []) ] ):
        invalid_list.append({
            "field" : "dc.relatedidentifier",
            "value" : metadata.get("dc.relatedidentifier", None),
            "reason" : "dc.relatedidentifier must contain only non-empty strings"
            })

    #dc.year, if it exists, must be an int
    if type(metadata.get("dc.year", 0)) is not int:
        invalid_list.append({
            "field" : "dc.year",
            "value" : metadata.get("dc.year", None),
            "reason" : "dc.year must be an integer"
            })

    #mdf_facts
    #Not implemented

    #mdf_credits
    #Not implemented

    #data must exist, and be a dictionary
    if type(metadata.get("data", None)) is not dict:
        invalid_list.append({
            "field" : "data",
            "value" : metadata.get("data", None),
            "reason" : "data is required"
            })
    elif type(metadata.get("data").get("raw", "")) is not str:
        invalid_list.append({
            "field" : "data['raw']",
            "value" : metadata.get("data").get("raw", None),
            "reason" : "data['raw'] must be a string"
            })
    elif type(metadata.get("data").get("files", None)) is not dict:
        invalid_list.append({
            "field" : "data['files']",
            "value" : metadata.get("data").get("files", None),
            "reason" : "data['files'] is required and must be a dictionary (but may be empty)"
            })


    #No other metadata is allowed
    disallowed_list = [x for x in metadata.keys() if x not in valid_list]
    for key in disallowed_list:
        invalid_list.append({
            "field" : key,
            "value" : metadata.get(key, None),
            "reason" : key + " is not a valid metadata field"
            })

    if not invalid_list:
        return {"success": True}
    else:
        return {
            "success": False,
            "invalid_metadata": invalid_list,
            "message": "Invalid record metadata"
            }

if __name__ == "__main__":
    print("\nThis is the Validator. You can use the Validator to write valid, converted data into feedstock.")
    print("There are in-depth instructions on this process in 'converter_template.py'.")
