from json import dump
from bson import ObjectId
import os
import paths

#Validator class holds data about a dataset while writing to feedstock
class Validator:
    #init takes dataset metadata to start processing and save another function call
    def __init__(self, metadata):
        self.__feedstock = None
        self.__dataset_id = None
        self.__mdf_source_name = None
        self.__cite_as = None
        self.__uris = []
        self.__collection = None
        self.__acl = None
        self.__license = None
        self.__data_class = None

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
        if self.__feedstock or self.__dataset_id or self.__mdf_source_name: #Metadata already set; cannot change
            return {
                "success": False,
                "message": "Metadata already written for this dataset"
                }

        md_val = validate_metadata(metadata, "dataset")
        if not md_val["success"]:
            return {
                "success": False,
                "message": md_val["message"],
                "invalid_metadata": md_val.get("invalid_metadata", "")
                }

        # Log mdf_source_name
        metadata["mdf_source_name"] = metadata["mdf_source_name"].lower().replace(" ", "_")
        self.__mdf_source_name = metadata["mdf_source_name"]

        # Log citation
        self.__cite_as = metadata["cite_as"]

        # Log collection
        self.__collection = metadata.get("mdf-publish.publication.collection", None)

        # Log default acls
        self.__acl = metadata["acl"]

        # Log default license
        self.__license = metadata.get("license", None)

        # Log data class
        self.__data_class = metadata.get("mdf_data_class", None)

        #Open feedstock file for the first time and write metadata entry
        feedstock_path = paths.feedstock + metadata["mdf_source_name"] + "_all.json"
        metadata["mdf_id"] = str(ObjectId())
        metadata["mdf_node_type"] = "dataset"
        try:
            self.__feedstock = open(feedstock_path, 'w')
            dump(metadata, self.__feedstock)
            self.__feedstock.write("\n")
            
            self.__dataset_id = metadata["mdf_id"]

            return {"success": True}

        except:
            return {
                "success": False,
                "message": "Error: Bad metadata"
                }

    #Output single record to feedstock
    def write_record(self, record):
        if (not self.__feedstock) or (not self.__dataset_id) or (not self.__mdf_source_name): #Metadata not set
            return {
                "success": False,
                "message": "Metadata not written for this dataset"
                }
        rec_val = validate_metadata(record, "record")
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

        # Copy/set non-user-settable metadata and dataset defaults
        record["mdf_id"] = str(ObjectId())
        record["parent_id"] = self.__dataset_id
        record["mdf_node_type"] = "record"
        record["mdf_source_name"] = self.__mdf_source_name

        if not record.get("cite_as", None):
            record["cite_as"] = self.__cite_as

        if not record.get("acl", None):
            record["acl"] = self.__acl

        if not record.get("mdf-publish.publication.collection", None) and self.__collection:
            record["mdf-publish.publication.collection"] = self.__collection

        if not record.get("license", None) and self.__license:
            record["license"] = self.__license

        if not record.get("mdf_data_class", None) and self.__data_class:
            record["mdf_data_class"] = self.__data_class
        elif record.get("mdf_data_class", None) != self.__data_class:
            return {
                "success": False,
                "message": "mdf_data_class mismatch: '" + record.get("mdf_data_class", None) + "' does not match dataset value of '" + self.__data_class + "'"
                }

        if record.get("mdf-base.material_composition", None):
            str_of_elem = ""
            for char in list(record["mdf-base.material_composition"]):
                if char.isupper(): #Uppercase indicates start of new element symbol
                    str_of_elem += " " + char
                elif char.islower(): #Lowercase indicates continuation of symbol
                    str_of_elem += char
                #Anything else is not useful (numbers, whitespace, etc.)
            record["mdf-base.elements"] = list(set(str_of_elem.split())) #split elements in string (on whitespace), make unique, make JSON-serializable


        new_data = {}
        namespace_exempt_keys = [
            "raw",
            "files"
            ]
        for key in namespace_exempt_keys:
            if key in record.get("data", {}).keys():
                new_data[key] = record["data"].pop(key)
        for key, value in record.get("data", {}).items():
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
        if (not self.__feedstock) or (not self.__dataset_id): #Metadata not set
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

    @property
    def dataset_id(self):
        return self.__dataset_id


# Function to validate metadata fields
# Args:
#   metadata: dict, metadata to validate
#   entry_type: string, type of metadata (dataset, record, etc.)
#   strict: bool, only allow whitelisted metadata? Default True.
def validate_metadata(metadata, entry_type, strict=True):
    # valid_meta format:
    #   field_name: {
    #       "req": bool, is field required?
    #       "type": type, datatype
    #       "contains": type, for lists, type of data inside (None for any type)
    #       }
    invalid_list = []
    if entry_type == "dataset":
        # Valid dataset metadata
        valid_meta = {
            "globus_subject": {
                "req": True,
                "type": str
                },
            "acl": {
                "req": True,
                "type": list,
                "contains": str
                },
            "mdf_source_name": {
                "req": True,
                "type": str
                },
            "mdf-publish.publication.collection": {
                "req": False,
                "type": str
                },
            "mdf_data_class": {
                "req": False,
                "type": str
                },
            "cite_as": {
                "req": True,
                "type": list,
                "contains": str
                },
            "license": {
                "req": False,
                "type": str
                },
            "dc.title": {
                "req": True,
                "type": str
                },
            "dc.creator": {
                "req": True,
                "type": str
                },
            "dc.identifier": {
                "req": True,
                "type": str
                },
            "dc.contributor.author": {
                "req": False,
                "type": list,
                "contains": str
                },
            "dc.subject": {
                "req": False,
                "type": list,
                "contains": str
                },
            "dc.description": {
                "req": False,
                "type": str
                },
            "dc.relatedidentifier": {
                "req": False,
                "type": list,
                "contains": str
                },
            "dc.year": {
                "req": False,
                "type": int
                }
            }
        # Not implemented: nist_mrr, mdf_credits
    elif entry_type == "record":
        # Valid record metadata
        valid_meta = {
            "globus_subject": {
                "req": True,
                "type": str
                },
            "acl": {
                "req": True,
                "type": list,
                "contains": str
                },
            "mdf-publish.publication.collection": {
                "req": False,
                "type": str
                },
            "mdf_data_class": {
                "req": False,
                "type": str
                },
            "mdf-base.material_composition": {
                "req": False,
                "type": str
                },
            "cite_as": {
                "req": False,
                "type": list,
                "contains": str
                },
            "license": {
                "req": False,
                "type": str
                },
            "dc.title": {
                "req": True,
                "type": str
                },
            "dc.creator": {
                "req": False,
                "type": str
                },
            "dc.identifier": {
                "req": False,
                "type": str
                },
            "dc.contributor.author": {
                "req": False,
                "type": list,
                "contains": str
                },
            "dc.subject": {
                "req": False,
                "type": list,
                "contains": str
                },
            "dc.description": {
                "req": False,
                "type": str
                },
            "dc.relatedidentifier": {
                "req": False,
                "type": list,
                "contains": str
                },
            "dc.year": {
                "req": False,
                "type": int
                },
            "data": {
                "req": False,
                "type": dict,
                "contains": None
                }
            }
        # Additional check for data block
        data_valid = validate_metadata(metadata.get("data", {}), "record_data", strict=False)
        if not data_valid["success"]:
            invalid_list += data_valid["invalid_list"]

    elif entry_type == "record_data":
        # Validate the data dict of a record's metadata
        valid_meta = {
            "raw": {
                "req": False,
                "type": str
                },
            "files": {
                "req": False,
                "type": dict,
                "contains": str
                }
            }
    else:
        return {
            "success": False,
            "message": entry_type + " is not a valid entry type."
            }

    # Check metadata
    for field, reqs in valid_meta.items():
        # If the field type is not correct or field is required but is missing, the metadata is invalid.
        # If the field is not required, the type will be instantiated and will subsequently pass the check.
        if type(metadata.get(field, None if reqs["req"] else reqs["type"]())) is not reqs["type"] or not metadata.get(field, not reqs["req"]):
            invalid_list.append({
                "field" : field,
                "value" : metadata.get(field, None),
                "reason" : field + (" is required and" if reqs["req"] else "") + " must be a non-empty " + reqs["type"].__name__
                })
        # If the field is a list and the contents should all be a given datatype, check the list.
        elif reqs["type"] is list and reqs.get("contains"):
            # Makes a list of bools. Each bool is True only is the element is not empty and is the correct type.
            # If not all bools are True, the metadata is invalid.
            if not all( [(type(elem) is reqs["contains"] and elem) for elem in metadata.get(field, None if reqs["req"] else reqs["type"]())] ):
                invalid_list.append({
                    "field" : field,
                    "value" : metadata.get(field, None),
                    "reason" : field + " must contain only non-empty " + reqs["contains"].__name__
                    })
        # Same as list check, but with a dictionary.
        elif reqs["type"] is dict and reqs.get("contains"):
            if not all( [(type(elem) is reqs["contains"] and elem) for elem in metadata.get(field, None if reqs["req"] else reqs["type"]()).values()] ):
                invalid_list.append({
                    "field": field,
                    "value" : metadata.get(field, None),
                    "reason" : field + " must contain only non-empty " + reqs["contains"].__name__
                    })

    if strict:
        # No other metadata is allowed
        disallowed_list = [x for x in metadata.keys() if x not in valid_meta.keys()]
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
            "message": "Invalid " + entry_type + " metadata"
            }

if __name__ == "__main__":
    print("\nThis is the Validator. You can use the Validator to write valid, converted data into feedstock.")
    print("There are in-depth instructions on this process in 'converter_template.py'.")
