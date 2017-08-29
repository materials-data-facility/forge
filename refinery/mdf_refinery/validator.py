import json
import os
import re
from copy import deepcopy
from datetime import datetime
import jsonschema
from bson import ObjectId

from mdf_refinery.config import PATH_FEEDSTOCK, MDF_PATH

PATH_SCHEMAS = os.path.join(os.path.dirname(__file__), "schemas")
PATH_REPO_CACHE = os.path.join(MDF_PATH, ".repositories.json")

##################
VALIDATOR_VERSION = "0.4.x"
##################

DICT_OF_ALL_ELEMENTS = {"Actinium": "Ac", "Silver": "Ag", "Aluminum": "Al", "Americium": "Am", "Argon": "Ar", "Arsenic": "As", "Astatine": "At", "Gold": "Au", "Boron": "B", "Barium": "Ba", "Beryllium": "Be", "Bohrium": "Bh", "Bismuth": "Bi", "Berkelium": "Bk", "Bromine": "Br", "Carbon": "C", "Calcium": "Ca", "Cadmium": "Cd", "Cerium": "Ce", "Californium": "Cf", "Chlorine": "Cl", "Curium": "Cm", "Copernicium": "Cn", "Cobalt": "Co", "Chromium": "Cr", "Cesium": "Cs", "Copper": "Cu", "Dubnium": "Db", "Darmstadtium": "Ds", "Dysprosium": "Dy", "Erbium": "Er", "Einsteinium": "Es", "Europium": "Eu", "Fluorine": "F", "Iron": "Fe", "Flerovium": "Fl", "Fermium": "Fm", "Francium": "Fr", "Gallium": "Ga", "Gadolinium": "Gd", "Germanium": "Ge", "Hydrogen": "H", "Helium": "He", "Hafnium": "Hf", "Mercury": "Hg", "Holmium": "Ho", "Hassium": "Hs", "Iodine": "I", "Indium": "In", "Iridium": "Ir", "Potassium": "K", "Krypton": "Kr", "Lanthanum": "La", "Lithium": "Li", "Lawrencium": "Lr", "Lutetium": "Lu", "Livermorium": "Lv", "Mendelevium": "Md", "Magnesium": "Mg", "Manganese": "Mn", "Molybdenum": "Mo", "Meitnerium": "Mt", "Nitrogen": "N", "Sodium": "Na", "Niobium": "Nb", "Neodymium": "Nd", "Neon": "Ne", "Nickel": "Ni", "Nobelium": "No", "Neptunium": "Np", "Oxygen": "O", "Osmium": "Os", "Phosphorus": "P", "Protactinium": "Pa", "Lead": "Pb", "Palladium": "Pd", "Promethium": "Pm", "Polonium": "Po", "Praseodymium": "Pr", "Platinum": "Pt", "Plutonium": "Pu", "Radium": "Ra", "Rubidium": "Rb", "Rhenium": "Re", "Rutherfordium": "Rf", "Roentgenium": "Rg", "Rhodium": "Rh", "Radon": "Rn", "Ruthenium": "Ru", "Sulfur": "S", "Antimony": "Sb", "Scandium": "Sc", "Selenium": "Se", "Seaborgium": "Sg", "Silicon": "Si", "Samarium": "Sm", "Tin": "Sn", "Strontium": "Sr", "Tantalum": "Ta", "Terbium": "Tb", "Technetium": "Tc", "Tellurium": "Te", "Thorium": "Th", "Titanium": "Ti", "Thallium": "Tl", "Thulium": "Tm", "Uranium": "U", "Ununoctium": "Uuo", "Ununpentium": "Uup", "Ununseptium": "Uus", "Ununtrium": "Uut", "Vanadium": "V", "Tungsten": "W", "Xenon": "Xe", "Yttrium": "Y", "Ytterbium": "Yb", "Zinc": "Zn", "Zirconium": "Zr"}

MAX_KEYS = 20
MAX_LIST = 5


#Validator class holds data about a dataset while writing to feedstock
class Validator:
    #init takes dataset metadata to start processing and save another function call
    def __init__(self, metadata=None, resource_type="dataset", version=VALIDATOR_VERSION):
        if not metadata:
            raise ValueError("You must specify the metadata for this " + resource_type)
        if not version.startswith(VALIDATOR_VERSION.replace(".x", "")):
            print("Caution: You are using the", VALIDATOR_VERSION, "version of the Validator for metadata in version", version, "which could cause errors.")
        self.__initialized = False
        self.__landing_pages = []
################################
        self.__scroll_id = 1

        self.__schemas = {}
        schema_items = []
        try:
            for item in os.listdir(PATH_SCHEMAS):
                # Save all schemas in schema dir
                if os.path.isfile(os.path.join(PATH_SCHEMAS, item)) and item.endswith(".schema"):
                    schema_items.append(item)
        except Exception as e:
            raise

        # If version was not specified in call, use highest available that matches validator version
        if "x" in version:
            base_ver = version.replace(".x", "")
            poss_vers = [ver.split("_")[0].replace(base_ver, "").strip(".") for ver in schema_items if ver.startswith(base_ver)]
            if not poss_vers:
                raise FileNotFoundError("No schemas found for validator version " + version)
            high_ver = max([int(ver) for ver in poss_vers])
            version = version.replace("x", str(high_ver))

        self.__version = version

        try:
            for schema in [s for s in schema_items if s.startswith(version)]:
                with open(os.path.join(PATH_SCHEMAS, schema)) as in_schema:
                    self.__schemas[schema.split("_")[1].replace(".schema", "")] = json.load(in_schema)
        except Exception as e:
            raise

        os.makedirs(PATH_FEEDSTOCK, exist_ok=True)

        res = self.__write_metadata(metadata, resource_type)
        if not res["success"]:
            raise ValueError("Invalid metadata: '" + res["message"] + "'\n" + res.get("details", ""))
        else:
            self.__initialized = True

        # If the metadata is a repository, cache the mdf_id and close out the feedstock file
        if resource_type == "repository":
            self.flush()
            self.__feedstock.close()
            if os.path.exists(PATH_REPO_CACHE):
                with open(PATH_REPO_CACHE) as repo_cache:
                    cache = json.load(repo_cache)
            else:
                cache = {}
            cache[self.__source_name] = {
                "mdf_id": self.__parent_id,
                "title": self.__parent_title
                }
            with open(PATH_REPO_CACHE, 'w') as repo_cache:
                json.dump(cache, repo_cache)


    #del attempts cleanup
    def __del__(self):
        try:
            self.__feedstock.close()
        except (AttributeError, NameError): #Feedstock wasn't opened
            pass

    #Sets metadata
    def __write_metadata(self, full_metadata, resource_type):
        if self.__initialized: #Metadata already set; cannot change
            return {
                "success": False,
                "message": "Metadata already written for this " + resource_type
                }

        if resource_type not in self.__schemas.keys():
            return {
                "success": False,
                "message": "No validation schema found for '" + resource_type
                }

        metadata = full_metadata.get("mdf", {})

        # Validator-added fields
        # mdf_id
        self.__parent_id = str(ObjectId())
        metadata["mdf_id"] = self.__parent_id

        # resource_type
        metadata["resource_type"] = resource_type

        # metadata_version
        metadata["metadata_version"] = self.__version

        # ingest_date
        metadata["ingest_date"] = datetime.utcnow().isoformat("T") + "Z"


        if type(metadata.get("title", None)) is str:
            self.__parent_title = metadata["title"]


        # Convenience processing
        # acl
        if metadata.get("acl", None) == "public":
            metadata["acl"] = ["public"]

        # citation
        if type(metadata.get("citation", None)) is str:
            metadata["citation"] = [metadata["citation"]]

        # author
        if type(metadata.get("author", None)) is dict:
            metadata["author"] = [metadata["author"]]
        # author.full_name
        authors = []
        for auth in metadata.get("author", []):
            if type(auth) is dict:
                auth["full_name"] = auth.get("given_name", "") + " " + auth.get("family_name", "")
                authors.append(auth)
        if authors:
            metadata["author"] = authors

        # repository
        repo = metadata.get("repository", None)
        if type(repo) is str:
            with open(PATH_REPO_CACHE) as repo_cache:
                cache = json.load(repo_cache)
            # Check if repo name has an id
            if cache.get(repo, None) is not None:
                metadata["links"]["parent_id"] = cache[repo]["mdf_id"]
            # Check if repository is an id
            elif repo in [r["mdf_id"] for r in cache.values()]:
                metadata["links"]["parent_id"] = repo
            # Check if repository is a title
            else:
                for sn, r in cache.items():
                    if repo.lower() == r["title"].lower():
                        metadata["links"]["parent_id"] = cache[sn]["mdf_id"]
                        break

        # tags
        if type(metadata.get("tags", None)) is str:
            metadata["tags"] = [metadata["tags"]]
        elif not metadata.get("tags", None):
            metadata["tags"] = []
        metadata["tags"] += [fmt for fmt in metadata.get("links", {}).keys() if fmt not in ["landing_page", "publication", "data_doi", "related_id"]]

        # publication
        if type(metadata.get("links", {}).get("publication", None)) is str:
            metadata["links"]["publication"] = [metadata["links"]["publication"]]

        # related_id
        if type(metadata.get("links", {}).get("related_id", None)) is str:
            metadata["links"]["related_id"] = [metadata["links"]["related_id"]]

        # data_contact
        if type(metadata.get("data_contact", None)) is dict:
            metadata["data_contact"]["full_name"] = metadata["data_contact"].get("given_name", "") + " " + metadata["data_contact"].get("family_name", "")

        # data_contributor
        if type(metadata.get("data_contributor", None)) is dict:
            metadata["data_contributor"] = [metadata["data_contributor"]]
        # data_contributor.full_name
        contribs = []
        for contrib in metadata.get("data_contributor", []):
            if type(contrib) is dict:
                contrib["full_name"] = contrib.get("given_name", "") + " " + contrib.get("family_name", "")
                contribs.append(contrib)
        if contribs:
            metadata["data_contributor"] = contribs


        # Log fields to copy to records
        # source_name
        src_nm = ""
        for char in metadata.get("source_name", "").lower().replace(" ", "_").replace("-", "_"):
            if char.isalnum() or char == "_":
                src_nm += char
        self.__source_name = src_nm
        metadata["source_name"] = self.__source_name

        # acl
        self.__acl = metadata.get("acl", None)

        # collection
        self.__collection = metadata.get("collection", None)

        # landing_page
        self.__landing_pages.append(metadata.get("links", {}).get("landing_page", None))


        # Finish mdf processing
        full_metadata["mdf"] = metadata


        # Process other blocks
        # MRR
        mrr = full_metadata.get("mrr", {})
        full_metadata["mrr"] = mrr


        # DC
        dc = full_metadata.get("dc", {})
        full_metadata["dc"] = dc


        # Validate metadata
        try:
            jsonschema.validate(full_metadata, self.__schemas[resource_type])
            # Validate user-added block
            # If it exists, the key must be the source_name
            if len(full_metadata) > 3 and full_metadata.get(self.__source_name, None) is None:
                raise(jsonschema.ValidationError("The user-defined data block for source name '" + self.__source_name + "' must be named '" + self.__source_name + "'"))
        except jsonschema.ValidationError as e:
           return {
                "success": False,
                "message": "Invalid metadata: " + str(e).split("\n")[0],
                "details": str(e)
                }


        # Open feedstock file for the first time and write metadata entry
        feedstock_path = os.path.join(PATH_FEEDSTOCK,  self.__source_name + "_all.json")
        try:
            self.__feedstock = open(feedstock_path, 'w')
            json.dump(full_metadata, self.__feedstock)
            self.__feedstock.write("\n")
            self.__feedstock.flush()
            return {
                "success": True
                }

        except Exception as e:
            return {
                "success": False,
                "message": "Error: Bad metadata: " + repr(e)
                }


    # Output single record to feedstock
    def write_record(self, full_record, resource_type="record"):
        if not self.__initialized or self.__feedstock.closed: #Metadata not set, or cancelled
            return {
                "success": False,
                "message": "Metadata not written for this dataset"
                }

        if resource_type not in self.__schemas.keys():
            return {
                "success": False,
                "message": "No validation schema found for '" + resource_type
                }

        record = full_record.get("mdf", {})


        # Validator-added fields
        # mdf_id
        record["mdf_id"] = str(ObjectId())

        # resource_type
        record["resource_type"] = resource_type

        # metadata_version
        record["metadata_version"] = self.__version

        # ingest_date
        record["ingest_date"] = datetime.utcnow().isoformat("T") + "Z"

        # source_name
        record["source_name"] = self.__source_name


        # Convenience processing
        # acl
        if record.get("acl", None) == "public":
            record["acl"] = ["public"]

        # citation
        if type(record.get("citation", [])) is str:
            record["citation"] = [record["citation"]]

        # author
        if type(record.get("author", None)) is dict:
            record["author"] = [record["author"]]
        # author.full_name
        authors = []
        for auth in record.get("author", []):
            if type(auth) is dict:
                auth["full_name"] = auth.get("given_name", "") + " " + auth.get("family_name", "")
                authors.append(auth)
        if authors:
            record["author"] = authors


        # tags
        if type(record.get("tags", None)) is str:
            record["tags"] = [record["tags"]]
        elif not record.get("tags", None):
            record["tags"] = []
        record["tags"] += [fmt for fmt in record.get("links", {}).keys() if fmt not in ["landing_page", "publication", "data_doi", "related_id", "parent_id"]]


        # publication
        if type(record.get("links", {}).get("publication", None)) is str:
            record["links"]["publication"] = [record["links"]["publication"]]

        # related_id
        if type(record.get("links", {}).get("related_id", None)) is str:
            record["links"]["related_id"] = [record["links"]["related_id"]]


        # Copy missing fields
        # acl
        if not record.get("acl") and self.__acl:
            record["acl"] = self.__acl

        # collection
        if not record.get("collection") and self.__collection:
            record["collection"] = self.__collection


        # Fields requiring special processing
        # links
        record_links = record.get("links", {})
        # parent_id
        record_links["parent_id"] = self.__parent_id
        # landing_page
        if not record_links.get("landing_page", None):
            record_links["landing_page"] = self.__landing_pages[0]
        if record_links.get("landing_page") in self.__landing_pages:
            record_links["landing_page"] += "#" + str(self.__scroll_id)
        self.__landing_pages.append(record_links.get("landing_page"))
        record["links"] = record_links

        # elements
        if record.get("composition", None):
            composition = record["composition"].replace(" and ", "")
            for element in DICT_OF_ALL_ELEMENTS.keys():
                composition = re.sub("(?i)"+element, DICT_OF_ALL_ELEMENTS[element], composition)
            str_of_elem = ""
            for char in list(composition):
                if char.isupper(): #Uppercase indicates start of new element symbol
                    str_of_elem += " " + char
                elif char.islower(): #Lowercase indicates continuation of symbol
                    str_of_elem += char
                #Anything else is not useful (numbers, whitespace, etc.)

            list_of_elem = list(set(str_of_elem.split())) #split elements in string (on whitespace), make unique, make JSON-serializable
            # If any "element" isn't in the periodic table, the entire composition is likely not a chemical formula and should not be parsed
            if all([elem in DICT_OF_ALL_ELEMENTS.values() for elem in list_of_elem]):
                record["elements"] = list_of_elem

################################
        record["scroll_id"] = self.__scroll_id
        self.__scroll_id += 1


        # Finish mdf processing
        full_record["mdf"] = record


        # Process other blocks
        # DC
        dc = full_record.get("dc", {})
        full_record["dc"] = dc


        # Validate metadata
        try:
            jsonschema.validate(full_record, self.__schemas[resource_type])
            # Validate user-added block
            # If it exists, the key must be the source_name
            if len(full_record) > 2 and full_record.get(self.__source_name, None) is None:
                raise(jsonschema.ValidationError("The user-defined data block for source name '" + self.__source_name + "' must be named '" + self.__source_name + "'"))
        except jsonschema.ValidationError as e:
           return {
                "success": False,
                "message": "Invalid metadata: " + str(e).split("\n")[0],
                "details": str(e)
                }


        # Write new record to feedstock
        try:
            json.dump(full_record, self.__feedstock)
            self.__feedstock.write("\n")
            return {
                "success" : True
                }
        except Exception as e:
            return {
                "success": False,
                "message": "Error: Bad record: " + repr(e)
                }


    # Cancels validation and cleans up partial feedstock file
    def cancel_validation(self):
        if not self.__initialized:
            return {
                "success": False,
                "message": "Validator not initialized"
                }
        elif self.__feedstock.closed:
            return {
                "success": False,
                "message": "Validation already cancelled"
                }
        try:
            self.__feedstock.close()
        except Exception as e:
            return {
                "success": False,
                "message": "Unable to close feedstock file",
                "details": repr(e)
                }
        try:
            feedstock_path = os.path.join(PATH_FEEDSTOCK,  self.__source_name + "_all.json")
            if not os.path.isfile(feedstock_path):
                raise IOError("Feedstock file is missing or corrupted.")
            os.remove(feedstock_path)
        except Exception as e:
            return {
                "success": False,
                "message": "Failed to delete feedstock file",
                "details": repr(e)
                }
        return {
            "success": True
            }


    # Flushes output
    def flush(self):
        self.__feedstock.flush()
        return True


    @property
    def dataset_id(self):
        return self.__dataset_id

if __name__ == "__main__":
    print("\nThis is the Validator. You can use the Validator to write valid, converted data into feedstock.")
    print("There are in-depth instructions on this process in 'forge/mdf-indexers/converters/converter_template.py'.")
