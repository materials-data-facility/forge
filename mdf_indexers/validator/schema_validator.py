import json
import os
import re
from copy import deepcopy
from datetime import datetime
import jsonschema
from bson import ObjectId

from ..utils import paths

PATH_FEEDSTOCK = paths.get_path(__file__, "feedstock")
PATH_SCHEMAS = paths.get_path(__file__, "schemas")

##################
VALIDATOR_VERSION = "0.2.x"
##################

DICT_OF_ALL_ELEMENTS = {"Actinium": "Ac", "Silver": "Ag", "Aluminum": "Al", "Americium": "Am", "Argon": "Ar", "Arsenic": "As", "Astatine": "At", "Gold": "Au", "Boron": "B", "Barium": "Ba", "Beryllium": "Be", "Bohrium": "Bh", "Bismuth": "Bi", "Berkelium": "Bk", "Bromine": "Br", "Carbon": "C", "Calcium": "Ca", "Cadmium": "Cd", "Cerium": "Ce", "Californium": "Cf", "Chlorine": "Cl", "Curium": "Cm", "Copernicium": "Cn", "Cobalt": "Co", "Chromium": "Cr", "Cesium": "Cs", "Copper": "Cu", "Dubnium": "Db", "Darmstadtium": "Ds", "Dysprosium": "Dy", "Erbium": "Er", "Einsteinium": "Es", "Europium": "Eu", "Fluorine": "F", "Iron": "Fe", "Flerovium": "Fl", "Fermium": "Fm", "Francium": "Fr", "Gallium": "Ga", "Gadolinium": "Gd", "Germanium": "Ge", "Hydrogen": "H", "Helium": "He", "Hafnium": "Hf", "Mercury": "Hg", "Holmium": "Ho", "Hassium": "Hs", "Iodine": "I", "Indium": "In", "Iridium": "Ir", "Potassium": "K", "Krypton": "Kr", "Lanthanum": "La", "Lithium": "Li", "Lawrencium": "Lr", "Lutetium": "Lu", "Livermorium": "Lv", "Mendelevium": "Md", "Magnesium": "Mg", "Manganese": "Mn", "Molybdenum": "Mo", "Meitnerium": "Mt", "Nitrogen": "N", "Sodium": "Na", "Niobium": "Nb", "Neodymium": "Nd", "Neon": "Ne", "Nickel": "Ni", "Nobelium": "No", "Neptunium": "Np", "Oxygen": "O", "Osmium": "Os", "Phosphorus": "P", "Protactinium": "Pa", "Lead": "Pb", "Palladium": "Pd", "Promethium": "Pm", "Polonium": "Po", "Praseodymium": "Pr", "Platinum": "Pt", "Plutonium": "Pu", "Radium": "Ra", "Rubidium": "Rb", "Rhenium": "Re", "Rutherfordium": "Rf", "Roentgenium": "Rg", "Rhodium": "Rh", "Radon": "Rn", "Ruthenium": "Ru", "Sulfur": "S", "Antimony": "Sb", "Scandium": "Sc", "Selenium": "Se", "Seaborgium": "Sg", "Silicon": "Si", "Samarium": "Sm", "Tin": "Sn", "Strontium": "Sr", "Tantalum": "Ta", "Terbium": "Tb", "Technetium": "Tc", "Tellurium": "Te", "Thorium": "Th", "Titanium": "Ti", "Thallium": "Tl", "Thulium": "Tm", "Uranium": "U", "Ununoctium": "Uuo", "Ununpentium": "Uup", "Ununseptium": "Uus", "Ununtrium": "Uut", "Vanadium": "V", "Tungsten": "W", "Xenon": "Xe", "Yttrium": "Y", "Ytterbium": "Yb", "Zinc": "Zn", "Zirconium": "Zr"}

MAX_KEYS = 20
MAX_LIST = 5


#Validator class holds data about a dataset while writing to feedstock
class Validator:
    #init takes dataset metadata to start processing and save another function call
    def __init__(self, metadata=None, node_type="dataset", version=VALIDATOR_VERSION):
        if not metadata:
            raise ValueError("You must specify the metadata for this " + node_type)
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
            high_ver = max([int(ver) for ver in poss_vers])
            version = version.replace("x", str(high_ver))

        self.__version = version

        try:
            for schema in [s for s in schema_items if s.startswith(version)]:
                with open(os.path.join(PATH_SCHEMAS, schema)) as in_schema:
                    self.__schemas[schema.split("_")[1].replace(".schema", "")] = json.load(in_schema)
        except Exception as e:
            raise

        res = self.__write_metadata(metadata, node_type)
        if not res["success"]:
            raise ValueError("Invalid metadata: '" + res["message"] + "'\n" + res.get("details", ""))
        else:
            self.__initialized = True

    #del attempts cleanup
    def __del__(self):
        try:
            self.__feedstock.close()
        except (AttributeError, NameError): #Feedstock wasn't opened
            pass

    #Sets metadata
    def __write_metadata(self, metadata, node_type):
        if self.__initialized: #Metadata already set; cannot change
            return {
                "success": False,
                "message": "Metadata already written for this " + node_type
                }

        if node_type not in self.__schemas.keys():
            return {
                "success": False,
                "message": "No validation schema found for '" + node_type
                }

        # Validator-added fields
        # mdf-id
        self.__parent_id = str(ObjectId())
        metadata["mdf-id"] = self.__parent_id

        # mdf-node_type
        metadata["mdf-node_type"] = node_type

        # mdf-metadata_version
        metadata["mdf-metadata_version"] = self.__version

        # mdf-ingest_date
        metadata["mdf-ingest_date"] = datetime.utcnow().isoformat("T") + "Z"


        # Convenience processing
        # mdf-acl
        if metadata.get("mdf-acl", None) == "public":
            metadata["mdf-acl"] = ["public"]

        # mdf-citation
        if type(metadata.get("mdf-citation", None)) is str:
            metadata["mdf-citation"] = [metadata["mdf-citation"]]

        # mdf-author
        if type(metadata.get("mdf-author", None)) is dict:
            metadata["mdf-author"] = [metadata["mdf-author"]]

        # mdf-data_format
        if type(metadata.get("mdf-data_format", None)) is str:
            metadata["mdf-data_format"] = [metadata["mdf-data_format"]]

        # mdf-data_type
        if type(metadata.get("mdf-data_type", None)) is str:
            metadata["mdf-data_type"] = [metadata["mdf-data_type"]]

        # mdf-tags
        if type(metadata.get("mdf-tags", None)) is str:
            metadata["mdf-tags"] = [metadata["mdf-tags"]]

        # mdf-publication
        if type(metadata.get("mdf-links", {}).get("mdf-publication", None)) is str:
            metadata["mdf-links"]["mdf-publication"] = [metadata["mdf-links"]["mdf-publication"]]

        # mdf-related_id
        if type(metadata.get("mdf-links", {}).get("mdf-related_id", None)) is str:
            metadata["mdf-links"]["mdf-related_id"] = [metadata["mdf-links"]["mdf-related_id"]]

        # mdf-data_contributor
        if type(metadata.get("mdf-data_contributor", None)) is dict:
            metadata["mdf-data_contributor"] = [metadata["mdf-data_contributor"]]


        # Validate metadata
        try:
            jsonschema.validate(metadata, self.__schemas[node_type])
        except jsonschema.ValidationError as e:
           return {
                "success": False,
                "message": "Invalid metadata: " + str(e).split("\n")[0],
                "details": str(e)
                }


        # Log fields to copy to records
        # mdf-source_name
        self.__source_name = metadata.get("mdf-source_name", "").lower().replace(" ", "_")
        metadata["mdf-source_name"] = self.__source_name

        # mdf-acl
        self.__acl = metadata.get("mdf-acl", None)

        # mdf-collection
        self.__collection = metadata.get("mdf-collection", None)
        
        # mdf-data_format
        self.__data_format = metadata.get("mdf-data_format", None)
        
        # mdf-data_type
        self.__data_type = metadata.get("mdf-data_type", None)
        
        # mdf-citation
        self.__citation = metadata.get("mdf-citation", None)

        # mdf-license
        self.__license = metadata.get("mdf-license", None)
        
        # mdf-author
        self.__author = metadata.get("mdf-author", None)
        
        # mdf-data_contact
        self.__data_contact = metadata.get("mdf-data_contact", None)
        
        # mdf-tags
        self.__tags = metadata.get("mdf-tags", None)

        # mdf-links
        self.__links = metadata.get("mdf-links", None)

        # mdf-landing_page
        self.__landing_pages.append(metadata.get("mdf-links", {}).get("mdf-landing_page", None))

        # mdf-year
        self.__year = metadata.get("mdf-year", None)

        # mdf-data_contributor
        self.__data_contributor = metadata.get("mdf-data_contributor", None)


        # Namespace user-supplied fields
        for field in [f for f in metadata.keys() if not f.startswith("mdf")]:
            metadata[self.__source_name + "-" + field] = metadata.pop(field)


        # Open feedstock file for the first time and write metadata entry
        feedstock_path = os.path.join(PATH_FEEDSTOCK,  self.__source_name + "_all.json")
        try:
            self.__feedstock = open(feedstock_path, 'w')
            json.dump(metadata, self.__feedstock)
            self.__feedstock.write("\n")
            return {
                "success": True
                }

        except Exception as e:
            return {
                "success": False,
                "message": "Error: Bad metadata: " + repr(e)
                }


    # Output single record to feedstock
    def write_record(self, record):
        node_type = "record"
        if not self.__initialized or self.__feedstock.closed: #Metadata not set, or cancelled
            return {
                "success": False,
                "message": "Metadata not written for this dataset"
                }

        if node_type not in self.__schemas.keys():
            return {
                "success": False,
                "message": "No validation schema found for '" + node_type
                }


        # Validator-added fields
        # mdf-id
        record["mdf-id"] = str(ObjectId())

        # mdf-node_type
        record["mdf-node_type"] = node_type

        # mdf-metadata_version
        record["mdf-metadata_version"] = self.__version

        # mdf-ingest_date
        record["mdf-ingest_date"] = datetime.utcnow().isoformat("T") + "Z"

        # mdf-source_name
        record["mdf-source_name"] = self.__source_name

        # mdf-data_contributor
        if self.__data_contributor:
            record["mdf-data_contributor"] = self.__data_contributor


        # Convenience processing
        # mdf-acl
        if record.get("mdf-acl", None) == "public":
            record["mdf-acl"] = ["public"]

        # mdf-citation
        if type(record.get("mdf-citation", [])) is str:
            record["mdf-citation"] = [record["mdf-citation"]]

        # mdf-author
        if type(record.get("mdf-author", None)) is dict:
            record["mdf-author"] = [record["mdf-author"]]

        # mdf-data_format
        if type(record.get("mdf-data_format", None)) is str:
            record["mdf-data_format"] = [record["mdf-data_format"]]

        # mdf-data_type
        if type(record.get("mdf-data_type", None)) is str:
            record["mdf-data_type"] = [record["mdf-data_type"]]

        # mdf-tags
        if type(record.get("mdf-tags", None)) is str:
            record["mdf-tags"] = [record["mdf-tags"]]

        # mdf-publication
        if type(record.get("mdf-links", {}).get("mdf-publication", None)) is str:
            record["mdf-links"]["mdf-publication"] = [record["mdf-links"]["mdf-publication"]]

        # mdf-related_id
        if type(record.get("mdf-links", {}).get("mdf-related_id", None)) is str:
            record["mdf-links"]["mdf-related_id"] = [record["mdf-links"]["mdf-related_id"]]


        # Copy missing fields
        # mdf-acl
        if not record.get("mdf-acl") and self.__acl:
            record["mdf-acl"] = self.__acl

        # mdf-collection
        if not record.get("mdf-collection") and self.__collection:
            record["mdf-collection"] = self.__collection

        # mdf-data_format
        if not record.get("mdf-data_format") and self.__data_format:
            record["mdf-data_format"] = self.__data_format

        # mdf-data_type
        if not record.get("mdf-data_type") and self.__data_type:
            record["mdf-data_type"] = self.__data_type

        # mdf-citation
        if not record.get("mdf-citation") and self.__citation:
            record["mdf-citation"] = self.__citation

        # mdf-license
        if not record.get("mdf-license") and self.__license:
            record["mdf-license"] = self.__license

        # mdf-author
        if not record.get("mdf-author") and self.__author:
            record["mdf-author"] = self.__author

        # mdf-data_contact
        if not record.get("mdf-data_contact") and self.__data_contact:
            record["mdf-data_contact"] = self.__data_contact

        # mdf-year
        if not record.get("mdf-year") and self.__year:
            record["mdf-year"] = self.__year


        # Fields requiring special processing
        # mdf-tags (combine dataset and record)
        if self.__tags:
            record["mdf-tags"] = list(set(record.get("mdf-tags", []) + self.__tags))

        # mdf-links
        record_links = deepcopy(self.__links) or {}
        record_links.update(record.get("mdf-links", {}))
        # mdf-parent_id
        record_links["mdf-parent_id"] = self.__parent_id
        # mdf-landing_page
        if not record_links.get("mdf-landing_page", None):
            record_links["mdf-landing_page"] = self.__landing_pages[0]
        if record_links.get("mdf-landing_page") in self.__landing_pages:
            record_links["mdf-landing_page"] += "#" + str(self.__scroll_id)
        self.__landing_pages.append(record_links.get("mdf-landing_page"))
        record["mdf-links"] = record_links or None

        # mdf-elements
        if record.get("mdf-composition", None):
            composition = record["mdf-composition"].replace(" and ", "")
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
                record["mdf-elements"] = list_of_elem

################################
        record["mdf-scroll_id"] = self.__scroll_id
        self.__scroll_id += 1


        # Validate metadata
        try:
            jsonschema.validate(record, self.__schemas[node_type])
        except jsonschema.ValidationError as e:
           return {
                "success": False,
                "message": "Invalid metadata: " + str(e).split("\n")[0],
                "details": str(e)
                }


        # Namespace user-supplied fields
        for field in [f for f in record.keys() if not f.startswith("mdf")]:
            record[self.__source_name + "-" + field] = record.pop(field)


        # Write new record to feedstock
        try:
            json.dump(record, self.__feedstock)
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


    @property
    def dataset_id(self):
        return self.__dataset_id

if __name__ == "__main__":
    print("\nThis is the Validator. You can use the Validator to write valid, converted data into feedstock.")
    print("There are in-depth instructions on this process in 'mdf-harvesters/converters/converter_template.py'.")
