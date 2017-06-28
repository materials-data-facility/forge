
# coding: utf-8

# # Setup

import json


# # Recursively format JSON Schema into a Python template

def format_md(md, indent=""):
    if type(md) is dict:
        if "$schema" in md.keys():  # Top-level
            global DEFINITIONS
            DEFINITIONS = md["definitions"]
            return format_md(md["properties"], indent)
        clean = ""
        tab = "    "
        for key in md.keys():
            data = md[key]
            req_lv, desc = data["description"].split(":", 1)
            if req_lv not in ["INTERNAL", "Undefined"]:
                clean += indent + tab
                if "$ref" in data.keys() or data["type"] == "object":
                    prop_loc = DEFINITIONS[data["$ref"].rsplit("/", 1)[1]] if "$ref" in data.keys() else data
                    clean += "# " + req_lv + " " + "dictionary" + ":" + desc
                    clean += '\n' + indent + tab + '"' + key + '": {\n\n'
                    clean += format_md(prop_loc["properties"], indent+tab)
                    if "$ref" in prop_loc.get("additionalProperties", {}).keys():
                        add_name = prop_loc["additionalProperties"]["$ref"].rsplit("/", 1)[1]
                        add_def = DEFINITIONS[add_name]
                        add_req_lv, add_desc = add_def["description"].split(":", 1)
                        clean += indent + tab*2 + "# " + add_req_lv + " " + "dictionary" + ":" + add_desc
                        clean += '\n' + indent + tab*2 + '"' + add_name + '": {\n\n'
                        clean += format_md(add_def["properties"], indent+tab*2)
                        clean += indent + tab*2 + "},\n\n"
                    clean += indent + tab + "},\n\n"
 
                elif data["type"] == "array":
                    clean += "# " + req_lv + " list of " + data["items"].get("type", "dictionarie") + "s:" + desc
                    clean += '\n' + indent + tab + '"' + key + '": '

                    if "$ref" in data["items"].keys():
                        clean += '[{\n\n'
                        clean += format_md(DEFINITIONS[data["items"]["$ref"].rsplit("/", 1)[1]]["properties"], indent+tab)
                        clean += '\n' + indent + tab + '}]'

                    clean += ",\n\n"
                    
                else:  # Non-container type
                    clean += "# " + req_lv + " " + data["type"] + ":" + desc
                    clean += '\n' + indent + tab + '"' + key + '": ,\n\n'
                    
                
        return clean
    else:
        raise TypeError("Not a dict")


# # Inject template into appropriate script

def inject_md(input_file, schema, md_type, version, indent="    "):
    start_flag = "## Metadata:" + md_type
    end_flag= "## End metadata"
    doc = ""
    pause = False
    for line in input_file:
        if "# VERSION" in line:
            doc += "# VERSION " + version + "\n"
        elif pause:
            if end_flag in line:
                doc += line
                pause = False
        elif start_flag in line:
            doc += line
            base_indent = line.split(start_flag)[0]
            doc += base_indent + md_type + "_metadata = {\n"
            doc += format_md(schema, base_indent)
            doc += "\n" + base_indent + "}\n"
            pause = True
        else:
            doc += line
    return doc


# # Save new template

def generate_template(md_type, md_version, template_file=None):
    with open(md_type+"_"+md_version+".schema") as schema_file:
        schema = json.load(schema_file)
    if not template_file:
        import os
        from ..utils import paths
        path_schemas = paths.get_path(__file__, "converters")
        template_file = os.path.join(paths_schemas, "converter_template.py")
        
    with open(template_file, "r") as input_file:
        new_template = inject_md(input_file, schema, md_type, md_version)
    with open(template_file, "w") as output_file:
        output_file.write(new_template)

    return {"success": True}

