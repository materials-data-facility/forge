
# coding: utf-8

import json
import os


# # Save new schemas in appropriate files

def write_schema(schema, md_type, md_version):
    schema_loc = "."
    conv_template_loc = "./converter_template.py"
    text_doc_loc = "./schema_doc.txt"

    # Write out to .schema
    with open(os.path.join(schema_loc, md_version + "_" + md_type + ".schema"), 'w') as md_file:
        json.dump(schema, md_file)

    # Inject into converter template
    with open(conv_template_loc, "r") as input_file:
        new_template = inject_md(input_file, schema, md_type, md_version)
    with open(conv_template_loc, "w") as output_file:
        output_file.write(new_template)

    # Inject into docs
    with open(text_doc_loc, "r") as input_file:
        new_template = format_docs(inject_md(input_file, schema, md_type, md_version), md_type)
    with open(text_doc_loc, "w") as output_file:
        output_file.write(new_template)

    return {"success": True}


# # Recursively format JSON Schema into a Python template

def format_md(md, indent=""):
    if type(md) is dict:
        if "$schema" in md.keys():  # Top-level
            # Need to save definitions in a global for recursion
            global DEFINITIONS
            DEFINITIONS = md["definitions"]
            return format_md(md["properties"], indent)
        clean = ""
        tab = "    "
        for key in md.keys():
            data = md[key]
            # Description in format "REQ: Desc"
            req_lv, desc = data["description"].split(":", 1)
            # INTERNAL marks fields users are not allowed to supply
            # Undefined marks fields that are not yet useful
            if req_lv not in ["INTERNAL", "Undefined"]:
                clean += indent + tab
                # All $ref fields point to objects (assumption)
                if "$ref" in data.keys() or data["type"] == "object":
                    # Expand $ref if present
                    prop_loc = DEFINITIONS[data["$ref"].rsplit("/", 1)[1]] if "$ref" in data.keys() else data
                    clean += "# " + req_lv + " " + "dictionary" + ":" + desc
                    clean += '\n' + indent + tab + '"' + key + '": {\n\n'
                    clean += format_md(prop_loc["properties"], indent+tab)

                    # Process $ref in additionalProperties
                    if type(prop_loc.get("additionalProperties", None)) is dict and "$ref" in prop_loc.get("additionalProperties", {}).keys():
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
                    
                    # Expand $ref
                    if "$ref" in data["items"].keys():
                        clean += '[{\n\n'
                        clean += format_md(DEFINITIONS[data["items"]["$ref"].rsplit("/", 1)[1]]["properties"], indent+tab)
                        clean += '\n' + indent + tab + '}]'

                    clean += ",\n\n"
                    
                else:
                    # Non-container types do not have further data inside
                    clean += "# " + req_lv + " " + data["type"] + ":" + desc
                    clean += '\n' + indent + tab + '"' + key + '": ,\n\n'
                
        return clean
    else:
        raise TypeError("Invalid JSON Schema")


# # Inject template into appropriate script

def inject_md(input_file, schema, md_type, version, indent="    "):
    # Overwrite doc from start flag to end flag (exclusive) with new template
    start_flag = "## Metadata:" + md_type
    end_flag= "## End metadata"
    doc = ""
    pause = False
    for line in input_file:
        # Update version number
        if "# VERSION" in line:
            doc += "# VERSION " + version + "\n"
        # If pause was set, template has been written
        # Ignore everything until the end flag to overwrite old template
        elif pause:
            if end_flag in line:
                doc += line
                pause = False
        # Add new template after start flag
        elif start_flag in line:
            doc += line
            base_indent = line.split(start_flag)[0]
            doc += base_indent + md_type + "_metadata = {\n"
            doc += format_md(schema, base_indent)
            doc += "\n" + base_indent + "}\n"
            pause = True
        # Everything that isn't the version or the template should not be altered
        else:
            doc += line
    return doc


# Format template into documentation
def format_docs(template, md_type):
    # Overwrite doc from start flag to end flag (exclusive) with docs
    start_flag = "## Metadata:" + md_type
    end_flag= "## End metadata"
    overwrite = False
    doc = ""
    info = ""
    field = ""
    indent = ""
    for line in template.split("\n"):
        if overwrite:
            # Line has end flag
            if end_flag in line:
                doc += line + "\n"
                overwrite = False
            # Line has comment
            elif "#" in line:
                # Check that info is not already added
                if info:
                    raise ValueError("Info already added for '" + line + "'")
                indent = line.split("#")[0]
                info = line.strip().strip("# ")
            # After info line is always field placeholder
            elif info and not field:
                # Field name is always in between the first two double-quotes
                field = line.split("\"")[1]
                # If we have the info and field, create documentation
                if info and field:
                    doc += indent + field + ": " + info + "\n\n"
                    info = ""
                    field = ""
                    indent = ""
                # Otherwise, something is wrong
                else:
                    raise ValueError("Field not found for '" + line + "'")
            # Else, line has nothing to process, should not be copied
        else:
            doc += line + "\n"
            if start_flag in line:
                overwrite = True

    return doc.strip()

