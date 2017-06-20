import json
import sys
from parsers.tab_parser import parse_tab
from tqdm import tqdm
from validator import Validator

# VERSION 0.1.0

# This is the convreter for the QSAR biodegradation Data Set 
# Arguments:
#   input_path (string): The file or directory where the data resides. This should not be hard-coded in the function, for portability.
#   metadata (string or dict): The path to the JSON dataset metadata file, a dict or json.dumps string containing the dataset metadata, or None to specify the metadata here. Default None.
#   verbose (bool): Should the script print status messages to standard output? Default False.
#       NOTE: The converter should have NO output if verbose is False, unless there is an error.
def convert(input_path, metadata=None, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    if not metadata:
        dataset_metadata = {
            "globus_subject": "https://archive.ics.uci.edu/ml/datasets/QSAR+biodegradation",
            "acl": ["public"],
            "mdf_source_name": "qsar_biodeg",
            "mdf-publish.publication.collection": "QSAR Biodegradation Data Set",
            "mdf_data_class": "csv",

            "cite_as": ["Mansouri, K., Ringsted, T., Ballabio, D., Todeschini, R., Consonni, V. (2013). Quantitative Structure - Activity Relationship models for ready biodegradability of chemicals. Journal of Chemical Information and Modeling, 53, 867-878", "Lichman, M. (2013). UCI Machine Learning Repository [http://archive.ics.uci.edu/ml]. Irvine, CA: University of California, School of Information and Computer Science."],
#            "license": ,
            "mdf_version": "0.1.0",

            "dc.title": "QSAR biodegradation Data Set",
            "dc.creator": "Milano Chemometrics and QSAR Research Group",
            "dc.identifier": "https://archive.ics.uci.edu/ml/machine-learning-databases/00254/",
            "dc.contributor.author": ["Mansouri, K.", "Ringsted, T.", "Ballabio, D.", "Todeschini, R.", "Consonni, V."],
#            "dc.subject": ,
            "dc.description": "Data set containing values for 41 attributes (molecular descriptors) used to classify 1055 chemicals into 2 classes (ready and not ready biodegradable).",
#            "dc.relatedidentifier": ,
            "dc.year": 2013
            }
    elif type(metadata) is str:
        try:
            dataset_metadata = json.loads(metadata)
        except Exception:
            try:
                with open(metadata, 'r') as metadata_file:
                    dataset_metadata = json.load(metadata_file)
            except Exception as e:
                sys.exit("Error: Unable to read metadata: " + repr(e))
    elif type(metadata) is dict:
        dataset_metadata = metadata
    else:
        sys.exit("Error: Invalid metadata parameter")



    # Make a Validator to help write the feedstock
    # You must pass the metadata to the constructor
    # Each Validator instance can only be used for a single dataset
    #dataset_validator = Validator(dataset_metadata, strict=False)
    # You can also force the Validator to treat warnings as errors with strict=True
    dataset_validator = Validator(dataset_metadata, strict=True)


    # Get the data
    #    Each record should be exactly one dictionary
    #    It is recommended that you convert your records one at a time, but it is possible to put them all into one big list (see below)
    #    It is also recommended that you use a parser to help with this process if one is available for your datatype
    i=1
    headers = ["SpMax_L", "J_Dz(e)", "nHM", "F01[N-N]", "F04[C-N]", "NssssC", "nCb-", "C%", "nCp", "nO", "F03[C-N]", "SdssC", "HyWi_B(m)", "LOC", " SM6_L", "F03[C-O]", "Me", "Mi", "nN-N", "nArNO2", "nCRX3", "SpPosA_B(p)", "nCIR", "B01[C-Br]", "B03[C-Cl]", "N-073", "SpMax_A", "Psi_i_1d", "B04[C-Br]", "SdO", "TI2_L", "nCrt", "C-026", "F02[C-N]", "nHDon", "SpMax_B(m)", "Psi_i_A", "nN", "SM6_B(m)", " nArCOOR", "nX", "experimental class"]
    # Each record also needs its own metadata sep=";"
    with open(input_path, 'r') as raw_in:
        for row_data in tqdm(parse_tab(raw_in.read(), sep=";", headers=headers), desc="Processing data", disable=not verbose):
            record = []
            for key, value in row_data.items():
                record.append(key+": "+value)
            uri = "https://data.materialsdatafacility.org/collections/" + "qsar_biodeg/biodeg.csv"
            record_metadata = {
                "globus_subject": uri + "#" + str(i),
                "acl": ["public"],
    #            "mdf-publish.publication.collection": ,
    #            "mdf_data_class": ,
    #            "mdf-base.material_composition": ,
    
    #            "cite_as": ,
    #            "license": ,
    
                "dc.title": "qsar_biodeg - " + "record: " + str(i),
    #            "dc.creator": ,
    #            "dc.identifier": ,
    #            "dc.contributor.author": ,
    #            "dc.subject": ,
    #            "dc.description": ,
    #            "dc.relatedidentifier": ,
    #            "dc.year": ,
    
                "data": {
                    "raw": json.dumps(record),
    #                "files": ,
                    }
                }
            i+=1
            # Pass each individual record to the Validator
            result = dataset_validator.write_record(record_metadata)
    
            # Check if the Validator accepted the record, and print a message if it didn't
            # If the Validator returns "success" == True, the record was written successfully
            if result["success"] is not True:
                print("Error:", result["message"], ":", result.get("invalid_metadata", ""))
            # The Validator may return warnings if strict=False, which should be noted
            if result.get("warnings", None):
                print("Warnings:", result["warnings"])

    # Alternatively, if the only way you can process your data is in one large list, you can pass the list to the Validator
    # You still must add the required metadata to your records
    # It is recommended to use the previous method if possible
    # result = dataset_validator.write_dataset(your_records_with_metadata)
    #if result["success"] is not True:
        #print("Error:", result["message"])

    # You're done!
    if verbose:
        print("Finished converting")


# Optionally, you can have a default call here for testing
# The convert function may not be called in this way, so code here is primarily for testing
if __name__ == "__main__":
    import paths
    convert(paths.datasets + "qsar_biodeg/biodeg.csv", verbose=True)
