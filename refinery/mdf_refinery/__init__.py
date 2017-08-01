import os
MDF_PATH = os.path.expanduser("~/mdf/")
os.makedirs(MDF_PATH, exist_ok=True)

PATH_DATASETS = os.path.join(MDF_PATH, "datasets")
os.makedirs(PATH_DATASETS, exist_ok=True)

PATH_FEEDSTOCK = os.path.join(MDF_PATH, "feedstock")
os.makedirs(PATH_FEEDSTOCK, exist_ok=True)

PATH_CREDENTIALS = os.path.join(MDF_PATH, "credentials")
os.makedirs(PATH_CREDENTIALS, exist_ok=True)

PATH_SCHEMAS = os.path.join(MDF_PATH, "schemas")
os.makedirs(PATH_SCHEMAS, exist_ok=True)

