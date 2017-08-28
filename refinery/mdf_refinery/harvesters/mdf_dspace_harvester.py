from globus_sdk import AccessTokenAuthorizer, RefreshTokenAuthorizer, GlobusAPIError
from globus_sdk.base import BaseClient

from json import dump, loads
import os
import os.path
from shutil import rmtree
from tqdm import tqdm

base_url = "https://publish.globus.org/v1/api/"

class DSpaceDatasetFetcher(BaseClient):
    allowed_authorizer_types = [AccessTokenAuthorizer, RefreshTokenAuthorizer]

    def __init__(self, base_url, **kwargs):
        app_name = kwargs.pop('app_name', 'DataPublication Client v0.1')
        BaseClient.__init__(self, "datapublication", app_name=app_name, **kwargs)
        self.base_url = base_url
        self._headers['Content-Type'] = 'application/json'

    def get_dataset(self, dataset_id, **params):
        return self.get('datasets/{}'.format(dataset_id), params=params)


def get_dataset_id_list():
    return range(1000)


#Collects available data from MDF and saves to the given directory
#out_dir: The path to the directory (which will be created) for the data files
#existing_dir:
#       -1: Remove out_dir if it exists
#        0: Error if out_dir exists (Default)
#        1: Overwrite files in out_dir if there are path collisions
#verbose: Print status messages? Default False
def harvest(out_dir, existing_dir=0, verbose=False):
    if os.path.exists(out_dir):
        if existing_dir == 0:
            exit("Directory '" + out_dir + "' exists")
        elif not os.path.isdir(out_dir):
            exit("Error: '" + out_dir + "' is not a directory")
        elif existing_dir == -1:
            rmtree(out_dir)
            os.mkdir(out_dir)
    else:
        os.mkdir(out_dir)

    client = DSpaceDatasetFetcher(base_url)

    #Get dataset metadata
    for ds_id in get_dataset_id_list():
        try:
            res = client.get_dataset(ds_id)
            
            with open(os.path.join(out_dir, str(ds_id) + "_metadata.json"), 'w') as out_file:
                dump(res.data, out_file)

        except GlobusAPIError as e:
            if e.http_status == 404 or e.http_status == 403:
                pass
            else:
                raise


