import os

import requests
from tqdm import tqdm

from ..utils.paths import get_path


SUBMISSION_URL = ""

# Function to submit feedstock to MDF
# Args:
#   path: Path to the feedstock
#   verbose: Should status messages be printed? Default False.
def submit_feedstock(path, verbose=False):
    with open(path) as in_file:
        feed_data = in_file.read()
    res = requests.post(SUBMISSION_URL, data=feed_data)
    if res.status_code != 200:
        return {
            "success": False,
            "message": "There was an error in submitting this feedstock (Error code " + str(res.status_code) + ")",
            "error_code": res.status_code
            }
    return res.json


# Function to submit all feedstock in a directory to MDF
# Args:
#   path: Path to the feedstock directory
#   strict: Should one failure stop further submissions? Default False.
#   verbose: Should status messages be printed? Default False.
def submit_all(path=None, strict=False, verbose=False):
    if not path:
        path = get_path(__file__, "feedstock")
    if verbose:
        print("Submitting all feedstock from '", path, "'", sep="")
    count = 0
    success = 0
    for feedstock in tqdm(os.listdir(path), desc="Submitting feedstock", disable= not verbose):
        # Must be actual feedstock
        if feedstock.endswith("_all.json"):
            result = submit_feedstock(os.path.join(path, feedstock), verbose=verbose)
            if result["success"]:
                success += 1
            elif strict:
                raise ValueError(result)
            count += 1

    if verbose:
        print("Successfully submitted ", success, "/", count, " total feedstock files", sep="")

    return {
        "success": True,
        "total": count,
        "success": success
        }


