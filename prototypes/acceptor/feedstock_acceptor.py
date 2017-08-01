import os
import json

from flask import Flask
from tqdm import tqdm

from ..validator.schema_validator import Validator
from ..utils.paths import get_path

app = Flask(__name__)

# Function to accept user-generated feedstock
# Args:
#   path: Path to the feedstock
#   remove_old: Should fully accepted feedstock be removed? Default False.
#   verbose: Should status messages be printed? Default False.
def accept_feedstock(path, remove_old=False, verbose=False):
    removed = False
    with open(path) as feedstock:
        val = Validator(json.loads(feedstock.readline()))
        for line in tqdm(feedstock, desc="Accepting " + os.path.basename(path), disable= not verbose):
            res = val.write_record(json.loads(line))
            if not res["success"]:
                if not val.cancel_validation()["success"]:
                    print("ERROR: Validation not cancelled. Feedstock may not have been removed.")
                raise ValueError(result["message"] + "\n" + result.get("details"))
    if remove_old:
        os.remove(path)
        removed = True

    return {
        "success": True,
        "source_deleted": removed
        }


# Function to accept all feedstock in a directory
# Args:
#   path: Path to the feedstock directory
#   remove_old: Should fully accepted feedstock be removed? Default False.
#   verbose: Should status messages be printed? Default False.
def accept_all(path=None, remove_old=False, verbose=False):
    if not path:
        path = get_path(__file__, "submissions")
    if verbose:
        print("Accepting all feedstock from '", path, "'", sep="")
    removed = []
    count = 0
    for feedstock in tqdm(os.listdir(path), desc="Accepting feedstock", disable= not verbose):
        # Must be actual feedstock
        if feedstock.endswith("_all.json"):
            result = accept_feedstock(os.path.join(path, feedstock), remove_old=remove_old, verbose=verbose)
            count += 1
            if result["source_deleted"]:
                removed.append(feedstock)
    if verbose:
        print("Accepted", count, "total feedstock files")

    return {
        "success": True,
        "removed": removed,
        "total": count
        }

