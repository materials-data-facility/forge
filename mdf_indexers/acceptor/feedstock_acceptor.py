import os
import json

from tqdm import tqdm

from ..validator.schema_validator import Validator

# Function to accept user-generated feedstock
# Args:
#   path: Path to the feedstock
#   strict: Should errors trigger exceptions? Default True.
#   remove_old: Should fully accepted feedstock be removed? Default False.
#   verbose: Should status messages be printed? Default False.
def accept_feedstock(path, strict=True, remove_old=False, verbose=False):
    full_accept = True
    removed = False
    with open(path) as feedstock:
        val = Validator(json.loads(feedstock.readline()))
        for line in tqdm(feedstock, desc="Accepting " + os.path.basename(path), disable= not verbose):
            res = val.write_record(json.loads(line))
            if not res["success"]:
                full_accept = False
                msg = "Error:", result["message"]
                if strict:
                    raise ValueError(msg)
                else:
                    print(msg)
    if full_accept and remove_old:
        os.remove(path)
        removed = True

    return {
        "success": True,
        "no_errors": full_accept,
        "source_deleted": removed
        }


# Function to accept all feedstock in a directory
# Args:
#   path: Path to the feedstock directory
#   strict: Should errors trigger exceptions? Default True.
#   remove_old: Should fully accepted feedstock be removed? Default False.
#   verbose: Should status messages be printed? Default False.
def accept_all(path=None, strict=True, remove_old=False, verbose=False):
    if not path:
        path = os.path.join(os.path.dirname(__file__), "acceptor_inbox")
    if verbose:
        print("Accepting all feedstock from '", path, "'", sep="")
    unaccepted = []
    removed = []
    count = 0
    for feedstock in tqdm(os.listdir(path), desc="Accepting feedstock", disable= not verbose):
        # Must be actual feedstock
        if feedstock.endswith("_all.json"):
            result = accept_feedstock(os.path.join(path, feedstock), strict=strict, remove_old=remove_old, verbose=verbose)
            count += 1
            if not result["no_errors"]:
                unaccepted.append(feedstock)
            elif result["source_deleted"]:
                removed.append(feedstock)
    if verbose:
        print("Accepted", count, "total feedstock files")

    return {
        "success": True,
        "unaccepted": unaccepted,
        "removed": removed
        }

