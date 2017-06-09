import os

# Returns the path to a specified resource in the repo
# Arguments:
#   cwd: Originating path
#   resource: Name of the directory to search for
#   strict: Should the function raise an exception if the resource is not found? Default True.
#       If False, will return None for an invalid path
def get_path(cwd, resource, strict=True):
    head, tail = os.path.split(cwd)
    while "mdf_indexers" not in tail and tail:
        head, tail = os.path.split(head)
    path = os.path.join(head, tail, resource, "")
    if not os.path.exists(path):
        if strict:
            raise ValueError("Requested resource not found")
        else:
            path = None
    return path

