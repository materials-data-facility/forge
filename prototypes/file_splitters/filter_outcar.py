#Evan Pike dep78@cornell.edu
#Script for filtering out OUTCAR files and perform a globus transfer
import globus_sdk
import os

def get_outcar_files(source_id, path):
    data = []
    for item in tc.operation_ls(source_id, path=path):
        if item["type"] == "dir":
            new_path = os.path.join(path, item["name"]) + "/"
            data += get_outcar_files(source_id, new_path)
        elif "OUTCAR" in item["name"]:
            path=os.path.join(path, item["name"])
            data.append((item["name"], path))
            print("path: " + path)
    return(data)

# this is the ID of the Jupyter Demo App
CLIENT_ID = '3b1925c0-a87b-452b-a492-2c9921d3bd14'
native_auth_client = globus_sdk.NativeAppAuthClient(CLIENT_ID)
native_auth_client.oauth2_start_flow()
print("Login Here:\n\n{0}".format(native_auth_client.oauth2_get_authorize_url()))
print(("\n\nNote that this link can only be used once! "
       "If login or a later step in the flow fails, you must restart it."))

# fill this line in with the code that you got
auth_code = ""
token_response = native_auth_client.oauth2_exchange_code_for_tokens(auth_code)
transfer_access_token = token_response.by_resource_server['transfer.api.globus.org']['access_token']
transfer_authorizer = globus_sdk.AccessTokenAuthorizer(transfer_access_token)
tc = globus_sdk.TransferClient(authorizer=transfer_authorizer)


#petrel#researchdataanalytics
source_id = "e38ee745-6d04-11e5-ba46-22000b92c6ec"
tc.endpoint_autoactivate(source_id)

#dep78
dest_id = "00d2823a-4c86-11e7-bd94-22000b9a448b"
tc.endpoint_autoactivate(dest_id)

source_path = "Trinkle/Mg-X-DiffusionDatabase/"
dest_path = "/~/Documents/Internship2017/mdf-harvesters/datasets/trinkle_mg_x/"
tdata = globus_sdk.TransferData(tc, source_id, dest_id)
tdata = globus_sdk.TransferData(tc, source_id, dest_id)

for outcar in get_outcar_files(source_id, source_path):
    total_path = outcar[1]
    indx = len("Trinkle/Mg-X-DiffusionDatabase/")
    end_path = total_path[indx:]
    tdata.add_item(outcar[1], dest_path + end_path, recursive=False)

submit_result = tc.submit_transfer(tdata)
print("Task ID:", submit_result["task_id"])
