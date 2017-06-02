
# coding: utf-8

# # Agent prototype: Create ML training set from PPPDB

# In[1]:

import json
import os
import pandas as pd
import transfer_auth
import globus_auth
from globus_sdk import TransferData, GlobusError
from gmeta_utils import gmeta_pop, format_gmeta

def run_agent():
    # In[2]:

    search_client = globus_auth.login("https://search.api.globus.org/", "globus_search")
    transfer_client = transfer_auth.login()


    # In[3]:

    dataset_name = "pppdb"
    local_ep = "0bc1cb98-d2af-11e6-9cb1-22000a1e3b52"
    dest_ep = "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec"
    dest_path = "/sample_data/"+dataset_name+"_train.csv"
    timeout = False
    timeout_intervals = 10
    interval_time = 10
    verbose = True

    # In[4]:

    if not local_ep:
        pgr_res = transfer_client.endpoint_search(filter_scope="my-endpoints")
        ep_candidates = pgr_res.data
        if len(ep_candidates) < 1: #Nothing found
            raise GlobusError("Error: No local endpoints found")
        elif len(ep_candidates) == 1: #Exactly one candidate
            if ep_candidates[0]["gcp_connected"] == False: #Is GCP, is not on
                raise GlobusError("Error: Globus Connect is not running")
            else: #Is GCServer or GCP and connected
                local_ep = ep_candidates[0]["id"]
        else: # >1 found
            #Filter out disconnected GCP
            ep_connections = [candidate for candidate in ep_candidates if candidate["gcp_connected"] is not False]
            #Recheck list
            if len(ep_connections) < 1: #Nothing found
                raise GlobusError("Error: No local endpoints running")
            elif len(ep_connections) == 1: #Exactly one candidate
                if ep_connections[0]["gcp_connected"] == False: #Is GCP, is not on
                    raise GlobusError("Error: Globus Connect is not active")
                else: #Is GCServer or GCP and connected
                    local_ep = ep_connections[0]["id"]
            else: # >1 found
                #Prompt user
                print("Multiple endpoints found:")
                count = 0
                for ep in ep_connections:
                    count += 1
                    print(count, ": ", ep["display_name"], "\t", ep["id"])
                print("\nPlease choose the endpoint on this machine")
                ep_num = 0
                while ep_num == 0:
                    usr_choice = input("Enter the number of the correct endpoint (-1 to cancel): ")
                    try:
                        ep_choice = int(usr_choice)
                        if ep_choice == -1: #User wants to quit
                            ep_num = -1 #Will break out of while to exit program
                        elif ep_choice in range(1, count+1): #Valid selection
                            ep_num = ep_choice #Break out of while, return valid ID
                        else: #Invalid number
                            print("Invalid selection")
                    except:
                        print("Invalid input")

                if ep_num == -1:
                    print("Cancelling")
                    sys.exit()
                local_ep = ep_connections[ep_num-1]["id"]


    # # Fetch and aggregate records into training set

    # In[5]:

    count = 0
    num_processed = 0
    data_list = []
    while True:
        query = {
            "q": ("mdf_source_name:"+dataset_name+" AND mdf_node_type:record AND "
            "globus_scroll_id:(>=" + str(count) + " AND <" + str(count + 10000) + ")"),
            "advanced": True,
            "limit": 10000
        }
        raw_res = search_client.structured_search(query)
        search_res = gmeta_pop(raw_res, True)
        for res in search_res:
            data_dict = json.loads(res["data"]["raw"])
            data_list.append(data_dict)
        num_ret = len(search_res)
        if num_ret:
            num_processed += num_ret
            count += 10000
        else:
            break
    if verbose:
        print("Processed:", len(data_list), "/", num_processed, "|", len(data_list) - num_processed)


    # In[6]:

    df = pd.DataFrame(data_list)
    df.to_csv(os.path.join(os.getcwd(), "temp_train.csv"))


    # # Upload to NCSA endpoint

    # In[7]:

    try:
        tdata = TransferData(transfer_client, local_ep, dest_ep, verify_checksum=True, notify_on_succeeded=False, notify_on_failed=False, notify_on_inactive=False)
        tdata.add_item(os.path.join(os.getcwd(), "temp_train.csv"), dest_path)
        res = transfer_client.submit_transfer(tdata)
        if res["code"] != "Accepted":
            raise GlobusError("Failed to transfer files: Transfer " + res["code"])
        else:
            intervals = 0
            while not transfer_client.task_wait(res["task_id"], timeout=interval_time, polling_interval=interval_time):
                for event in transfer_client.task_event_list(res["task_id"]):
                    if event["is_error"]:
                        transfer_client.cancel_task(res["task_id"])
                        raise GlobusError("Error: " + event["description"])
                    if timeout and intervals >= timeout_intervals:
                        transfer_client.cancel_task(res["task_id"])
                        raise GlobusError("Transfer timed out.")
                    intervals += 1
    except Exception as e:
        raise
    finally:
        os.remove(os.path.join(os.getcwd(), "temp_train.csv"))


    # # Update dataset entry

    # In[8]:

    query = {
        "q": "mdf_source_name:"+dataset_name+" AND mdf_node_type:dataset",
        "advanced": True
    }
    raw_res = search_client.structured_search(query)
    search_res = gmeta_pop(raw_res)
    if len(search_res) != 1:
        raise ValueError("Incorrect number of results: " + str(len(search_res)))
    ingest = search_res[0]
    ingest["globus_subject"] = raw_res["gmeta"][0]["subject"]
    ingest["acl"] = ["public"]
    ingest["http://materialsdatafacility.org/#training_set"] = {
        "http://materialsdatafacility.org/#endpoint": dest_ep,
        "http://materialsdatafacility.org/#path": dest_path,
        "http://materialsdatafacility.org/#https": "https://data.materialsdatafacility.org" + dest_path
    }
    gmeta = format_gmeta([format_gmeta(ingest)])

    gmeta = json.loads(json.dumps(gmeta).replace("mdf-publish.publication.community", "http://globus.org/publish-terms/#publication/community"))


    # In[9]:

    search_client.ingest(gmeta)


    # # Check ingest

    # In[10]:

    query = {
        "q": "mdf_source_name:"+dataset_name+" AND mdf_node_type:dataset",
        "advanced": True
    }
    raw_res = search_client.structured_search(query)
    search_res = gmeta_pop(raw_res, True)


    # In[11]:

    if verbose:
        print("Verification:\n", json.dumps(search_res[0]["training_set"], sort_keys=True, indent=4, separators=(',', ': ')))


if __name__ == "__main__":
    run_agent()
