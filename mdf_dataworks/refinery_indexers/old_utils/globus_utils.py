from globus_sdk import GlobusError

# Attempts to autodetect the local GCP endpoint ID
# If multiple candidates are found, prompt user to choose correct EP
def get_local_ep(transfer_client):
    pgr_res = transfer_client.endpoint_search(filter_scope="my-endpoints")
    ep_candidates = pgr_res.data
    if len(ep_candidates) < 1: #Nothing found
        raise GlobusError("Error: No local endpoints found")
    elif len(ep_candidates) == 1: #Exactly one candidate
        if ep_candidates[0]["gcp_connected"] == False: #Is GCP, is not on
            raise GlobusError("Error: Globus Connect is not running")
        else: #Is GCServer or GCP and connected
            return ep_candidates[0]["id"]
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
                return ep_connections[0]["id"]
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
                raise SystemExit
            return ep_connections[ep_num-1]["id"]

