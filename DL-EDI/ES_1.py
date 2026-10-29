import socket, pickle, struct, traceback, os, time
import Functionalities_ES as Functionalities
import EM_GRID as EM_Grid

# ______________________________ Edge Server Information_________________________________________#

client_id = "Edge-Server-1"
block_size = 512  #KB
data_loc = "C:/My Drive/PHD Works/Task 2& 3/Work 3 OFD/Code/replicas"
# replica_ids = ['R-2', 'R-3', 'R-5', 'R-7']

csv_filename = "C:/My Drive/PHD Works/Task 2& 3/Work 3 OFD/Code/DL-EDI/edge_data.csv"
replica_ids = Functionalities.csv_to_edge_info(csv_filename, client_id)
# print(replica_ids)

Data_replicas = Functionalities.load_replicas_from_dir_ES(data_loc, replica_ids, block_size)

# Data_replicas = Functionalities.Modify_data(Data_replicas, 'R-2', 0)
# d_dad_ = Data_replicas[0][0]
# d_dad = d_dad_.decode('utf-8')
# d_dad = 'd'+d_dad
# Data_replicas[0][0] = d_dad
time_all_ES = []
flag_corrupt = 1

CORRUPTION_RATIO = 5
#______________________________________ Private Function ________________________________________#
def generate_reply_message(challenge_message, client_id, replica_ids, Data_replicas):
    Trees_ES = []
    Roots_ES = []
    G_leafs_ES = []
    block_loc_key = {}
    # print(challenge_message)
    ES_id, sh_key, sec_code, A_info = tuple(challenge_message)

    if client_id != ES_id:
        print(f"[{client_id}] Wrong Edge Server in challenge. Expected {client_id}, got {ES_id}")
        return [client_id, "WrongEdgeID"]

    for index_ss in range(len(Data_replicas)):
        modification_status = False
        if index_ss==1 and flag_corrupt<=CORRUPTION_RATIO:
            modification_status = True     # Corrupting data
        leafs, loc_key = Functionalities.generate_leaf_node(Data_replicas[index_ss], replica_ids[index_ss], sh_key, sec_code, modification_status)
        # EM-Grid root over shuffled blocks (corruption applied if modification_status=True)
        shuffled_blocks = Functionalities.shuffle_and_maybe_corrupt_blocks(Data_replicas[index_ss], sh_key, modification_status )
        em_info = EM_Grid.build_em_grid_bls(shuffled_blocks, sec_code)
        em_root_hash = em_info["EM_root_hash"]

        Roots_ES.append([f"{replica_ids[index_ss]}-EMRoot", 0, index_ss, True, em_root_hash])

    # print(Trees_ES[0])
    G_leafs_ES = Functionalities.transform_list(Roots_ES)
    root = Functionalities.build_minimal_tree(G_leafs_ES, A_info, sec_code)

    Response_edge = [client_id, root, block_loc_key]
    return Response_edge

# _________________________________ Main Edge Server setup _______________________________________#
Edge_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
cloud_server_address = ('127.0.0.1', 12345) # replace with your cloud server's IP address
print(f"[{client_id}] Connecting to Cloud_server at {cloud_server_address}")

try:
    Edge_server.connect(cloud_server_address)
    print(f"[{client_id}] Connected to Cloud_server.")

    Edge_server.sendall(f"{client_id}: Connected".encode())

    while True:
        data = Edge_server.recv(4096)
        if not data:
            print(f"[{client_id}] Cloud_server closed connection.")
            break

        try:
            start_time_RS = time.time()
            message_list = pickle.loads(data)
            # print(f"len of r_ids: {len(replica_ids)}, len of Data replicas: {len(Data_replicas)}")
            # print(f"[{client_id}] Received: {message_list}")
            message_list = list(message_list)
            reply_message = generate_reply_message(message_list, client_id, replica_ids, Data_replicas)

            try:
                # print(f"[{client_id}] Sending reply: {reply_message}") 
                serialized_msg = pickle.dumps(reply_message)
                
                Edge_server.sendall(serialized_msg)
                print(f"[{client_id}] has sent integrity proof to CS")
            except Exception as e:
                print(f"[{client_id}] Error sending reply: {e}")

            print(f"Verification iteration: {flag_corrupt}")
            flag_corrupt += 1
            time_all_ES.append(time.time() - start_time_RS)
            avg_time_for_proof_generation = sum(time_all_ES) / len(time_all_ES) if time_all_ES else 0.0
            
            print(f"Average proof generation time: {avg_time_for_proof_generation}")
        except Exception as es:
            traceback.print_exc()
            print(f"[{client_id}] Received non-pickled data. {es}")

except KeyboardInterrupt:
    print(f"\n[{client_id}] Shutting down on user request.")

except Exception as e:
    print(f"[{client_id}] Error: {e}")

finally:
    print(f"[{client_id}] Closing connection.")
    Edge_server.close()
