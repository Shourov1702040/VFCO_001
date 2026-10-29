import socket, pickle, struct, traceback, os, time
from pathlib import Path
import Functionalities_ES as Functionalities



def _recv_frame(sock):
    """Read exactly one length-prefixed message, independent of TCP packet boundaries."""
    import io
    def exact(n):
        buf = bytearray()
        while len(buf) < n:
            part = sock.recv(n - len(buf))
            if not part:
                if not buf:
                    return None
                raise ConnectionError("Truncated TCP frame")
            buf.extend(part)
        return bytes(buf)
    header = exact(4)
    if header is None:
        return None, 0
    size = int.from_bytes(header, 'big')
    if not 0 < size <= 64 * 1024 * 1024:
        raise ValueError(f"Invalid TCP message length: {size}")
    raw = exact(size)
    class PrimitiveUnpickler(pickle.Unpickler):
        def find_class(self, module, name):
            raise pickle.UnpicklingError(f"Object type not allowed: {module}.{name}")
    return PrimitiveUnpickler(io.BytesIO(raw)).load(), size + 4


def _frame(payload):
    return len(payload).to_bytes(4, 'big') + payload

# ______________________________ Edge Server Information_________________________________________#

client_id = "Edge-Server-4"
block_size = 512  #KB
data_loc = str(Path(__file__).resolve().parent.parent / "replicas")
# replica_ids = ['R-2', 'R-3', 'R-5', 'R-7']

csv_filename = str(Path(__file__).resolve().parent / "edge_data.csv")
replica_ids = Functionalities.csv_to_edge_info(csv_filename, client_id)
# print(replica_ids)

Data_replicas = Functionalities.load_replicas_from_dir_ES(data_loc, replica_ids, block_size)

# Data_replicas = Functionalities.Modify_data(Data_replicas, 'R-2', 0)
# d_dad_ = Data_replicas[0][0]
# d_dad = d_dad_.decode('utf-8')
# d_dad = 'd'+d_dad
# Data_replicas[0][0] = d_dad
time_all_ES = []
flag_corrupt = 0
CORRUPTION_RATIO = float(os.environ.get('VFCO_CORRUPTION_RATIO', '0.05'))
tamper_started_s = 0.0
#______________________________________ Private Function ________________________________________#
def generate_reply_message(challenge_message, client_id, replica_ids, Data_replicas):
    Trees_ES = []
    Roots_ES = []
    G_leafs_ES = []
    block_loc_key = {}
    # print(challenge_message)
    ES_id, sh_key, sec_code, A_info = tuple(challenge_message[:4])

    if client_id != ES_id:
        print(f"[{client_id}] Wrong Edge Server in challenge. Expected {client_id}, got {ES_id}")
        return [client_id, "WrongEdgeID"]

    for index_ss in range(len(Data_replicas)):
        modification_status = False
        # Cached blocks are modified only by the injected attack, not by proof hashing.
        leafs, loc_key = Functionalities.generate_leaf_node(Data_replicas[index_ss], replica_ids[index_ss], sh_key, sec_code, modification_status)
        tree = Functionalities.build_MHT(leafs, replica_ids[index_ss], sh_key, sec_code)
        Trees_ES.append(tree)
        block_loc_key[replica_ids[index_ss]] = loc_key
        Roots_ES.append(tree[-1])

    # print(Trees_ES[0])
    G_leafs_ES = Functionalities.transform_list(Roots_ES)
    root = Functionalities.build_minimal_tree(G_leafs_ES, A_info, sec_code)

    Response_edge = [client_id, root, block_loc_key]
    return Response_edge

# _________________________________ Main Edge Server setup _______________________________________#
Edge_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
cloud_server_address = ('127.0.0.1', 12345)
print(f"[{client_id}] Connecting to Cloud_server at {cloud_server_address}")
i=1
try:
    Edge_server.connect(cloud_server_address)
    print(f"[{client_id}] Connected to Cloud_server.")

    Edge_server.sendall(f"{client_id}: Connected".encode())

    while True:
        message_list, _wire_size = _recv_frame(Edge_server)
        if message_list is None:
            print(f"[{client_id}] Cloud_server closed connection.")
            break

        try:
            start_time_RS = time.time()
            start_cpu_RS = time.process_time()
            if (isinstance(message_list, (tuple, list)) and len(message_list) == 4
                    and message_list[1] == 'RECOVER'):
                _, _, replica_id, trusted_blocks = message_list
                if replica_id in replica_ids:
                    Data_replicas[replica_ids.index(replica_id)] = trusted_blocks
                    tamper_started_s = 0.0
                    print(f"[{client_id}] Recovered cached {replica_id} from CS")
                continue
            # print(f"len of r_ids: {len(replica_ids)}, len of Data replicas: {len(Data_replicas)}")
            # print(f"[{client_id}] Received: {message_list}")
            message_list = list(message_list)
            reply_message = generate_reply_message(message_list, client_id, replica_ids, Data_replicas)
            # Bind response to the exact fresh CS challenge and report measured CPU time.
            if len(reply_message) < 3:
                raise ValueError('Invalid challenge')
            reply_message.extend([message_list[2], time.process_time() - start_cpu_RS, tamper_started_s])

            try:
                # print(f"[{client_id}] Sending reply: {reply_message}") 
                serialized_msg = pickle.dumps(reply_message)
                
                Edge_server.sendall(_frame(serialized_msg))
                print(f"[{client_id}] has sent integrity proof to CS")
            except Exception as e:
                print(f"[{client_id}] Error sending reply: {e}")
                
            flag_corrupt += 1
            if (tamper_started_s == 0.0 and len(Data_replicas) > 0
                    and __import__('secrets').randbelow(10**6) < int(10**6 * (float(message_list[4]) if len(message_list) > 4 else CORRUPTION_RATIO))):
                target_idx = min(1, len(Data_replicas) - 1)
                if Data_replicas[target_idx]:
                    first_block = Data_replicas[target_idx][0]
                    if isinstance(first_block, (bytes, bytearray)):
                        Data_replicas[target_idx][0] = bytes([first_block[0] ^ 1]) + bytes(first_block[1:])
                    else:
                        Data_replicas[target_idx][0] = ('X' + str(first_block))
                    tamper_started_s = time.time()
                    print(f"[{client_id}] Injected persistent post-verification corruption")
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
