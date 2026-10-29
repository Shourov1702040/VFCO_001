import random, os, sys, hashlib, math, pickle, string, time, zlib, base64, time, re, csv
from collections import deque, defaultdict
from typing import List, Tuple, Dict, Any, Union
from blake3 import blake3
import zstandard as zstd
import EM_GRID as EM_Grid


# _____________________________________ Data generation  _____________________________________
def generate_replicas(block_size_KB, num_blocks, num_replicas, save_dir, use_random_data=True):
    os.makedirs(save_dir, exist_ok=True)
    replica_files = []
    block_size = (block_size_KB * 1024)  # KB → bytes

    for r in range(1, num_replicas + 1):
        chunks = []
        for i in range(num_blocks):
            if use_random_data:
                chunk = os.urandom(block_size)
            else:
                pattern = b"Kademlia-64KB-Chunk-Pattern" + os.urandom(16)
                base_chunk = (pattern * (block_size // len(pattern) + 1))[:block_size]
                chunk = base_chunk + str(i).encode()
            chunks.append(chunk)

        file_path = os.path.join(save_dir, f"replica_{r}.txt")
        with open(file_path, "wb") as f:
            # Use pickle to dump list of chunks
            pickle.dump(chunks, f, protocol=0)  # ASCII protocol
        replica_files.append(file_path)
    # return replica_files

# _________________________________________ Data Load  __________________________________________
def load_replicas_from_dir(save_dir, block_size_KB):
    block_size = block_size_KB * 1024# KB → bytes
    replicas_lst = []
    dir_lst = list(os.listdir(save_dir))
    dir_lst.sort(key=lambda x: int(x.split('_')[1].split('.')[0]))
    for file_name in dir_lst:
        if file_name.endswith(".txt"):
            file_path = os.path.join(save_dir, file_name)

            with open(file_path, "rb") as f:
                chunks = pickle.load(f)  
            replica_data = bytearray().join(chunks)

            blocks = [replica_data[i:i+block_size] for i in range(0, len(replica_data), block_size)]

            replicas_lst.append(blocks)
    return replicas_lst


def index_alloc(csv_filename, total_clients, Total_data, data_scale):
    with open(csv_filename, 'w', newline='') as f:
        writer = csv.writer(f)
        
        header = [f'Edge-Server-{i+1}' for i in range(total_clients)]
        writer.writerow(header)
        
        # All data identifiers
        all_r_values = [f'R-{i}' for i in range(1, Total_data + 1)]
        
        columns_data = []
        for _ in range(total_clients):
            selected = random.sample(all_r_values, data_scale)
            selected.sort(key=lambda x: int(x.split('-')[1])) 
            columns_data.append(selected)
        
        for row_idx in range(data_scale):
            row = [columns_data[col][row_idx] for col in range(total_clients)]
            writer.writerow(row)

def csv_to_edge_info(csv_filename, e_id=''):
    edge_info = {}

    with open(csv_filename, 'r') as f:
        reader = csv.reader(f)

        headers = next(reader)

        for header in headers:
            edge_info[header] = []

        for row in reader:
            for header, value in zip(headers, row):
                edge_info[header].append(value)

    if e_id:
        return edge_info[e_id]
    else:
        return edge_info


def Modify_data_block_hash(hash_val):
    chars = list(hash_val)
    n = len(chars)
    for i in range(n-1, 0, -1):
        j = random.randint(0, i)
        chars[i], chars[j] = chars[j], chars[i]
    return ''.join(chars)

# ________________________________________ Hashing Data __________________________________________
def deterministic_shuffle(lst, shuffle_key: int):
    def shuffle_score(item):
        combined = f"{str(item)}_{shuffle_key}".encode()
        return hashlib.sha256(combined).hexdigest()
    
    return sorted(lst, key=shuffle_score)

def hash_data_SHA_3(data, sec_code):
    """Hashes data using SHA3-256 with security code concatenation."""
    if isinstance(data, str):
        data = data.encode()
    # Convert sec_code to bytes if it isn't already
    if isinstance(sec_code, str):
        sec_code = sec_code.encode()
    data_with_code = data + sec_code
    return hashlib.sha3_256(data_with_code).hexdigest()

def hash_data_black_3(data, sec_code):
    """Hashes data using BLAKE3 with security code concatenation."""
    if isinstance(data, str):
        data = data.encode()
    # Convert sec_code to bytes if it isn't already
    if isinstance(sec_code, str):
        sec_code = sec_code.encode()
    data_with_code = data + sec_code
    return blake3(data_with_code).hexdigest()

# ______________________________ Building MHT _____________________________________________
def generate_leaf_node(replica, replica_id, shuffle_key, sec_code, modify=True):
    # data_list = deterministic_shuffle(replica, shuffle_key)
    data_list = shuffle_and_maybe_corrupt_blocks(replica, shuffle_key, modify)
    node_counter = 0
    hashes = []
    current_level = []
    for i, item in enumerate(data_list):
        hash_i = hash_data_SHA_3(str(item), sec_code)
        # if modify==True and i==1:
        #     hash_i = Modify_data_block_hash(hash_i)
        node = [f"{replica_id}-Node-{node_counter}", 0, i, True, hash_i]
        current_level.append(node)   # keep references so we can update in place on promotion
        hashes.append(hash_i)
        node_counter += 1
    loc_key = Loc_key_gen(hashes)
    return current_level, loc_key

def resequence_nodes(data, prefix):
    resequenced = []
    for i, row in enumerate(data):
        new_row = [
            f"{prefix}-Node-{i}",  # new node id
            0,                # reset ln to 0
            i,                # reset pn to sequence number
            True,             # keep is_leaf value
            row[4]            # keep hash value
        ]
        resequenced.append(new_row)
    return resequenced


def _next_pow2(n: int) -> int:
    p = 1
    while p < n:
        p <<= 1
    return p

def EM_root_builder(leaf_nodes, replica_id, shuffle_key, sec_code):
    """
    Binary Merkle Hash Tree with padding to 2^n leaves.
    Padding rule: duplicate the LAST leaf hash until leaf count becomes power of two.
    Output format stays identical: [node_id, ln, pn, is_leaf, hash]
    """
    if not leaf_nodes:
        raise ValueError("leaf_nodes is empty")

    # copy leaves so we don't mutate caller list unexpectedly
    leaves = [list(n) for n in leaf_nodes]

    # ---- pad to power-of-two leaves ----
    n0 = len(leaves)
    target = _next_pow2(n0)

    node_counter = len(leaves)
    last_hash = leaves[-1][4]

    while len(leaves) < target:
        pn = len(leaves)
        dup_leaf = [f"{replica_id}-Node-{node_counter}", 0, pn, True, last_hash]
        leaves.append(dup_leaf)
        node_counter += 1

    # ensure pn are sequential (0..target-1)
    for i in range(len(leaves)):
        leaves[i][1] = 0
        leaves[i][2] = i
        leaves[i][3] = True

    nodes = leaves[:]         # include padded leaves
    current_level = leaves
    ln = 1

    # ---- build binary parents ----
    while len(current_level) > 1:
        next_level = []
        pos = 0
        for i in range(0, len(current_level), 2):
            left = current_level[i]
            right = current_level[i + 1]

            merged_hash = hash_data_black_3(str(left[4]) + str(right[4]), sec_code)
            parent = [f"{replica_id}-Node-{node_counter}", ln, pos, False, merged_hash]
            node_counter += 1

            nodes.append(parent)
            next_level.append(parent)
            pos += 1

        current_level = next_level
        ln += 1

    return sorted(nodes, key=lambda n: (n[1], n[2]))



# _________________________________ Challenge Generation ___________________________________________

def generate_edge_dict(total_clients, total_data, data_scale):
    edge_server_ids = [f"Edge-Server-{i}" for i in range(1, total_clients+1)]

    edge_dict = {}
    for cid in edge_server_ids:
        nums = sorted(random.sample(range(1, total_data+1), data_scale))  # 4 unique numbers from 0..N
        nums = [f"R-{n}" for n in nums] 
        edge_dict[cid] = nums

    return edge_dict

def Generate_Additional_info(global_tree, edge_replicas):
    # Build lookup: id -> node
    id_map = {n[0]: n for n in global_tree}

    # Map replica IDs (R-1, R-2, ...) -> leaf IDs (G-Node-x)
    mapped = {}
    for rid in edge_replicas:
        num = int(re.search(r'(\d+)$', rid).group(1)) - 1
        leaf_id = f"G-Node-{num}"
        if leaf_id in id_map:
            mapped[rid] = leaf_id

    required_leaves = set(mapped.values())

    # Group nodes by level
    level_nodes = defaultdict(list)
    for n in global_tree:
        level_nodes[n[1]].append(n)

    max_level = max(level_nodes.keys())

    # Build child relationships (BINARY)
    children_map = defaultdict(list)
    for n in global_tree:
        lvl, pos = n[1], n[2]
        if lvl < max_level:
            parent_pos = pos // 2   # ✅ binary
            parent_lvl = lvl + 1
            for parent in level_nodes.get(parent_lvl, []):
                if parent[2] == parent_pos:
                    children_map[parent[0]].append(n[0])
                    break

    # Compute descendant leaves
    descendant_map = {}
    def get_descendants(node_id):
        node = id_map[node_id]
        if node[3]:  # is_leaf
            descendant_map[node_id] = {node_id}
            return {node_id}
        if node_id in descendant_map:
            return descendant_map[node_id]
        desc = set()
        for child in children_map.get(node_id, []):
            desc |= get_descendants(child)
        descendant_map[node_id] = desc
        return desc

    for nid in id_map:
        get_descendants(nid)

    # Find lowest covering parent
    candidates = []
    for nid, node in id_map.items():
        if not node[3]:  # internal only
            if required_leaves.issubset(descendant_map[nid]):
                candidates.append(node)

    if not candidates:
        return None

    covering = min(candidates, key=lambda n: n[1])

    # -------- NEW: find additional required nodes --------
    additional_nodes = set()

    def find_additional(node_id):
        """Check what nodes we must include under this parent."""
        node = id_map[node_id]
        if node[3]:  # leaf
            if node_id not in required_leaves:
                additional_nodes.add(node_id)
            return

        children = children_map.get(node_id, [])
        for child in children:
            child_leaves = descendant_map[child]
            if child_leaves & required_leaves:  
                # Child has required leaves
                if not id_map[child][3]:  # internal
                    find_additional(child)
                elif child not in required_leaves:  # leaf not in required list
                    additional_nodes.add(child)
            else:
                # Child has no required leaves -> include it directly
                additional_nodes.add(child)

    find_additional(covering[0])
    additionals_nodes = sorted(additional_nodes, key=lambda x: int(x.split('-')[-1]))
    proof_root = covering
    
    return proof_root,  additionals_nodes

def generate_challenge(edge_info, Data_replicas):
    replica_ids = [f"R-{i}" for i in range(1, len(Data_replicas)+1)]
    ES_challenges = {}
    ES_proofs = {} 

    # shuffle_key = random.randint(0, 10)
    # sec_code = ''.join(random.choices(string.ascii_letters + string.digits, k=16))

    shuffle_key = 3
    sec_code = "knRpR3Yvf5Seq2Sz"

    Trees = []
    Roots = []
    G_leafs = []
    G_Tree = []

    block_loc_key = {}

    for i in range(len(Data_replicas)):
        leafs, loc_key = generate_leaf_node(Data_replicas[i], replica_ids[i], shuffle_key, sec_code, False)

        # EM-Grid root over shuffled blocks (no corruption at CS side)
        shuffled_blocks = shuffle_and_maybe_corrupt_blocks(Data_replicas[i], shuffle_key, False)
        em_info = EM_Grid.build_em_grid_bls(shuffled_blocks, sec_code)
        em_root_hash = em_info["EM_root_hash"]

        # store loc_key as before
        block_loc_key[f"R-{i+1}"] = loc_key

        # treat EM-root as the "replica root node"
        Roots.append([f"{replica_ids[i]}-EMRoot", 0, i, True, em_root_hash])

    G_leafs = resequence_nodes(Roots, "G")
    G_Tree = EM_root_builder(G_leafs, "G", shuffle_key, sec_code)
    challenge_all =[]
    proof_all = {}
    loc_key_all = {}

    for e_id, replicas in edge_info.items():
        Specific_List = replicas
        proof_root, additional_nodes_ids = Generate_Additional_info(G_Tree, Specific_List)
        additional_nodes = [n for n in G_Tree if n[0] in additional_nodes_ids]
        loc_keys = {key: block_loc_key[key] for key in Specific_List if key in block_loc_key}

        chal = [e_id, shuffle_key, sec_code, additional_nodes]
        challenge_all.append(chal)

        proof_all[e_id] = proof_root
        loc_key_all[e_id] = loc_keys
    
    return challenge_all, proof_all, loc_key_all


#_______________________________________ Transform Data _____________________________________________

def transform_list(data):
    result = []
    i = 0
    for row in data:
        r_num = int(row[0].split('-')[1]) 
        g_num = r_num - 1  
        new_row = [f'G-Node-{g_num}', 0, g_num, True, row[4]]
        result.append(new_row)
        i+=1
    return result

def _next_unique_id(existing_ids: set, base_name="Node") -> str:
    n = 1
    while f"{base_name}-{n}" in existing_ids:
        n += 1
    new_id = f"{base_name}-{n}"
    existing_ids.add(new_id)
    return new_id



# ________________________________ Localization Key Generation ____________________________________

def Loc_key_gen(list_B_added):
    # Use first 8 hex chars from each pre-hashed item
    partial_hashes = [item[:8] for item in list_B_added]
    key = ''.join(partial_hashes)
    raw_bytes = bytes.fromhex(key)
    cctx = zstd.ZstdCompressor(level=22)  # max compression
    compressed = cctx.compress(raw_bytes)
    return base64.b85encode(compressed).decode()
    return key

# _________________________________ Corruption Localization _______________________________________

def Detection_function_from_dicts(dict_A, dict_B):
    dctx = zstd.ZstdDecompressor()
    detected_all = {}

    for replica_id in dict_A:
        key_A = dict_A[replica_id]
        key_B = dict_B.get(replica_id)

        if key_B is None:
            continue  
        if key_A == key_B:
            continue
        decompressed_A = dctx.decompress(base64.b85decode(key_A.encode())).hex()
        decompressed_B = dctx.decompress(base64.b85decode(key_B.encode())).hex()
        chunks_A = [decompressed_A[i*8:(i+1)*8] for i in range(len(decompressed_A)//8)]
        chunks_B = [decompressed_B[i*8:(i+1)*8] for i in range(len(decompressed_B)//8)]

        corrupted_indices = []

        def detect_range(start, end):
            if start > end:
                return
            if chunks_A[start:end+1] == chunks_B[start:end+1]:
                return
            if start == end:
                corrupted_indices.append(start)
                return
            mid = (start + end) // 2
            detect_range(start, mid)
            detect_range(mid+1, end)

        detect_range(0, len(chunks_A)-1)
        detected_all[replica_id] = corrupted_indices

    return detected_all


def Detection_fucntion(list_A, key):
    compressed = base64.b85decode(key.encode())
    dctx = zstd.ZstdDecompressor()
    decompressed = dctx.decompress(compressed)
    key_2 = decompressed.hex()

    detected = []
    for i, item in enumerate(list_A):
        expected_hash = key_2[i*8:(i+1)*8]
        if item[:8] != expected_hash:
            detected.append(i)

    return detected


def shuffle_and_maybe_corrupt_blocks(replica_blocks, shuffle_key: int, modify: bool):
    """
    Returns shuffled blocks list.
    If modify=True, corrupt the 2nd block (index 1) content so EM-Grid root changes.
    """
    blocks = deterministic_shuffle(replica_blocks, shuffle_key)
    if modify and len(blocks) > 1:
        b = blocks[1]
        if isinstance(b, (bytes, bytearray)):
            bb = bytearray(b)
            bb[0] = (bb[0] ^ 0xFF)  # flip first byte
            blocks[1] = bytes(bb)
        else:
            s = str(b)
            blocks[1] = ("X" + s[1:]) if len(s) > 0 else "X"
    return blocks


# _______________________________ New addaed for experiments _______________________________
