import random, os, sys, hashlib, math ,pickle, string, time, zlib, base64, time, re, csv
from collections import deque, defaultdict
from typing import List, Tuple, Dict, Any, Union
from blake3 import blake3
import zstandard as zstd


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

def load_replicas_from_dir_ES(save_dir, Replica_ids, block_size_KB):
    block_size = block_size_KB * 1024  # MB → bytes
    F_LIST = []
    for replica_id in Replica_ids:
        file_number = replica_id.split('-')[1]
        file_name = f'replica_{file_number}.txt'
        F_LIST.append(file_name)

    replicas_lst = []
    for file_name in F_LIST: # 💡 This line is changed
        file_path = os.path.join(save_dir, file_name)
        try:
            if os.path.exists(file_path) and file_name.endswith(".txt"):
                with open(file_path, "rb") as f:
                    chunks = pickle.load(f)
                replica_data = bytearray().join(chunks)

                blocks = [replica_data[i:i+block_size] for i in range(0, len(replica_data), block_size)]

                replicas_lst.append(blocks)
        except (IndexError, ValueError):
            print(f"Warning: Invalid replica ID format '{replica_id}'. Skipping.")

    return replicas_lst


# def Modify_data(Data_replicas, Replica_no, b_no):
#     r_no = int(Replica_no.split('-')[1])

#     d_block = Data_replicas[r_no][b_no]
#     d_block[90:92] = b"#!!"

#     Data_replicas[r_no][b_no] = d_block

#     return Data_replicas


def index_alloc(loc_index,  T_es, Total_data, N_r):
    with open(loc_index, 'w', newline='') as f:
        writer = csv.writer(f)
        
        # Write header row
        header = [f'Edge-Server-{i+1}' for i in range(T_es)]
        writer.writerow(header)
        
        # Generate unique R values for each column and sort them
        all_r_values = [f'R-{i+1}' for i in range(Total_data)]
        
        # For each column, assign unique R values and sort them
        columns_data = []
        for i in range(Total_data):
            # Shuffle and take first N_r values for this column, then sort
            shuffled = all_r_values.copy()
            random.shuffle(shuffled)
            column_values = shuffled[:N_r]
            # Sort by the number after "R-"
            column_values.sort(key=lambda x: int(x.split('-')[1]))
            columns_data.append(column_values)
        
        # Write rows by transposing the columns data
        for row_idx in range(N_r):
            row = [columns_data[col_idx][row_idx] for col_idx in range(Total_data)]
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
    data_list = deterministic_shuffle(replica, shuffle_key)
    node_counter = 0
    hashes = []
    current_level = []
    for i, item in enumerate(data_list):
        hash_i = hash_data_SHA_3(str(item), sec_code)
        if modify==True and i==1:
            hash_i = Modify_data_block_hash(hash_i)
        node = [f"{replica_id}-Node-{node_counter}", 0, i, True, hash_i]
        current_level.append(node)   # keep references so we can update in place on promotion
        hashes.append(hash_i)
        node_counter += 1
    loc_key = Loc_key_gen(hashes)
    return current_level, loc_key


def resequence_nodes_ES(data, R_ids):
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

def build_MHT(leaf_nodes, replica_id, shuffle_key, sec_code):
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




#_______________________________________ Proof generation _____________________________________________

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

def build_minimal_tree(G_leafs: List[List], A_info: List[List], sec_code: str) -> List[List]:
    """
    Binary minimal-tree reconstruction:
    - uses leaves (G_leafs) and additional nodes (A_info) from CS
    - repeatedly pairs siblings at each level to compute parents
    - returns the top/root node (max ln)
    """

    # load all available nodes keyed by (ln, pn)
    node_map = {}
    existing_ids = set()

    for n in (A_info or []):
        nn = list(n)
        node_map[(nn[1], nn[2])] = nn
        existing_ids.add(nn[0])

    for lf in (G_leafs or []):
        ll = list(lf)
        node_map[(ll[1], ll[2])] = ll
        existing_ids.add(ll[0])

    if not node_map:
        raise ValueError("No nodes to build proof tree.")

    made_progress = True
    while made_progress:
        made_progress = False

        # group keys by level
        levels = sorted({ln for (ln, pn) in node_map.keys()})
        for ln in levels:
            # for each even pn, try build parent if sibling exists
            pns = sorted([pn for (l, pn) in node_map.keys() if l == ln])
            for pn in pns:
                if pn % 2 != 0:
                    continue  # handle pairs only from even side
                left_key = (ln, pn)
                right_key = (ln, pn ^ 1)

                if left_key not in node_map or right_key not in node_map:
                    continue

                left = node_map[left_key]
                right = node_map[right_key]

                parent_ln = ln + 1
                parent_pn = pn // 2
                parent_key = (parent_ln, parent_pn)

                if parent_key in node_map:
                    continue

                merged_hash = hash_data_black_3(str(left[4]) + str(right[4]), sec_code)
                new_id = _next_unique_id(existing_ids, base_name="Node")
                parent = [new_id, parent_ln, parent_pn, False, merged_hash]
                node_map[parent_key] = parent
                made_progress = True

    # return node with max level (root of reconstructed minimal tree)
    root = max(node_map.values(), key=lambda n: n[1])
    return root


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

