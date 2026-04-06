# EM_Grid.py
from typing import List, Dict, Any, Tuple
import hashlib

USE_REAL_BLS = False
try:
    from blspy import AugSchemeMPL, G2Element
    USE_REAL_BLS = True
except Exception:
    USE_REAL_BLS = False

SIM_MOD = 2**256 - 189

def hash_data_SHA_3(data, sec_code: str) -> str:
    if isinstance(data, str):
        data = data.encode()
    if isinstance(sec_code, str):
        sec_code = sec_code.encode()
    return hashlib.sha3_256(data + sec_code).hexdigest()


def deterministic_seed_from_replica(blocks: List[Any]) -> bytes:
    normalized = []
    for b in blocks:
        if isinstance(b, (bytes, bytearray)):
            normalized.append(b.decode("utf-8", errors="ignore"))
        else:
            normalized.append(str(b))
    concat = "|".join(normalized).encode("utf-8")
    return hashlib.sha256(concat).digest()


def int_from_seed(seed: bytes) -> int:
    return int.from_bytes(seed, "big") % SIM_MOD


# -------- BLS wrappers --------
if USE_REAL_BLS:
    def bls_keygen(seed: bytes):
        sk = AugSchemeMPL.key_gen(seed)
        pk = sk.get_g1()
        return sk, pk

    def bls_sign(sk, message: bytes):
        return AugSchemeMPL.sign(sk, message)

    def bls_serialize_sig(sig) -> str:
        try:
            return sig.serialize().hex()
        except AttributeError:
            return bytes(sig).hex()

    def bls_verify(pk, message: bytes, sig_hex: str) -> bool:
        try:
            sig = G2Element.from_bytes(bytes.fromhex(sig_hex))
            return AugSchemeMPL.verify(pk, message, sig)
        except Exception:
            return False
else:
    def _hash_to_int(b: bytes) -> int:
        return int(hashlib.sha256(b).hexdigest(), 16) % SIM_MOD

    def bls_keygen(seed: bytes) -> Tuple[int, int]:
        sk = int_from_seed(seed) or 1
        pk = sk
        return sk, pk

    def bls_sign(sk_int: int, message: bytes) -> int:
        return (sk_int * _hash_to_int(message)) % SIM_MOD

    def bls_serialize_sig(sig_int: int) -> str:
        return hex(sig_int)[2:]

    def bls_verify(pk_int: int, message: bytes, sig_hex: str) -> bool:
        sig_int = int(sig_hex, 16)
        h = _hash_to_int(message)
        return (pk_int * h) % SIM_MOD == sig_int % SIM_MOD


def build_em_grid_bls(blocks: List[Any], sec_code: str) -> Dict[str, Any]:
    if not isinstance(blocks, list) or len(blocks) == 0:
        raise ValueError("blocks must be a non-empty list")

    n = len(blocks)
    seed = deterministic_seed_from_replica(blocks)
    sk, pk = bls_keygen(seed)

    # block hashes
    h = [hash_data_SHA_3(blocks[i] if isinstance(blocks[i], (bytes, bytearray)) else str(blocks[i]), sec_code)
         for i in range(n)]

    # grid G (hash + signature)
    G = [[None for _ in range(n)] for _ in range(n)]
    for i in range(n):
        for j in range(n):
            node_hash = hash_data_SHA_3(h[i] + h[j], sec_code)
            sig = bls_sign(sk, node_hash.encode("utf-8"))
            G[i][j] = {"hash": node_hash, "sig": bls_serialize_sig(sig)}

    # row hashes + sigs
    row_hashes, row_sigs = [], []
    for i in range(n):
        concat_row = ''.join(G[i][j]["hash"] for j in range(n))
        r_hash = hash_data_SHA_3(concat_row, sec_code)
        r_sig = bls_sign(sk, r_hash.encode("utf-8"))
        row_hashes.append(r_hash)
        row_sigs.append(bls_serialize_sig(r_sig))

    # col hashes + sigs
    col_hashes, col_sigs = [], []
    for j in range(n):
        concat_col = ''.join(G[i][j]["hash"] for i in range(n))
        c_hash = hash_data_SHA_3(concat_col, sec_code)
        c_sig = bls_sign(sk, c_hash.encode("utf-8"))
        col_hashes.append(c_hash)
        col_sigs.append(bls_serialize_sig(c_sig))

    # build merkle root for a list (duplicate last leaf if odd)
    def merkle_root(vals: List[str]) -> str:
        cur = list(vals)
        while len(cur) > 1:
            nxt = []
            for k in range(0, len(cur), 2):
                left = cur[k]
                right = cur[k + 1] if k + 1 < len(cur) else cur[k]
                nxt.append(hash_data_SHA_3(left + right, sec_code))
            cur = nxt
        return cur[0]

    TR_root = merkle_root(row_hashes)
    TC_root = merkle_root(col_hashes)

    EM_root_hash = hash_data_SHA_3(TR_root + TC_root, sec_code)
    EM_root_sig = bls_serialize_sig(bls_sign(sk, EM_root_hash.encode("utf-8")))

    return {
        "n": n,
        "pk": pk,
        "EM_root_hash": EM_root_hash,
        "EM_root_sig": EM_root_sig,
        "row_hashes": row_hashes,
        "col_hashes": col_hashes,
    }