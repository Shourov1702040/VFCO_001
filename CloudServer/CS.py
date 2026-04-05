import socket, threading, sys, pickle, random, time, os, csv, statistics, tempfile, scheduling
import pandas as pd
import Functionalities_CS as Functionalities
import AHP_weight_determination as weight_det
from pathlib import Path
from datetime import datetime
from frequency_determination import choose_f_next, write_f_next, event_triggered_frequency


# ____________________________ Experiment metadata ____________________________

EDI_METHOD = "DL-EDI"   # "DL-EDI" or "OR-EDI"

policies_list = ["MinFreq", "AvgFreq", "MaxFreq", "Event-Triggered", "VFCO"]
# policies_list = ["MinFreq", "MaxFreq", "Event-Triggered", "VFCO"]
scheduling_policy = policies_list[4]

# Experimental parameters for the current run
DATA_SCALE_EXP = 8              #2, 4, 8, 16, 32 ->8
CORRUPTION_RATIO = 0.05         #2, 3, 4, 5, 7 ->5
ACTIVE_USERS_EXP = 30           #10, 20, 30, 40, 50 -> 30
ARRIVAL_RATE_EXP = 5            #1, 3, 5, 7, 9 ->5
CPU_UTIL_EXP = 0.35             #0.25, 0.30, 0.35, 0.40, 0.45 -> 0.35
T_CONTROL = 300.0               # in sec. 180, 240, 300, 360, 420 ->300

F_initial = 0.5              # 0.167, 0.25, 0.3334, 0.41667, 0.5
NUM_EDGE_SERVERS_EXP = 4

EXP_ROOT = Path("C:/My Drive/PHD Works/Task 2& 3/Work 3 OFD/Code/VFCO_Experiments")
CONFIG_DIR = EXP_ROOT / "config"
# RAW_RUNS_DIR = EXP_ROOT / "raw_runs"
RAW_RUNS_DIR = EXP_ROOT / "logs"
DERIVED_DIR = EXP_ROOT / "derived"
FIG_DIR = EXP_ROOT / "figure_csv"
LOG_DIR = EXP_ROOT / "logs"
PLOTS_DIR = EXP_ROOT / "plots"

for p in [CONFIG_DIR, RAW_RUNS_DIR, DERIVED_DIR, FIG_DIR, LOG_DIR, PLOTS_DIR]:
    p.mkdir(parents=True, exist_ok=True)

RUN_ID = (
    f"{EDI_METHOD}_{scheduling_policy}_T{int(T_CONTROL)}"
    f"_CR{CORRUPTION_RATIO}_DS{DATA_SCALE_EXP}"
    f"_U{ACTIVE_USERS_EXP}_A{ARRIVAL_RATE_EXP}_CPU{CPU_UTIL_EXP}_"
)

RUN_DIR = RAW_RUNS_DIR / RUN_ID
RUN_DIR.mkdir(parents=True, exist_ok=True)

# Simplified layout: only four per-edge files directly inside RUN_DIR
parameter_loc = str(RUN_DIR)

static_values = pd.read_csv(str(CONFIG_DIR / "es_static.csv"))
last_row_series = static_values.iloc[-1]
_, _1, f_min_cc, f_max_cc = tuple(last_row_series.values)
match scheduling_policy:
    case "MinFreq":
        F_applied_initial = f_min_cc
    case "AvgFreq":
        F_applied_initial = (f_min_cc + f_max_cc) / 2.0
    case "MaxFreq":
        F_applied_initial = f_max_cc
    case "Event-Triggered":
        F_applied_initial = F_initial
    case "VFCO":
        F_applied_initial = F_initial
    case _:
        F_applied_initial = F_initial

# ____________________________ EDI setup __________________________

connected_clients = {}
connected_clients_lock = threading.Lock()
running = True

block_size = 512
replica_scale = 8
Total_data = 40
data_scale = DATA_SCALE_EXP
sample_scale = 32
total_clients = NUM_EDGE_SERVERS_EXP

csv_filename = "C:/My Drive/PHD Works/Task 2& 3/Work 3 OFD/Code/DL-EDI/edge_data.csv"
# Functionalities.index_alloc(csv_filename, total_clients, Total_data, data_scale)
edge_info = Functionalities.csv_to_edge_info(csv_filename)

data_loc = "C:/My Drive/PHD Works/Task 2& 3/Work 3 OFD/Code/replicas"
Data_replicas = Functionalities.load_replicas_from_dir(data_loc, block_size)

time_all = []
challenge_all_per_edge = {}
challenges, proof_all, loc_key_all = [], {}, {}

expected_proof_by_edge = {}
expected_loc_key_by_edge = {}
expected_lock = threading.Lock()

challenge_size_kb_by_edge = {}
frequencies_per_C_T = {}


# ____________________________ Bootstrap initialization __________________________

INITIAL_METRIC_VALUES = {
    "Edge-Server-1": {
        "f_applied": F_applied_initial,
        "u_avg": ACTIVE_USERS_EXP,
        "a_avg": ARRIVAL_RATE_EXP,
        "D_avg_s": 2.22,
        "tau_cpu_avg_s": 0.003,
        "U_cpu_avg": CPU_UTIL_EXP,
        "U_net_avg": 0.30,
        "b_net_avg_bytes": 3000.0,
    },
    "Edge-Server-2": {
        "f_applied": F_applied_initial,
        "u_avg": ACTIVE_USERS_EXP,
        "a_avg": ARRIVAL_RATE_EXP,
        "D_avg_s": 2.22,
        "tau_cpu_avg_s": 0.003,
        "U_cpu_avg": CPU_UTIL_EXP,
        "U_net_avg": 0.30,
        "b_net_avg_bytes": 3000.0,
    },
    "Edge-Server-3": {
        "f_applied": F_applied_initial,
        "u_avg": ACTIVE_USERS_EXP,
        "a_avg": ARRIVAL_RATE_EXP,
        "D_avg_s": 2.22,
        "tau_cpu_avg_s": 0.003,
        "U_cpu_avg": CPU_UTIL_EXP,
        "U_net_avg": 0.30,
        "b_net_avg_bytes": 3000.0,
    },
    "Edge-Server-4": {
        "f_applied": F_applied_initial,
        "u_avg": ACTIVE_USERS_EXP,
        "a_avg": ARRIVAL_RATE_EXP,
        "D_avg_s": 2.22,
        "tau_cpu_avg_s": 0.003,
        "U_cpu_avg": CPU_UTIL_EXP,
        "U_net_avg": 0.30,
        "b_net_avg_bytes": 3000.0,
    },
}

DEFAULT_B_NET_BYTES = 3000.0

# Final per-edge file format = previous window_summary format + window_start_ts/window_end_ts - es_id
METRICS_HEADER = [
    "run_id", "window_start_ts", "window_end_ts", "edi_method", "policy",
    "window_id", "control_interval_s", "slot_time_s", "f_applied",
    "tp", "tn", "n_verify", "u_avg", "a_avg", "D_avg_s",
    "tau_cpu_avg_s", "b_net_avg_bytes", "U_cpu_avg", "U_net_avg",
    "C_nominal_req_per_s", "cst_s", "usdt_s", "rc_comp_s",
    "rc_comm_bytes", "f_next"
]

# Internal compatibility header for choose_f_next()
LEGACY_FREQ_HEADER = [
    "window_id", "window_start_ts", "window_end_ts", "f_applied", "TP", "TN",
    "u_avg", "a_avg", "D_avg_s", "tau_cpu_avg_s", "b_net_avg_bytes",
    "U_cpu_avg", "U_net_avg", "f_next"
]


# ____________________________ Metrics state __________________________

es_metrics_state = {}
Verification_TP = {}
Verification_TN = {}
Verification_D_list = {}
Verification_BNET_list = {}
Verification_sent_count = {}
Verification_received_count = {}
last_send_ts = {}

schedule_by_slot = {}
all_events = []
S_slots = 0
slot_step = 6.0
es_labels_for_schedule = []

window_start_ts_str = ""
window_end_ts_str = ""

iteration_counter = 0
auto_running = False
window_finalized = False
window_finalize_lock = threading.Lock()

n_iter = 5
interval_sec = 6.0


# ____________________________ Utility functions __________________________

def _now_hhmmss_cc():
    s = datetime.now().strftime("%H:%M:%S.%f")
    return s[:-4]


def _to_float_safe(x, default=0.0):
    try:
        if x is None:
            return default
        s = str(x).strip()
        if s == "" or s.lower() in ("none", "nan"):
            return default
        return float(s)
    except Exception:
        return default


def _to_int_safe(x, default=0):
    try:
        if x is None:
            return default
        s = str(x).strip()
        if s == "" or s.lower() in ("none", "nan"):
            return default
        return int(float(s))
    except Exception:
        return default


def _metrics_file_for_es(es_id: str) -> str:
    return os.path.join(parameter_loc, f"{es_id}-metrics.csv")


def _read_all_metrics_rows(csv_path: str):
    try:
        if not os.path.exists(csv_path):
            return []
        with open(csv_path, "r", newline="") as f:
            return list(csv.DictReader(f))
    except Exception as e:
        print(f"[Metrics] Failed reading all rows from {csv_path}: {e}")
        return []


def _read_last_metrics_row(csv_path: str):
    rows = _read_all_metrics_rows(csv_path)
    return rows[-1] if rows else None


def _count_metrics_rows(csv_path: str) -> int:
    return len(_read_all_metrics_rows(csv_path))


def _ensure_metrics_file_exists(csv_path: str):
    try:
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        if not os.path.exists(csv_path):
            with open(csv_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=METRICS_HEADER)
                writer.writeheader()
    except Exception as e:
        print(f"[Metrics] Failed creating metrics file {csv_path}: {e}")


def _initialize_bootstrap_row_if_needed(es_id: str):
    # Keep only the per-edge summary files with the final schema.
    # No bootstrap data row is written; only the header is ensured.
    csv_path = _metrics_file_for_es(es_id)
    _ensure_metrics_file_exists(csv_path)


def _build_compat_metrics_csv_for_frequency(es_id: str) -> str:
    """
    Build a temporary compatibility CSV using the legacy metrics schema
    so choose_f_next() can keep working without changing external modules.
    """
    current_csv = _metrics_file_for_es(es_id)
    rows = _read_all_metrics_rows(current_csv)

    fd, temp_path = tempfile.mkstemp(prefix=f"{es_id}_compat_", suffix=".csv")
    os.close(fd)

    try:
        with open(temp_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=LEGACY_FREQ_HEADER)
            writer.writeheader()

            for row in rows:
                writer.writerow({
                    "window_id": row.get("window_id", ""),
                    "window_start_ts": row.get("window_start_ts", ""),
                    "window_end_ts": row.get("window_end_ts", ""),
                    "f_applied": row.get("f_applied", ""),
                    "TP": row.get("tp", row.get("TP", "")),
                    "TN": row.get("tn", row.get("TN", "")),
                    "u_avg": row.get("u_avg", ""),
                    "a_avg": row.get("a_avg", ""),
                    "D_avg_s": row.get("D_avg_s", ""),
                    "tau_cpu_avg_s": row.get("tau_cpu_avg_s", ""),
                    "b_net_avg_bytes": row.get("b_net_avg_bytes", ""),
                    "U_cpu_avg": row.get("U_cpu_avg", ""),
                    "U_net_avg": row.get("U_net_avg", ""),
                    "f_next": row.get("f_next", ""),
                })
    except Exception:
        try:
            os.remove(temp_path)
        except Exception:
            pass
        raise

    return temp_path


def _write_f_next_current_file(csv_path: str, f_next_value: float):
    rows = _read_all_metrics_rows(csv_path)
    if not rows:
        raise RuntimeError(f"No data row found in {csv_path} to update f_next.")

    rows[-1]["f_next"] = str(float(f_next_value))

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=METRICS_HEADER)
        writer.writeheader()
        writer.writerows(rows)


def _read_es_static(es_id: str):
    es_static_csv = CONFIG_DIR / "es_static.csv"

    if not es_static_csv.exists():
        print(f"[Config] WARNING: {es_static_csv} not found. Using defaults.")
        return {
            "C_nominal_req_per_s": 100.0,
            "f_min": 0.1,
            "f_max": 1.0,
        }

    try:
        with open(es_static_csv, "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("es_id") == es_id:
                    return {
                        "C_nominal_req_per_s": _to_float_safe(row.get("C_nominal_req_per_s"), 100.0),
                        "f_min": _to_float_safe(row.get("f_min"), 0.1),
                        "f_max": _to_float_safe(row.get("f_max"), 1.0),
                    }
    except Exception as e:
        print(f"[Config] Failed reading es_static.csv for {es_id}: {e}")

    print(f"[Config] WARNING: es_id={es_id} not found in es_static.csv. Using defaults.")
    return {
        "C_nominal_req_per_s": 100.0,
        "f_min": 0.1,
        "f_max": 1.0,
    }


def _append_manifest_row(status="started", end_ts=""):
    manifest_path = CONFIG_DIR / "experiment_manifest.csv"
    header = [
        "run_id", "edi_method", "policy", "control_interval_s", "corruption_ratio",
        "data_scale", "active_users_avg", "arrival_rate_avg", "cpu_util_avg",
        "num_edge_servers", "start_ts", "end_ts", "status"
    ]

    row = {
        "run_id": RUN_ID,
        "edi_method": EDI_METHOD,
        "policy": scheduling_policy,
        "control_interval_s": T_CONTROL,
        "corruption_ratio": CORRUPTION_RATIO,
        "data_scale": DATA_SCALE_EXP,
        "active_users_avg": ACTIVE_USERS_EXP,
        "arrival_rate_avg": ARRIVAL_RATE_EXP,
        "cpu_util_avg": CPU_UTIL_EXP,
        "num_edge_servers": NUM_EDGE_SERVERS_EXP,
        "start_ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "end_ts": end_ts,
        "status": status,
    }

    file_exists = manifest_path.exists()
    try:
        with open(manifest_path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
    except Exception as e:
        print(f"[Manifest] Failed appending experiment_manifest.csv: {e}")


def _update_manifest_completed():
    manifest_path = CONFIG_DIR / "experiment_manifest.csv"
    if not manifest_path.exists():
        return

    try:
        with open(manifest_path, "r", newline="") as f:
            rows = list(csv.DictReader(f))

        for row in rows:
            if row.get("run_id") == RUN_ID:
                row["end_ts"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                row["status"] = "completed"

        if rows:
            with open(manifest_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)

    except Exception as e:
        print(f"[Manifest] Failed updating experiment_manifest.csv: {e}")


# ____________________________ Finalization helpers __________________________

def _all_expected_responses_received():
    with connected_clients_lock:
        es_ids = list(connected_clients.keys())

    for es_id in es_ids:
        sent_n = Verification_sent_count.get(es_id, 0)
        recv_n = Verification_received_count.get(es_id, 0)
        if recv_n < sent_n:
            return False
    return True


def _print_window_progress():
    with connected_clients_lock:
        es_ids = list(connected_clients.keys())

    print("[FinalizeCheck] Sent/Received status:")
    for es_id in es_ids:
        sent_n = Verification_sent_count.get(es_id, 0)
        recv_n = Verification_received_count.get(es_id, 0)
        print(f"  {es_id}: sent={sent_n}, received={recv_n}")


def _wait_and_finalize_window(retry_count=0, max_retry=30, retry_delay=1.0):
    global window_finalized

    with window_finalize_lock:
        if window_finalized:
            return

        if _all_expected_responses_received():
            print("[FinalizeCheck] All expected responses received. Finalizing window now.")
            window_finalized = True
            _finalize_window_and_write_metrics()
            return

        _print_window_progress()

        if retry_count >= max_retry:
            print("[FinalizeCheck] Timeout waiting for all responses. Finalizing with available results.")
            window_finalized = True
            _finalize_window_and_write_metrics()
            return

    threading.Timer(
        retry_delay,
        lambda: _wait_and_finalize_window(retry_count + 1, max_retry, retry_delay)
    ).start()


# ____________________________ Frequency loading and scheduling __________________________

def _load_es_f_applied_from_metrics(es_id: str) -> float:
    csv_path = _metrics_file_for_es(es_id)
    _initialize_bootstrap_row_if_needed(es_id)

    last_row = _read_last_metrics_row(csv_path)

    if last_row is None:
        default_f = INITIAL_METRIC_VALUES.get(es_id, {}).get("f_applied", F_applied_initial)
        es_metrics_state[es_id] = {
            "last_row": None,
            "csv_path": csv_path,
            "f_applied_sec": default_f
        }
        return default_f

    f_next_sec = _to_float_safe(
        last_row.get("f_next", ""),
        default=INITIAL_METRIC_VALUES.get(es_id, {}).get("f_applied", F_applied_initial)
    )
    if f_next_sec <= 0:
        f_next_sec = _to_float_safe(
            last_row.get("f_applied", ""),
            default=INITIAL_METRIC_VALUES.get(es_id, {}).get("f_applied", F_applied_initial)
        )

    es_metrics_state[es_id] = {
        "last_row": last_row,
        "csv_path": csv_path,
        "f_applied_sec": f_next_sec
    }
    return f_next_sec


def _build_schedule_from_connected():
    global schedule_by_slot, all_events, S_slots, slot_step, es_labels_for_schedule, n_iter, interval_sec, frequencies_per_C_T

    with connected_clients_lock:
        es_labels_for_schedule = list(connected_clients.keys())

    es_labels_for_schedule.sort()

    F_counts = []
    for es_id in es_labels_for_schedule:
        f_applied_sec = es_metrics_state.get(es_id, {}).get(
            "f_applied_sec",
            INITIAL_METRIC_VALUES.get(es_id, {}).get("f_applied", F_applied_initial)
        )
        fi = int(round(f_applied_sec * T_CONTROL))
        if fi < 0:
            fi = 0
        F_counts.append(fi)

    schedule_by_slot, all_events, S_slots, slot_step_local = scheduling.frequency_wise_schedule(
        F_counts, T_CONTROL, es_labels=es_labels_for_schedule
    )

    slot_step = slot_step_local if slot_step_local and slot_step_local > 0 else 0.0
    n_iter = int(S_slots) if S_slots else 0
    interval_sec = slot_step if slot_step and slot_step > 0 else 1.0

    print("\n[Schedule] Connected ES labels:", es_labels_for_schedule)
    print("[Schedule] F_applied (iter/sec):", {
        es: es_metrics_state[es]["f_applied_sec"] for es in es_labels_for_schedule
    })

    frequencies_per_C_T = dict(zip(es_labels_for_schedule, F_counts))
    print("[Schedule] F per control interval:", frequencies_per_C_T)
    print(f"[Schedule] S(n_iter)={n_iter}, slot_step(interval_sec)={interval_sec}")


# ____________________________ Derived metric computation __________________________

def _compute_window_metrics(row: dict, es_id: str):
    tp = _to_int_safe(row.get("tp", row.get("TP")), 0)
    tn = _to_int_safe(row.get("tn", row.get("TN")), 0)

    n_verify = _to_int_safe(row.get("n_verify"), tp + tn)
    if n_verify <= 0 and (tp + tn) > 0:
        n_verify = tp + tn

    f_applied = _to_float_safe(row.get("f_applied"), 0.0)
    u_avg = _to_float_safe(row.get("u_avg"), 0.0)
    a_avg = _to_float_safe(row.get("a_avg"), 0.0)
    D_avg_s = _to_float_safe(row.get("D_avg_s"), 0.0)
    tau_cpu_avg_s = _to_float_safe(row.get("tau_cpu_avg_s"), 0.0)
    b_net_avg_bytes = _to_float_safe(row.get("b_net_avg_bytes"), 0.0)

    es_static = _read_es_static(es_id)
    C_nominal = es_static["C_nominal_req_per_s"]

    slot_time_s = T_CONTROL / max(1, n_verify)
    cst_s = slot_time_s * tn *1.043276

    C_eff = max(1e-9, C_nominal * max(1e-6, (1.0 - f_applied * D_avg_s)))
    alpha = min(0.999999, a_avg / C_eff)
    gamma = 1.0 / max(1e-6, (1.0 - alpha))
    usdt_s = T_CONTROL * u_avg * (gamma - 1.0)

    rc_comp_s = tau_cpu_avg_s * n_verify
    rc_comm_bytes = b_net_avg_bytes * n_verify

    return {
        "slot_time_s": slot_time_s,
        "n_verify": n_verify,
        "C_nominal_req_per_s": C_nominal,
        "cst_s": cst_s,
        "usdt_s": usdt_s,
        "rc_comp_s": rc_comp_s,
        "rc_comm_bytes": rc_comm_bytes,
    }


def _append_window_summary(es_id: str):
    # No separate window_summary.csv file anymore.
    return


def _write_run_summary():
    # No separate run_summary.csv file anymore.
    return


# ____________________________ Metrics writing __________________________

def _append_new_window_row_for_es(es_id: str):
    state = es_metrics_state.get(es_id, {})
    csv_path = state.get("csv_path", _metrics_file_for_es(es_id))
    _ensure_metrics_file_exists(csv_path)

    last_row = _read_last_metrics_row(csv_path)

    init_vals = INITIAL_METRIC_VALUES.get(es_id, {
        "f_applied": F_applied_initial,
        "u_avg": ACTIVE_USERS_EXP,
        "a_avg": ARRIVAL_RATE_EXP,
        "tau_cpu_avg_s": 0.003,
        "U_cpu_avg": CPU_UTIL_EXP,
        "U_net_avg": 0.30,
        "b_net_avg_bytes": DEFAULT_B_NET_BYTES,
    })

    f_applied_sec = state.get(
        "f_applied_sec",
        _to_float_safe(
            last_row.get("f_next") if last_row else "",
            init_vals.get("f_applied", F_applied_initial)
        )
    )

    prev_window_id = _to_int_safe(last_row.get("window_id") if last_row else "", default=0)
    new_window_id = 1 if prev_window_id <= 0 else prev_window_id + 1

    u_avg = _to_float_safe(
        last_row.get("u_avg") if last_row else init_vals.get("u_avg"),
        init_vals.get("u_avg", ACTIVE_USERS_EXP)
    )
    a_avg = _to_float_safe(
        last_row.get("a_avg") if last_row else init_vals.get("a_avg"),
        init_vals.get("a_avg", ARRIVAL_RATE_EXP)
    )
    tau_cpu_avg_s = _to_float_safe(
        last_row.get("tau_cpu_avg_s") if last_row else init_vals.get("tau_cpu_avg_s"),
        init_vals.get("tau_cpu_avg_s", 0.003)
    )
    U_cpu_avg = _to_float_safe(
        last_row.get("U_cpu_avg") if last_row else init_vals.get("U_cpu_avg"),
        init_vals.get("U_cpu_avg", CPU_UTIL_EXP)
    )
    U_net_avg = _to_float_safe(
        last_row.get("U_net_avg") if last_row else init_vals.get("U_net_avg"),
        init_vals.get("U_net_avg", 0.30)
    )
    prev_b_net_avg_bytes = _to_float_safe(
        last_row.get("b_net_avg_bytes") if last_row else init_vals.get("b_net_avg_bytes"),
        init_vals.get("b_net_avg_bytes", DEFAULT_B_NET_BYTES)
    )

    tp = Verification_TP.get(es_id, 0)
    tn = Verification_TN.get(es_id, 0)

    d_list = Verification_D_list.get(es_id, [])
    d_avg = (sum(d_list) / len(d_list)) if d_list else 0.0

    bnet_list = Verification_BNET_list.get(es_id, [])
    if bnet_list:
        bnet_avg_bytes = (sum(bnet_list) / len(bnet_list)) * 1024.0
    else:
        bnet_avg_bytes = prev_b_net_avg_bytes

    sent_n = Verification_sent_count.get(es_id, 0)
    recv_n = Verification_received_count.get(es_id, 0)

    metrics_input = {
        "f_applied": f_applied_sec,
        "tp": tp,
        "tn": tn,
        "n_verify": tp + tn,
        "u_avg": u_avg,
        "a_avg": a_avg,
        "D_avg_s": d_avg,
        "tau_cpu_avg_s": tau_cpu_avg_s,
        "b_net_avg_bytes": bnet_avg_bytes,
        "U_cpu_avg": U_cpu_avg,
        "U_net_avg": U_net_avg,
    }

    derived_metrics = _compute_window_metrics(metrics_input, es_id)

    row = {
        "run_id": RUN_ID,
        "window_start_ts": window_start_ts_str,
        "window_end_ts": window_end_ts_str,
        "edi_method": EDI_METHOD,
        "policy": scheduling_policy,
        "window_id": str(new_window_id),
        "control_interval_s": str(T_CONTROL),
        "slot_time_s": str(derived_metrics["slot_time_s"]),
        "f_applied": str(f_applied_sec),
        "tp": str(tp),
        "tn": str(tn),
        "n_verify": str(derived_metrics["n_verify"]),
        "u_avg": str(u_avg),
        "a_avg": str(a_avg),
        "D_avg_s": str(d_avg),
        "tau_cpu_avg_s": str(tau_cpu_avg_s),
        "b_net_avg_bytes": str(bnet_avg_bytes),
        "U_cpu_avg": str(U_cpu_avg),
        "U_net_avg": str(U_net_avg),
        "C_nominal_req_per_s": str(derived_metrics["C_nominal_req_per_s"]),
        "cst_s": str(derived_metrics["cst_s"]),
        "usdt_s": str(derived_metrics["usdt_s"]),
        "rc_comp_s": str(derived_metrics["rc_comp_s"]),
        "rc_comm_bytes": str(derived_metrics["rc_comm_bytes"]),
        "f_next": ""
    }

    try:
        with open(csv_path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=METRICS_HEADER)
            writer.writerow(row)

        print(f"[Metrics] Appended new window row to {csv_path}")
        print(
            f"[Metrics] Row summary for {es_id}: "
            f"sent={sent_n}, received={recv_n}, tp={tp}, tn={tn}, "
            f"D_avg_s={d_avg}, b_net_avg_bytes={bnet_avg_bytes}"
        )

        f_next_updated = next_frequency_determination(es_id)

        updated_last_row = _read_last_metrics_row(csv_path)
        updated_f = _to_float_safe(
            updated_last_row.get("f_next", ""),
            f_applied_sec
        ) if updated_last_row else f_applied_sec

        es_metrics_state[es_id] = {
            "last_row": updated_last_row,
            "csv_path": csv_path,
            "f_applied_sec": updated_f if updated_f > 0 else f_applied_sec
        }

        print(f"[Metrics] f_next updated for {es_id} -> {f_next_updated}")

    except Exception as e:
        print(f"[Metrics] ERROR while processing {es_id}: {repr(e)}")
        raise


def _finalize_window_and_write_metrics():
    global window_end_ts_str
    window_end_ts_str = _now_hhmmss_cc()

    with connected_clients_lock:
        es_ids = list(connected_clients.keys())

    for es_id in es_ids:
        _append_new_window_row_for_es(es_id)

    _update_manifest_completed()


# ____________________________ Challenge generation and communication __________________________

def generate_messages_for_edges():
    global challenges, proof_all, loc_key_all

    start_time_GC = time.time()
    challenges, proof_all, loc_key_all = Functionalities.generate_challenge(edge_info, Data_replicas)
    time_all.append(time.time() - start_time_GC)

    edge_keys = list(edge_info.keys())
    for i in range(len(edge_keys)):
        client_id = edge_keys[i]
        cha = challenges[i]
        challenge_all_per_edge[client_id] = cha


def send_message_to_client(target_client_id, message_list):
    with connected_clients_lock:
        if target_client_id in connected_clients:
            client_info = connected_clients[target_client_id]
            try:
                serialized_msg = pickle.dumps(message_list)
                client_info['socket'].sendall(serialized_msg)
                return serialized_msg
            except Exception as e:
                print(f"Error sending to [{target_client_id}]: {e}")
                remove_client(target_client_id)
                return None
        else:
            print(f"Client '{target_client_id}' not connected.")
            return None


def remove_client(client_id_to_remove):
    with connected_clients_lock:
        if client_id_to_remove in connected_clients:
            try:
                connected_clients[client_id_to_remove]['socket'].close()
            except Exception:
                pass
            del connected_clients[client_id_to_remove]
            print(f"[{client_id_to_remove}] removed.")


# ____________________________ Edge server handler __________________________

def handle_edge_server(edge_server_socket, edge_server_address):
    current_client_id = f"Unknown Client ({edge_server_address[1]})"

    try:
        data = edge_server_socket.recv(1024)
        if not data:
            return

        initial_message_from_client = data.decode()
        if ':' in initial_message_from_client:
            parts = initial_message_from_client.split(':', 1)
            current_client_id = parts[0].strip()

        with connected_clients_lock:
            connected_clients[current_client_id] = {
                'socket': edge_server_socket,
                'address': edge_server_address
            }

        f_applied_sec = _load_es_f_applied_from_metrics(current_client_id)
        Verification_TP.setdefault(current_client_id, 0)
        Verification_TN.setdefault(current_client_id, 0)
        Verification_D_list.setdefault(current_client_id, [])
        Verification_BNET_list.setdefault(current_client_id, [])
        Verification_sent_count.setdefault(current_client_id, 0)
        Verification_received_count.setdefault(current_client_id, 0)

        print(f"[Connect] {current_client_id} connected. f_applied(iter/sec)={f_applied_sec}")

        while True:
            data = edge_server_socket.recv(4096)
            if not data:
                break

            try:
                start_time_Verification = time.time()
                Response_edge = pickle.loads(data)

                E_id = Response_edge[0]
                proof_root_node = Response_edge[1]
                Loc_key_ES = Response_edge[2]

                with expected_lock:
                    Original_proof_root_node = expected_proof_by_edge.get(E_id)
                    Original_Loc_key_ES = expected_loc_key_by_edge.get(E_id)

                if Original_proof_root_node is None or Original_Loc_key_ES is None:
                    print(f"[Verify] No expected proof/loc_key stored for {E_id}. Skipping verification.")
                    continue

                if E_id in last_send_ts:
                    dur = time.time() - last_send_ts[E_id]
                    Verification_D_list.setdefault(E_id, []).append(dur)

                if Original_proof_root_node[4] == proof_root_node[4]:
                    print(f"{E_id}'s data is integral")
                    Verification_TP[E_id] = Verification_TP.get(E_id, 0) + 1
                else:
                    print(f"{E_id}'s data is not integral")
                    Verification_TN[E_id] = Verification_TN.get(E_id, 0) + 1
                    loc_result = Functionalities.Detection_function_from_dicts(
                        Original_Loc_key_ES, Loc_key_ES
                    )
                    print(f"Corrupted data replica and block: {loc_result}")

                Verification_received_count[E_id] = Verification_received_count.get(E_id, 0) + 1

                time_all.append(time.time() - start_time_Verification)

                time_all_sum = [sum(time_all[i:i+3]) for i in range(0, len(time_all), 3)]
                avg_time_for_verification = sum(time_all_sum) / len(time_all_sum) if time_all_sum else 0.0
                print(f"Average time per edge server for verification process: {avg_time_for_verification}")

                proof_size_kb = sys.getsizeof(str(Response_edge)) / 1024.0
                challenge_size_kb = challenge_size_kb_by_edge.get(E_id, 0.0)
                total_communication_cost_kb = round((challenge_size_kb + proof_size_kb), 3)

                Verification_BNET_list.setdefault(E_id, []).append(total_communication_cost_kb)

                print(f"Total communication cost (Challenge+response): {total_communication_cost_kb} KB\n")

            except Exception as e:
                print(f"Error decoding client message from {current_client_id}: {e}")
                continue

    except Exception as e:
        print(f"Error with {current_client_id}: {e}")
    finally:
        remove_client(current_client_id)


# ____________________________ Next frequency determination __________________________

def next_frequency_determination(es_id):
    a12, a13, a23 = 5, 3, 2
    weights, CR, CI, lam_max, A = weight_det.ahp_weights(a12, a13, a23)

    es_static_csv = str(CONFIG_DIR / "es_static.csv")
    es_metrics_csv = os.path.join(parameter_loc, f"{es_id}-metrics.csv")

    _initialize_bootstrap_row_if_needed(es_id)

    print(f"\n[Freq] -------- next_frequency_determination START --------")
    print(f"[Freq] es_id = {es_id}")
    print(f"[Freq] es_static_csv = {es_static_csv}")
    print(f"[Freq] es_metrics_csv = {es_metrics_csv}")
    print(f"[Freq] es_static exists? {os.path.exists(es_static_csv)}")
    print(f"[Freq] es_metrics exists? {os.path.exists(es_metrics_csv)}")

    last_row = _read_last_metrics_row(es_metrics_csv)
    print(f"[Freq] last_row before choose_f_next = {last_row}")

    if last_row is None:
        raise RuntimeError(f"No metrics row found for {es_id}")

    f_applied_prev = _to_float_safe(last_row.get("f_applied"), F_applied_initial)
    D_avg_s = _to_float_safe(last_row.get("D_avg_s"), 0.0)

    # overload detection
    overload_ratio = f_applied_prev * D_avg_s
    if overload_ratio >= 1.0:
        print(f"[Freq] WARNING: overload detected for {es_id}. f_applied_prev * D_avg_s = {overload_ratio:.4f}")
        print(f"[Freq] Using safe fallback frequency instead of raw choose_f_next output.")
        es_static = _read_es_static(es_id)
        f_min = es_static["f_min"]
        f_max = es_static["f_max"]

        # safe_f = min(f_max, max(f_min, 0.7 / max(D_avg_s, 1e-6)))
        # safe_f = (f_min_cc + f_max_cc) / 2.0
        safe_f = f_max_cc
        _write_f_next_current_file(es_metrics_csv, safe_f)

        updated_last_row = _read_last_metrics_row(es_metrics_csv)
        print(f"[Freq] fallback safe_f = {safe_f}")
        print(f"[Freq] last_row after fallback write = {updated_last_row}")
        print(f"[Freq] -------- next_frequency_determination END --------\n")
        
        return safe_f

    compat_metrics_csv = None
    try:
        compat_metrics_csv = _build_compat_metrics_csv_for_frequency(es_id)

        result = choose_f_next(
            es_static_csv=es_static_csv,
            es_metrics_csv=compat_metrics_csv,
            es_id=es_id,
            T_control=T_CONTROL,
            weights=weights,
            weight_refs={"rho": 0.10, "a_avg": 5.0, "b_net": 2000.0},
            weight_exponents={"rho": 1.2, "a_avg": 1.2, "b_net": 1.2},
        )
    except Exception as e:
        print(f"[Freq] ERROR inside choose_f_next for {es_id}: {repr(e)}")
        raise
    finally:
        if compat_metrics_csv and os.path.exists(compat_metrics_csv):
            try:
                os.remove(compat_metrics_csv)
            except Exception:
                pass

    if not isinstance(result, tuple):
        raise RuntimeError(f"choose_f_next returned unexpected non-tuple value for {es_id}: {result}")

    if len(result) == 6:
        rho, f_next_vfco, f_per_T, f_min, f_max, f_applied_prev = result
    elif len(result) == 2:
        # fallback branch from choose_f_next
        print(f"[Freq] WARNING: choose_f_next returned 2 values for {es_id}: {result}")
        es_static = _read_es_static(es_id)
        f_min = es_static["f_min"]
        f_max = es_static["f_max"]

        rho = _to_float_safe(result[0], 0.0)
        f_next_vfco = _to_float_safe(result[1], f_applied_prev)
        f_per_T = int(round(f_next_vfco * T_CONTROL))
    else:
        raise RuntimeError(f"choose_f_next returned unexpected tuple length {len(result)} for {es_id}: {result}")

    match scheduling_policy:
        case "MinFreq":
            f_next_updated = f_min
        case "AvgFreq":
            f_next_updated = (f_min + f_max) / 2.0
        case "MaxFreq":
            f_next_updated = f_max
        case "Event-Triggered":
            f_next_updated = event_triggered_frequency(f_min, f_max, f_applied_prev, rho)
        case "VFCO":
            f_next_updated = f_next_vfco
        case _:
            f_next_updated = f_applied_prev

    f_next_updated = float(f_next_updated)
    print()

    print(f"[Freq] rho = {rho}")
    print(f"[Freq] f_per_T = {f_per_T}")
    print(f"[Freq] f_min = {f_min}")
    print(f"[Freq] f_max = {f_max}")
    print(f"[Freq] f_applied_prev = {f_applied_prev}")
    print(f"[Freq] computed f_next_updated = {f_next_updated}")

    try:
        _write_f_next_current_file(es_metrics_csv, f_next_updated)
        print(f"[Freq] f_next updated successfully in {es_metrics_csv} for {es_id}")
    except Exception as e:
        print(f"[Freq] ERROR updating f_next for {es_id}: {repr(e)}")
        raise

    updated_last_row = _read_last_metrics_row(es_metrics_csv)
    print(f"[Freq] last_row after f_next update = {updated_last_row}")
    print(f"[Freq] -------- next_frequency_determination END --------\n")

    return f_next_updated


# ____________________________ Auto execution loop __________________________

def box_animation(duration=2.5, delay=0.0225):
    end_time = time.time() + duration
    white_box = "█"
    black_box = "░"
    N_cycle = 25

    print(f"\n\nEDI verification round: {iteration_counter}")

    while time.time() < end_time:
        for i in range(N_cycle):
            if time.time() >= end_time:
                break
            boxes = [white_box] * N_cycle
            boxes[i] = black_box
            print(f"\r Verifying Edge Data Integrity {''.join(boxes)}", end="", flush=True)
            time.sleep(delay)

        for i in range(N_cycle - 2, 0, -1):
            if time.time() >= end_time:
                break
            boxes = [white_box] * N_cycle
            boxes[i] = black_box
            print(f"\r Verifying Edge Data Integrity {''.join(boxes)}", end="", flush=True)
            time.sleep(delay)

    print("\r Verification Done! " + " " * 15)


def auto_execute():
    global iteration_counter, auto_running, window_start_ts_str

    if not running or not auto_running or iteration_counter >= n_iter:
        auto_running = False
        return

    iteration_counter += 1
    box_animation(duration=2.5)

    if iteration_counter == 1:
        window_start_ts_str = _now_hhmmss_cc()

    try:
        generate_messages_for_edges()

        events = schedule_by_slot.get(iteration_counter, [])
        if not events:
            print(f"[Schedule] No ES scheduled in slot/iteration {iteration_counter}")
        else:
            for ev in events:
                if ev.es_index < len(es_labels_for_schedule):
                    client_id = es_labels_for_schedule[ev.es_index]
                else:
                    continue

                if client_id in challenge_all_per_edge:
                    start_time_MSend = time.time()
                    serialized_msg = send_message_to_client(client_id, challenge_all_per_edge[client_id])
                    if serialized_msg is not None:
                        time_all.append(time.time() - start_time_MSend)
                        last_send_ts[client_id] = time.time()
                        Verification_sent_count[client_id] = Verification_sent_count.get(client_id, 0) + 1
                        challenge_size_kb_by_edge[client_id] = len(serialized_msg) / 1024.0

                        with expected_lock:
                            if client_id in proof_all:
                                expected_proof_by_edge[client_id] = proof_all[client_id]
                            if client_id in loc_key_all:
                                expected_loc_key_by_edge[client_id] = loc_key_all[client_id]
                else:
                    print(f"No message available for {client_id}")

    except Exception as e:
        print(f"[AutoMode] Error during cycle #{iteration_counter}: {e}")

    if iteration_counter < n_iter:
        threading.Timer(interval_sec, auto_execute).start()
    else:
        auto_running = False
        print(f"\n[AutoMode] Finished all {n_iter} iterations.")
        threading.Timer(1.0, _wait_and_finalize_window).start()


# ____________________________ Command handler __________________________

def server_command_handler():
    global running, auto_running, iteration_counter, window_finalized

    print("\nCloud_server Command Interface:")
    print("Commands: 'send', 'list', 'exit'")

    while running:
        try:
            command_line = input("Cloud_server Command: ").strip().lower()

            if command_line == 'exit':
                running = False
                auto_running = False
                break

            elif command_line == 'list':
                with connected_clients_lock:
                    if not connected_clients:
                        print("No active Edge servers.")
                    else:
                        print("--- Connected Edge Servers ---")
                        for client_id in connected_clients:
                            addr = connected_clients[client_id]['address']
                            print(f"- {client_id} (Address: {addr[0]}:{addr[1]})")

            elif command_line == 'send':
                if not connected_clients:
                    print("[AutoMode] No connected Edge servers to send data.")
                    continue

                if not auto_running:
                    _build_schedule_from_connected()

                    if n_iter <= 0:
                        print("[AutoMode] scheduling produced n_iter=0 (no verifications).")
                        continue

                    iteration_counter = 0
                    window_finalized = False

                    with expected_lock:
                        expected_proof_by_edge.clear()
                        expected_loc_key_by_edge.clear()

                    challenge_size_kb_by_edge.clear()

                    for es_id in list(connected_clients.keys()):
                        Verification_TP[es_id] = 0
                        Verification_TN[es_id] = 0
                        Verification_D_list[es_id] = []
                        Verification_BNET_list[es_id] = []
                        Verification_sent_count[es_id] = 0
                        Verification_received_count[es_id] = 0

                    print(f"[AutoMode] Starting scheduled sending every {interval_sec} seconds for {n_iter} iterations...")
                    auto_running = True
                    threading.Timer(1, auto_execute).start()
                else:
                    print("[AutoMode] Already running auto-send loop.")

            else:
                print("Unknown command.")
                print("Valid Commands: 'send', 'list', 'exit'")

        except EOFError:
            running = False
            auto_running = False
            break
        except Exception as e:
            print(f"Command handler error: {e}")


# ____________________________ Main cloud server setup __________________________

_append_manifest_row(status="started")

Cloud_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
Cloud_server.bind(('0.0.0.0', 12345))
Cloud_server.listen(5)
print("Cloud_server listening on port 12345...")

command_thread = threading.Thread(target=server_command_handler, daemon=True)
command_thread.start()

while running:
    try:
        Cloud_server.settimeout(1.0)
        edge_server_socket, edge_server_address = Cloud_server.accept()
        Cloud_server.settimeout(None)

        client_thread = threading.Thread(
            target=handle_edge_server,
            args=(edge_server_socket, edge_server_address),
            daemon=True
        )
        client_thread.start()

    except socket.timeout:
        continue
    except KeyboardInterrupt:
        running = False
        Cloud_server.close()
        break
    except Exception as e:
        print(f"Accept error: {e}")

with connected_clients_lock:
    for client_id in list(connected_clients.keys()):
        remove_client(client_id)

Cloud_server.close()
print("Cloud_server shut down.")