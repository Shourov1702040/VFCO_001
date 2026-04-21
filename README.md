# VFCO: Verification Frequency Control and Optimization

VFCO is an adaptive framework that determines how often edge data integrity verification should be performed. It monitors system conditions (e.g., verification results, workload, and resource usage) and dynamically adjusts verification frequency using a multi-objective optimization approach. The goal is to minimize corruption exposure while reducing service degradation and resource overhead.

---

## Setup and Execution

To run the verification process, distribute the files and configure the network as follows:

### Step 1: Cloud Server Setup
Place the following files on the Cloud Server device (Windows, Linux, or other machines):
* `CS.py`
* `Functionalities_CS.py`
* `AHP_weight_determination.py`
* `frequency_determination.py`
* `scheduling.py`
* `EM_GRID.py`

### Step 2: Edge Server Setup
Place the following file on the Edge Server device (e.g., Raspberry Pi):
* `ES_N.py`
* `Functionalities_CS.py`
* `edge_data.csv`
* `EM_GRID.py`

### Step 3: Network Configuration
1.  Identify the IP address of the **Cloud Server** device.
2.  Open the `ES_N.py` file on the Edge Server.
3.  Update the server IP address field to match the Cloud Server's current network IP.

### Step 4: Execution
1.  **First**, run the cloud server code CS.py
2.  Then run edge servers one by one, ES1.py, ES2.py.....ESn.py 
3.  In the cloud server program, enter the 'send' command
