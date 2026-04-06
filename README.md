## Working Modules

### 1. Cloud Server (CS)
* **Functionalities_CS.py**: Contains the backend operations and logic for the Cloud Server.
* **CS.py**: The main execution file. It simulates CS operations, including sending challenges, receiving responses, and performing integrity verification.

### 2. Edge Server (ES)
* **Functionalities_ES.py**: Contains the backend operations and logic for the Edge Server.
* **ES_N.py**: The main execution file for the Edge Server. It handles receiving challenges, generating integrity proofs, and returning them to the CS. Each instance of this file represents an individual Edge Server.

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
1.  **First**, run the Cloud Server:
    ```bash
