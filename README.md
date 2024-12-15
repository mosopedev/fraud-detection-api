# Fraud Detection API

This project demonstrates a **Fraud Detection System** using real-time processing with **Apache Flink**, **Kafka**, and a **Flask API**. It integrates a pre-trained ML model for fraud prediction and stores processed transactions in **Couchbase**.

<!-- 
## Features
- **Flask API**: Provides endpoints for transaction analytics.
- **Kafka Producer**: Simulates real-world transaction streams by sending transaction data to Kafka.
- **Flink Service**: Processes transactions in real time, predicts fraud, and ensures fault tolerance with checkpointing. Stores processed results in Couchbase.
 -->
 ## Prerequisites
Ensure the following are installed on your system:
- **Python 3.11.5 **
- **Kafka and Zookeeper**


## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/mosopedev/fraud-detection-api.git
   cd fraud-detection-api
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Start the Flask Server
Run the Flask server to expose API endpoints:
```bash
python start.py
```

### Run the Kafka Producer
Simulate real-world transaction data sent to Flink for real-time processing:
```bash
python kafka_producer.py
```

### Start the Flink Service
Run the Flink job to process transactions, predict fraud, and store results in Couchbase:
```bash
python flink.py
```

---

## System Architecture
1. **Kafka**:
   - Acts as the messaging system for streaming transaction data.

2. **Flink**:
   - Consumes data from Kafka, applies the pre-trained ML model to detect fraud, and stores processed results in Couchbase.
   - Implements fault tolerance with checkpointing to recover lost transactions.

3. **Flask**:
   - Provides an interface for interacting with the processed data.

4. **Couchbase**:
   - Stores the final processed and classified transactions for further analysis.

---

## Example Workflow
1. Kafka producer sends transaction data.
2. Flink consumes the data, processes it in real time, and applies fraud detection.
3. Processed transactions are stored in Couchbase for persistent storage.
4. Flask API allows interaction with the processed data.
