# Distributed Persian Text Correction Pipeline (Spark + Hadoop + MT5)

A distributed, real‑time text‑correction pipeline built with **Apache Spark Streaming**, **Hadoop HDFS**, and a **Flask‑based MT5 inference service**.  
This system streams Persian sentences over TCP, distributes inference across multiple nodes, and stores corrected outputs in HDFS.

---

## Overview

This project implements a **multi‑component distributed pipeline** for Persian text correction using a fine‑tuned `mT5` model.  
The system consists of three main modules:

1. **sendData.py**  
   Streams raw text lines over a TCP socket (port 9999).

2. **inferenceService.py**  
   A Flask API that loads the MT5 model once, runs inference on GPU/CPU, and exposes a `/correct` endpoint.

3. **correctData.py**  
   A Spark Streaming job that:
   - reads incoming text from the socket  
   - distributes inference requests across multiple inference servers  
   - collects corrected sentences  
   - writes results to **HDFS** as CSV  

This architecture enables **real‑time**, **scalable**, and **fault‑tolerant** text correction in a distributed environment.

---

## Architecture

```text
                +-----------------------+
                |   sendData.py         |
                |  (TCP text streamer)  |
                +-----------+-----------+
                            |
                            v
                +-----------------------+
                |  Spark Streaming      |
                |  processMulti.py      |
                |  - reads socket       |
                |  - repartitions RDD   |
                |  - round‑robin calls  |
                +-----------+-----------+
                            |
        -------------------------------------------------
        |                                               |
        v                                               v
+---------------+                             +----------------+
| inference     |                             | inference      |
| service #1    |                             | service #2     |
| port 5000     |                             | port 5001      |
+-------+-------+                             +-------+--------+
        |                                               |
        ------------------- corrected text --------------
                            |
                            v
                +-----------------------+
                |   HDFS (CSV output)   |
                |  hdfs://master:9000   |
                +-----------------------+


## Project Structure

```
.
├── processMulti.py        # Spark Streaming job (distributed inference + HDFS output)
├── inferenceService.py    # Flask inference API (loads MT5 model once)
├── sendData.py            # TCP text streamer
└── README.md
```

## Requirements
Python packages (for inference services)

```bash
pip install flask
pip install torch
pip install transformers
pip install requests
```
Spark & Hadoop
Apache Spark (Standalone cluster mode)
Hadoop HDFS
Spark master URL: spark://master:7077
HDFS namenode: hdfs://master:9000




