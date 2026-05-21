# --------------------------------------------------------------------
# sendData.py
# --------------------------------------------------------------------
# Simple TCP server that streams a text file line‑by‑line to a single
# client.  The server listens on all interfaces (0.0.0.0) and port 9999.
# --------------------------------------------------------------------

import socket
import time

# Host and port for the server to listen on
host = '0.0.0.0'
port = 9999

# --------------------------------------------------------------------
# Create a blocking socket and bind it to HOST:PORT
# --------------------------------------------------------------------
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((host, port))
s.listen(1)  # allow one pending connection
print(f"Listening on {host}:{port}...")

# --------------------------------------------------------------------
# Accept a single client connection
# --------------------------------------------------------------------
conn, addr = s.accept()
print(f"Connection from {addr} established.")

# --------------------------------------------------------------------
# Open the input file and stream each line to the client.
# Each line is stripped of trailing newlines, re‑encoded as UTF‑8,
# and sent with a newline byte.  A short sleep is added to pace
# the stream; remove or adjust if you need higher throughput.
# --------------------------------------------------------------------
with open("/home/Downloads/FarsiDataWrongTest.txt", "r", encoding="utf-8") as f:
    for line in f:
        conn.sendall(line.strip().encode() + b"\n")
        time.sleep(1)

# --------------------------------------------------------------------
# Clean up the sockets
# --------------------------------------------------------------------
conn.close()
s.close()
