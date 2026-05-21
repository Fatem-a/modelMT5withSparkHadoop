# ---------------------------------------------
# inferenceService.py
# ---------------------------------------------
# A Flask‑based inference server that loads a single
# MT5 model on startup and exposes a POST endpoint
# `/correct` for sentence correction.  The model is
# loaded once, moved to GPU if available, and kept
# in evaluation mode to speed up inference.
# ---------------------------------------------

from flask import Flask, request, jsonify
import torch
from transformers import MT5ForConditionalGeneration, MT5Tokenizer
import subprocess, os

# ---------------------------------------------
# Paths for the model – first try the local cache,
# otherwise pull from HDFS.
# ---------------------------------------------
LOCAL_MODEL_PATH = "/tmp/models"
HDFS_MODEL_PATH = "/models/models"

# If the local directory is missing or empty, fetch it from HDFS
if not os.path.exists(LOCAL_MODEL_PATH) or not os.listdir(LOCAL_MODEL_PATH):
    subprocess.run(["hdfs", "dfs", "-get", HDFS_MODEL_PATH, LOCAL_MODEL_PATH], check=True)

MODEL_PATH = LOCAL_MODEL_PATH

# ---------------------------------------------
# Flask app initialization
# ---------------------------------------------
app = Flask(__name__)

# ---------------------------------------------
# Load tokenizer & model once at startup
# ---------------------------------------------
print("Loading MT5 model once...")
tokenizer = MT5Tokenizer.from_pretrained(MODEL_PATH)
model = MT5ForConditionalGeneration.from_pretrained(MODEL_PATH)

# Move the model to GPU if available; otherwise CPU
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model.to(device)
model.eval()      # set the model to evaluation mode

# ---------------------------------------------
# /correct endpoint – accepts JSON {"sentences": [...]}
# ---------------------------------------------
@app.route("/correct", methods=["POST"])
def correct():
    # Parse the request body as JSON
    data = request.get_json(force=True)
    sentences = data.get("sentences", [])

    # If no sentences were supplied, return an empty list
    if not sentences:
        return jsonify({"corrected": []})

    # Tokenise the sentences and move tensors to the right device
    inputs = tokenizer(
        sentences,
        return_tensors="pt",
        padding=True,
        truncation=True,
        max_length=128
    ).to(device)

    # Run inference in a no‑grad context for speed
    with torch.no_grad():
        outputs = model.generate(
            input_ids=inputs["input_ids"],
            attention_mask=inputs["attention_mask"],
            max_length=128,
            num_beams=2,
            early_stopping=True
        )

    # Decode the model outputs back into strings
    corrected = [tokenizer.decode(o, skip_special_tokens=True) for o in outputs]

    # Return the corrected sentences as JSON
    return jsonify({"corrected": corrected})

# ---------------------------------------------
# Start the Flask development server
# ---------------------------------------------
if __name__ == "__main__":
    # Bind to all interfaces so the service can be reached
    # from other nodes in the cluster
    app.run(host="0.0.0.0", port=5000)
