import pandas as pd
import csv
from pymongo import MongoClient
import os
import warnings
import json
from tqdm import tqdm

# Constants
WORKLOAD_DIR = "./workload.now"
CHECKPOINT_FILE = os.path.join(WORKLOAD_DIR, "checkpoint.txt")
SKIP_FILE = os.path.join(WORKLOAD_DIR, "skip.txt")
DB_NAME = "reddit"
CHUNK_SIZE = 1000  # Reduced CHUNK_SIZE for memory considerations

# Establish connection to MongoDB
client = MongoClient()
db = client[DB_NAME]

def is_file_completed(file_path, checkpoints):
    return checkpoints.get(file_path, {}).get("completed", False)

def get_checkpoint(file_path, checkpoints):
    return checkpoints.get(file_path, {}).get("rows_processed", 0)

def update_checkpoint(file_path, rows_processed, completed=False):
    checkpoints = {}
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            checkpoints = json.load(f)
    if file_path not in checkpoints:
        checkpoints[file_path] = {}
    checkpoints[file_path]["rows_processed"] = rows_processed
    checkpoints[file_path]["completed"] = completed
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoints, f)

def log_skipped_rows(file_path, count):
    with open(SKIP_FILE, "a") as f:
        f.write(f"Skipped {count} rows from {file_path}\n")

def process_file(file_path, collection_name, checkpoints):
    if is_file_completed(file_path, checkpoints):
        print(f"Skipping {file_path} as it has already been processed.")
        return

    collection = db[collection_name]
    rows_processed = get_checkpoint(file_path, checkpoints)
    warnings_count = 0

    # Custom warning handler to count the number of warnings
    def warn_count(*args, **kwargs):
        nonlocal warnings_count
        warnings_count += 1
        return warnings.showwarning(*args, **kwargs)

    # Open the CSV file and set up a CSV reader
    with open(file_path, 'r', encoding='utf-8') as f:
        filtered_lines = (line for line in f if '\x00' not in line)
        reader = csv.reader(filtered_lines)
        
        # Read the header
        header = next(reader)

        # Skip any previously processed rows
        for _ in range(rows_processed):
            next(reader)

        with warnings.catch_warnings(record=True):
            warnings.showwarning = warn_count

            # Process rows in chunks and insert each chunk into MongoDB
            chunk = []
            for row in tqdm(reader, desc=f"Processing {file_path}"):
                # Convert row to dictionary
                row_dict = dict(zip(header, row))
                chunk.append(row_dict)
                
                if len(chunk) == CHUNK_SIZE:
                    collection.insert_many(chunk)
                    chunk = []
                rows_processed += 1

            # Insert any remaining rows from the last chunk
            if chunk:
                collection.insert_many(chunk)

        # Log the number of skipped rows to skip.txt for every file
        log_skipped_rows(file_path, warnings_count)

        # Update the checkpoint to indicate completion
        update_checkpoint(file_path, rows_processed, completed=True)

def main():
    csv.field_size_limit(2147483647)  # Set to maximum possible value
    checkpoints = {}
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            checkpoints = json.load(f)
    
    # Process all RC_* files (Comments)
    for file in sorted(os.listdir(WORKLOAD_DIR)):
        if file.startswith("sorted_") and file.endswith("comments.csv"):
            process_file(os.path.join(WORKLOAD_DIR, file), "comments_filtered", checkpoints)
    
    # Process all RS_* files (Submissions)
    for file in sorted(os.listdir(WORKLOAD_DIR)):
        if file.startswith("sorted_") and file.endswith("comments.csv"):
            process_file(os.path.join(WORKLOAD_DIR, file), "submissions_filtered", checkpoints)

if __name__ == "__main__":
    main()
