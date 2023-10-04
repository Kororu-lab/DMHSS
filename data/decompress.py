import zstandard as zstd
import json
import csv
import os
from tqdm import tqdm
from collections import defaultdict

def decompress_and_write_to_csv(input_file_path, output_file_path):
    # Initialize a set to hold all unique fields (columns) found in the data
    all_fields = set()
    
    # Temporary storage for the comments
    comments = []

    with open(input_file_path, 'rb') as f:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(f) as reader:
            decompressed_data = reader.read().decode('utf-8')
            lines = decompressed_data.split('\n')
            
            # Wrap lines with tqdm for progress monitoring
            for line in tqdm(lines, desc=f"Processing {os.path.basename(input_file_path)}", unit="lines"):
                if not line:  # skip empty lines
                    continue
                comment = json.loads(line)
                all_fields.update(comment.keys())
                comments.append(comment)

    # Write data to CSV
    with open(output_file_path, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=list(all_fields))
        writer.writeheader()
        for comment in comments:
            writer.writerow(comment)

    print(f"Data from {input_file_path} has been extracted to {output_file_path}.")

# Directory paths
input_dir = "compressed"
output_dir = "processed"

# Ensure output directory exists
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Iterate over all zst files in the input directory
for filename in os.listdir(input_dir):
    if filename.endswith(".zst"):
        input_file_path = os.path.join(input_dir, filename)
        output_file_path = os.path.join(output_dir, filename.replace(".zst", ".csv"))
        decompress_and_write_to_csv(input_file_path, output_file_path)
