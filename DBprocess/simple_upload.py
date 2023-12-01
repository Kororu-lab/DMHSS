import pandas as pd
from tqdm import tqdm
import subprocess

def process_chunk(chunk):
    """Process each chunk of the dataframe."""
    chunk['[deleted]'] = chunk['[deleted]'].fillna(chunk['[deleted].1'])
    return chunk.drop(columns=['[deleted].1'])

# Define file paths
input_file = 'filtered_submissions_by_subreddit.csv'
output_file = 'modified_filtered_submissions.csv'

# Define chunk size
chunk_size = 10000  # Adjust this based on your memory constraints

# Initialize tqdm and process the file in chunks
with pd.read_csv(input_file, chunksize=chunk_size) as reader, tqdm(desc="Processing", unit="chunk") as pbar:
    for chunk in reader:
        processed_chunk = process_chunk(chunk)
        mode = 'a' if pbar.n > 0 else 'w'  # 'w' for the first chunk, 'a' for appending subsequent chunks
        header = pbar.n == 0  # Include header only for the first chunk
        processed_chunk.to_csv(output_file, mode=mode, index=False, header=header)
        pbar.update()

# MongoDB import command
mongoimport_cmd = f"mongoimport --type csv -d reddit -c filtered_submissions_bunch --headerline --file {output_file}"

# Execute the command
subprocess.run(mongoimport_cmd, shell=True)
