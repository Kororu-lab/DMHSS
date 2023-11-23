from pymongo import MongoClient
import pandas as pd
from tqdm import tqdm
import threading
import os

# Constants
NUM_THREADS = 8
SUBREDDIT_CHUNK_SIZE = 100  # or any other number that suits your data size and memory capacity

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['reddit']

# List of subreddits to filter by
subreddit_lst = []  # Fill in your list of subreddits

# Split the subreddit list into chunks for multi-threading
chunks = [subreddit_lst[i:i + SUBREDDIT_CHUNK_SIZE] for i in range(0, len(subreddit_lst), SUBREDDIT_CHUNK_SIZE)]

# Hardcoded total count for comments
TOTAL_COMMENTS = 10248412613

# Directory for storing chunks
if not os.path.exists('./comments'):
    os.makedirs('./comments')

def fetch_data_by_subreddit_chunk(subreddit_chunk, collection_name, output_file):
    with tqdm(total=len(subreddit_chunk), desc=f"Thread-{threading.current_thread().name}") as pbar:
        for subreddit in subreddit_chunk:
            cursor = db[collection_name].find({"subreddit": subreddit})
            data = list(cursor)
            if data:
                df = pd.DataFrame(data)
                df.to_csv(output_file, mode='a', header=False, index=False)
            pbar.update(1)

# Use multi-threading to fetch data in parallel
threads = []
for i, subreddit_chunk in enumerate(chunks):
    output_file = f"./comments/filtered_comments_{i}.csv"
    # Write the header for each chunk file
    sample_doc = db['comments'].find_one()
    df_headers = pd.DataFrame([sample_doc])
    df_headers.to_csv(output_file, index=False, mode='w')
    
    thread = threading.Thread(target=fetch_data_by_subreddit_chunk, args=(subreddit_chunk, "comments", output_file))
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()

# Merge all chunk files
all_files = [f"./comments/filtered_comments_{i}.csv" for i in range(len(chunks))]
df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
df.to_csv("filtered_comments.csv", index=False)
