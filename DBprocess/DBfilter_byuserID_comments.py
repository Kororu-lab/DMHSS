from pymongo import MongoClient
import pandas as pd
from tqdm import tqdm
import threading
import os

# Constants
NUM_THREADS = 8

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['reddit']

# Load user IDs from CSV
user_ids_df = pd.read_csv("usr_submission.csv")
user_ids_list = user_ids_df["author"].tolist()

# Split the user IDs list into chunks for multi-threading
chunks = [user_ids_list[i::NUM_THREADS] for i in range(NUM_THREADS)]

# Hardcoded total count for comments
TOTAL_COMMENTS = 10248412613

# Directory for storing chunks
if not os.path.exists('./comments'):
    os.makedirs('./comments')

def fetch_data_by_user_chunk(user_chunk, collection_name, output_file):
    with tqdm(total=len(user_chunk), desc=f"Thread {user_chunk[0]}") as pbar:
        for user_id in user_chunk:
            cursor = db[collection_name].find({"author": user_id})
            data = list(cursor)
            if data:
                df = pd.DataFrame(data)
                df.to_csv(output_file, mode='a', header=False, index=False)
            pbar.update(1)

# Use multi-threading to fetch data in parallel
threads = []
for i, user_chunk in enumerate(chunks):
    output_file = f"./comments/filtered_comments_{i}.csv"
    # Write the header for each chunk file
    sample_doc = db['comments'].find_one()
    df_headers = pd.DataFrame([sample_doc])
    df_headers.to_csv(output_file, index=False, mode='w')
    
    thread = threading.Thread(target=fetch_data_by_user_chunk, args=(user_chunk, "comments", output_file))
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()

# Merge all chunk files
all_files = [f"./comments/filtered_comments_{i}.csv" for i in range(NUM_THREADS)]
df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
df.to_csv("filtered_comments.csv", index=False)
