from pymongo import MongoClient
import pandas as pd
from tqdm import tqdm
import threading
import os

# Constants
NUM_THREADS = 4
FIELDS_TO_EXPORT = ['_id', 'id', 'created_utc', 'subreddit', 'author', 'title', 'selftext', 'score', 'num_comments']  # Adjust these fields as per your collection structure

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['reddit']

# List of subreddits to filter by, converted to lower case
intersection = [
    "findareddit", "NoStupidQuestions", "offmychest", "WritingPrompts",
    "self", "lgbt", "explainlikeimfive", "mentalhealth", "raisedbynarcissists",
    "skyrim", "unpopularopinion", "AskWomen", "Christianity", "jobs",
    "OutOfTheLoop", "relationship_advice", "AMA", "TwoXChromosomes",
    "AskOuija", "loseit", "Advice", "teenagers", "tifu",
    "AskMen", "AmItheAsshole", "confession", "NoFap", "atheism", "copypasta",
    "relationships", "TrueOffMyChest", "AskDocs", "dating_advice",
    "CasualConversation", "depression", "rant", "sex", "Tinder", "Drugs",
    "Anxiety", "legaladvice", "TooAfraidToAsk", "ADHD", "tipofmytongue",
    "askscience"
]

mh_subreddits = [
    "depression", "mentalhealth", "traumatoolbox", "BipolarReddit", 
    "BPD", "ptsd", "psychoticreddit", "EatingDisorders", "StopSelfHarm", 
    "survivorsofabuse", "rapecounseling", "hardshipmates", 
    "panicparty", "socialanxiety"
]

# Union of the two sets to remove duplicates and then convert back to list
subreddit_lst = list(set(intersection).union(mh_subreddits))

# Optional: Convert to lower case for case-insensitive matching
subreddit_lst = [sub.lower() for sub in subreddit_lst]

# Directory for storing chunks
if not os.path.exists('./submissions'):
    os.makedirs('./submissions')

def fetch_and_filter_data(thread_id, collection_name, output_file, fields_to_export):
    with tqdm(desc=f"Thread-{thread_id}") as pbar:
        cursor = db[collection_name].find({})
        for index, document in enumerate(cursor):
            if index % NUM_THREADS == thread_id:
                subreddit = document.get('subreddit', '').lower()
                if subreddit in subreddit_lst:
                    # Select only the specified fields
                    filtered_doc = {field: document.get(field, '') for field in fields_to_export}
                    df = pd.DataFrame([filtered_doc])
                    df.to_csv(output_file, mode='a', header=False, index=False)
            pbar.update(1)

# Use multi-threading to fetch data in parallel
threads = []

for i in range(NUM_THREADS):
    output_file = f"./submissions/filtered_submissions_{i}.csv"

    # Ensure the file exists and write header for the first file
    with open(output_file, 'a') as file:
        if i == 0:
            sample_doc = db['submissions'].find_one()
            df_headers = pd.DataFrame([sample_doc])
            df_headers.to_csv(file, index=False, mode='w')
    
    thread = threading.Thread(target=fetch_and_filter_data, args=(i, "submissions", output_file, FIELDS_TO_EXPORT))
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()

# Merge all chunk files
all_files = [f"./submissions/filtered_submissions_{i}.csv" for i in range(NUM_THREADS)]
df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
df.to_csv("filtered_submissions_by_subreddit.csv", index=False)
