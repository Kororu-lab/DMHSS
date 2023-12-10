import csv
import numpy as np
from pymongo import MongoClient
from collections import defaultdict
from tqdm import tqdm
import os

### Hyperparameters and Configurations ###
DB_NAME = "reddit"
OUTPUT_DIR = "./integrated/"
OUTPUT_FILE = OUTPUT_DIR + "top500_scores.csv"
ORDER = "descending"  # Can be "ascending" or "descending"
NORMALIZE = True  # Set to False if scores are already normalized
TOP_N = 500  # Number of top/low users to retrieve
BATCH_SIZE = 10000  # Batch size for processing

# Score field names in MongoDB
SENTIMENT_SUBMISSION_FIELD = 'sentiment_score_complex'
SENTIMENT_COMMENT_FIELD = 'sentiment_score'
POS_FIELD = 'pos_score'
GRAPH_FIELD = 'graph'
TIME_BASED_FIELD = 'time_based_score'

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Weights for submissions and comments
SUBMISSION_WEIGHT = 2
COMMENT_WEIGHT = 1

# MongoDB Collections
COLLECTION_SUBMISSIONS_SENTIMENT = "filtered_submissions_sentiment"
COLLECTION_COMMENTS_SENTIMENT = "filtered_comments_sentiment"
COLLECTION_SUBMISSIONS_STANDARD = "filtered_submissions_standard"
COLLECTION_COMMENTS_STANDARD = "filtered_comments_standard"

# MongoDB Connection
client = MongoClient()
db = client[DB_NAME]

# Define normalization ranges and weights for integration
norm_ranges = {
    'sentiment': (0, 1),
    'pos': (0, 1),
    'graph': (0, 1),
    'time_based': (0, 1)
}

weights = {
    'sentiment': 50,
    'pos': 0,
    'graph': 0,
    'time_based': 0
}

# Functions for score normalization and integration
def normalize_score(score, score_type):
    """ Normalize the score to a 0-1 range """
    min_score, max_score = norm_ranges[score_type]
    return (score - min_score) / (max_score - min_score)

def integrate_scores(user_data):
    """ Calculate the integrated score for a user """
    if NORMALIZE:
        integrated_score = sum(weights[type] * normalize_score(user_data[type], type) for type in weights)
    else:
        integrated_score = sum(weights[type] * user_data[type] for type in weights)
    return integrated_score

# Initialize user scores
users_scores = defaultdict(lambda: defaultdict(list))

# Function to process a batch of documents
def process_batch(docs, weight, score_fields):
    for doc in docs:
        for field, score_key in score_fields.items():
            weighted_score = doc.get(score_key, 0) * weight
            users_scores[doc["author"]][field].append(weighted_score)

# Fetch and process documents in batches
def process_documents(collection_name, score_fields, weight):
    collection = db[collection_name]
    last_id = None
    while True:
        query = {'_id': {'$gt': last_id}} if last_id else {}
        cursor = collection.find(query).limit(BATCH_SIZE)
        batch_docs = list(cursor)

        # Process current batch
        process_batch(batch_docs, weight, score_fields)
        if not batch_docs:
            break
        last_id = batch_docs[-1]['_id']

# Score fields for sentiment and standard collections
sentiment_fields = {
    'sentiment': SENTIMENT_SUBMISSION_FIELD if 'submissions' in COLLECTION_SUBMISSIONS_SENTIMENT else SENTIMENT_COMMENT_FIELD
}
standard_fields = {
    'pos': POS_FIELD,
    'graph': GRAPH_FIELD,
    'time_based': TIME_BASED_FIELD
}

# Process sentiment and standard collections
process_documents(COLLECTION_SUBMISSIONS_SENTIMENT, sentiment_fields, SUBMISSION_WEIGHT)
process_documents(COLLECTION_COMMENTS_SENTIMENT, sentiment_fields, COMMENT_WEIGHT)
process_documents(COLLECTION_SUBMISSIONS_STANDARD, standard_fields, SUBMISSION_WEIGHT)
process_documents(COLLECTION_COMMENTS_STANDARD, standard_fields, COMMENT_WEIGHT)

# Calculate integrated scores for each user
users_integrated_scores = {}
for user, scores in tqdm(users_scores.items(), desc="Integrating Scores"):
    user_data = {
        'sentiment': np.mean(scores['sentiment']),
        'pos': np.mean(scores['pos']) if scores['pos'] else 0,
        'graph': np.mean(scores['graph']) if scores['graph'] else 0,
        'time_based': np.mean(scores['time_based']) if scores['time_based'] else 0
    }
    integrated_score = integrate_scores(user_data)
    users_integrated_scores[user] = integrated_score

# Sort users by their integrated scores and select the top/low N
sorted_users = sorted(users_integrated_scores.items(), key=lambda x: x[1], reverse=(ORDER == "descending"))[:TOP_N]

# Write user scores to CSV
with open(OUTPUT_FILE, "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["Rank", "User", "Integrated_Score"])
    for rank, (user, score) in enumerate(sorted_users, start=1):
        writer.writerow([rank, user, score])

# Close MongoDB connection
client.close()

print(f"Finished generating {OUTPUT_FILE}.")
