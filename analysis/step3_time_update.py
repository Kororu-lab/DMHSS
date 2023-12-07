import pymongo
import pandas as pd
from datetime import datetime
from tqdm import tqdm
import numpy as np

# MongoDB Connection Parameters
DB_NAME = "reddit"
SUBMISSION_COLLECTION_NAME = "filtered_submissions_standard"
COMMENT_COLLECTION_NAME = "filtered_comments_standard"
BATCH_SIZE = 10000  # Adjust as needed

# Scoring Hyperparameters
W1 = 0.6  # Weight for daily score
W2 = 0.4  # Weight for monthly score
DECAY_RATE = 0.05  # Decay rate for time-decay function

def connect_to_mongodb(db_name):
    client = pymongo.MongoClient()
    db = client[db_name]
    return db

def fetch_data(collection, batch_size, skip):
    cursor = collection.find({}, {"created_day": 1, "author": 1}).skip(skip).limit(batch_size)
    return pd.DataFrame(list(cursor))

def process_batch(df):
    df['created_day'] = pd.to_datetime(df['created_day'])
    df['count'] = 1

    # Calculate daily scores
    daily_scores = df.groupby(['author', 'created_day']).count()['count'].groupby('author').diff().fillna(0)

    # Calculate monthly scores
    df['month'] = df['created_day'].dt.to_period('M')
    monthly_counts = df.groupby(['author', 'month'])['count'].sum()
    monthly_scores = monthly_counts.groupby('author').diff().fillna(0)

    # Combine daily and monthly scores
    combined_scores = W1 * daily_scores + W2 * monthly_scores

    # Flatten the multi-index of combined_scores
    combined_scores = combined_scores.reset_index(name='score')

    # Calculate days ago for time decay
    max_date = df['created_day'].max()
    combined_scores['days_ago'] = (max_date - combined_scores['created_day']).dt.days

    # Apply time decay and normalize scores
    combined_scores['time_based'] = (combined_scores['score'] * np.exp(-DECAY_RATE * combined_scores['days_ago']))
    min_score, max_score = combined_scores['time_based'].min(), combined_scores['time_based'].max()
    combined_scores['time_based'] = (combined_scores['time_based'] - min_score) / (max_score - min_score)

    # Set index back for updating MongoDB
    combined_scores.set_index(['author', 'created_day'], inplace=True)

    return combined_scores['time_based']

def update_scores_in_mongo(db, collection_name, scores):
    collection = db[collection_name]
    for (author, created_day), score in tqdm(scores.items(), total=scores.size):
        query = {"author": author, "created_day": created_day.strftime('%Y-%m-%d')}
        update = {"$set": {"time_based": score}}
        collection.update_many(query, update)

# Main Execution
db = connect_to_mongodb(DB_NAME)

def process_and_update(collection_name):
    collection = db[collection_name]
    total_docs = collection.count_documents({})
    for skip in tqdm(range(0, total_docs, BATCH_SIZE), desc=f"Processing {collection_name}"):
        batch_data = fetch_data(collection, BATCH_SIZE, skip)
        if batch_data.empty:
            continue
        scores = process_batch(batch_data)
        update_scores_in_mongo(db, collection_name, scores)

print("Processing submissions...")
process_and_update(SUBMISSION_COLLECTION_NAME)

print("Processing comments...")
process_and_update(COMMENT_COLLECTION_NAME)

print("Processing complete.")
