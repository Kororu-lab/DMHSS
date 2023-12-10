import pymongo
import pandas as pd
from datetime import datetime
from tqdm import tqdm
import numpy as np

# MongoDB Connection Parameters
DB_NAME = "reddit"
SUBMISSION_COLLECTION_NAME = "stat_submissions"
COMMENT_COLLECTION_NAME = "stat_comments"
BATCH_SIZE = 50000  # Adjust based on memory capacity

# Scoring Hyperparameters
DECAY_RATE = 0.05

def connect_to_mongodb(db_name):
    client = pymongo.MongoClient()
    db = client[db_name]
    return db

def fetch_data(collection, skip, limit):
    cursor = collection.find({}, {"created_day": 1, "author": 1}).skip(skip).limit(limit)
    return pd.DataFrame(list(cursor))

def calculate_scores(df):
    df['created_day'] = pd.to_datetime(df['created_day'])
    df['count'] = 1

    # Count daily activities and reset index to align with time decay
    activity_scores = df.groupby(['author', 'created_day']).size().reset_index(name='activity_count')

    # Calculate days ago for time decay
    max_date = df['created_day'].max()
    activity_scores['days_ago'] = (max_date - activity_scores['created_day']).dt.days
    time_decay = np.exp(-DECAY_RATE * activity_scores['days_ago'])

    # Apply time decay to activity count
    activity_scores['time_based'] = activity_scores['activity_count'] * time_decay

    # Normalize scores
    min_score, max_score = activity_scores['time_based'].min(), activity_scores['time_based'].max()
    activity_scores['time_based'] = (activity_scores['time_based'] - min_score) / (max_score - min_score)

    # Set index back for updating MongoDB
    activity_scores.set_index(['author', 'created_day'], inplace=True)

    return activity_scores['time_based']

def process_and_update(db, collection_name, skip, limit):
    collection = db[collection_name]
    batch_data = fetch_data(collection, skip, limit)
    if not batch_data.empty:
        scores = calculate_scores(batch_data)
        update_scores_in_mongo(db, collection_name, scores)

def update_scores_in_mongo(db, collection_name, scores):
    collection = db[collection_name]
    for (author, created_day), score in tqdm(scores.items(), total=scores.size):
        query = {"author": author, "created_day": created_day.strftime('%Y-%m-%d')}
        update = {"$set": {"time_based": score}}
        collection.update_many(query, update)

# Main Execution
db = connect_to_mongodb(DB_NAME)

submissions_total = db[SUBMISSION_COLLECTION_NAME].count_documents({})
comments_total = db[COMMENT_COLLECTION_NAME].count_documents({})

print("Processing submissions...")
for skip in tqdm(range(0, submissions_total, BATCH_SIZE), desc="Submissions"):
    process_and_update(db, SUBMISSION_COLLECTION_NAME, skip, BATCH_SIZE)

print("Processing comments...")
for skip in tqdm(range(0, comments_total, BATCH_SIZE), desc="Comments"):
    process_and_update(db, COMMENT_COLLECTION_NAME, skip, BATCH_SIZE)

print("Processing complete.")
