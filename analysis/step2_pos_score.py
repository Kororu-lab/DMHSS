import pymongo
import pandas as pd
from datetime import datetime
from tqdm import tqdm
import numpy as np

# MongoDB Connection Parameters
DB_NAME = "reddit"
SUBMISSION_COLLECTION_NAME = "filtered_submissions_standard"
COMMENT_COLLECTION_NAME = "filtered_comments_standard"
BATCH_SIZE = 10000

# Scoring Hyperparameters
W1 = 0.6  # Weight for daily score
W2 = 0.4  # Weight for monthly score
DECAY_RATE = 0.05  # Decay rate for time-decay function

def connect_to_mongodb(db_name, collection_name):
    client = pymongo.MongoClient()
    db = client[db_name]
    collection = db[collection_name]
    return collection

def calculate_and_store_scores(collection, batch_size):
    pipeline = [
        {"$project": {"author": 1, "created_day": 1}},
        {"$sort": {"created_day": 1}},
        {"$skip": 0},
        {"$limit": batch_size}
    ]
    skip = 0
    while True:
        pipeline[2]["$skip"] = skip
        batch = list(collection.aggregate(pipeline, allowDiskUse=True))
        if not batch:
            break

        df = pd.DataFrame(batch)
        df['created_day'] = pd.to_datetime(df['created_day'])

        # Calculate daily and monthly scores
        daily_scores = df.groupby(['author', 'created_day'])['_id'].count().diff().fillna(0)
        monthly_scores = df.groupby(['author', pd.Grouper(key='created_day', freq='M')])['_id'].count().diff().fillna(0)

        # Combine scores with weights and apply time-decay
        max_date = df['created_day'].max()
        df['days_ago'] = (max_date - df['created_day']).dt.days
        df['time_based'] = (W1 * daily_scores + W2 * monthly_scores) * np.exp(-DECAY_RATE * df['days_ago'])

        # Normalize 'time_based' scores
        df['time_based'] = (df['time_based'] - df['time_based'].min()) / (df['time_based'].max() - df['time_based'].min())

        # Update MongoDB Documents
        for _, row in df.iterrows():
            collection.update_one({'_id': row['_id']}, {'$set': {'time_based': row['time_based']}})

        skip += batch_size
        tqdm.write(f"Processed batch up to skip = {skip}")

# Process Submissions and Comments
submissions_collection = connect_to_mongodb(DB_NAME, SUBMISSION_COLLECTION_NAME)
comments_collection = connect_to_mongodb(DB_NAME, COMMENT_COLLECTION_NAME)

calculate_and_store_scores(submissions_collection, BATCH_SIZE)
calculate_and_store_scores(comments_collection, BATCH_SIZE)

print("Processing complete.")
