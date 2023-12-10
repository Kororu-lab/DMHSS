import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pymongo import MongoClient
import numpy as np

# Hyperparameters
DB_NAME = 'reddit'
COMMENTS_COLLECTION_NAME = 'filtered_comments_standard'
SUBMISSIONS_COLLECTION_NAME = 'filtered_submissions_standard'
INTERACTION_WEIGHT = 1
UNIQUE_USER_WEIGHT = 2
SUBREDDIT_DIVERSITY_WEIGHT = 3
OUTLIER_THRESHOLD_RATIO = 0.90  # Top 20% are considered outliers
BATCH_SIZE = 500  # Adjustable batch size for processing large datasets
OVERWRITE = False  # Set to True to overwrite existing 'comm_score' field

client = MongoClient()
db = client[DB_NAME]

def calculate_communication_scores(subreddit_group=None):
    match_stage = {}
    if subreddit_group and subreddit_group != "ALL":
        match_stage = {"subreddit_grp": subreddit_group}

    pipeline = [
        {"$match": match_stage},
        {
            "$unionWith": {
                "coll": SUBMISSIONS_COLLECTION_NAME,
                "pipeline": [{"$project": {"author": 1, "user_grp": 1, "subreddit_grp": 1, "created_day": 1}}]
            }
        },
        {
            "$addFields": {"month": {"$substr": ["$created_day", 0, 7]}}
        },
        {
            "$group": {
                "_id": {"author": "$author", "month": "$month"},
                "total_interactions": {"$sum": 1},
                "unique_users": {"$addToSet": "$parent_author"},
                "unique_subreddits": {"$addToSet": "$subreddit_grp"},
                "user_grp": {"$first": "$user_grp"}  # Correctly extract user_grp
            }
        },
        {
            "$group": {
                "_id": {"author": "$_id.author", "month": "$_id.month"},
                "user_grp": {"$first": "$user_grp"},
                "avg_comm_score": {"$avg": {
                    "$add": [
                        {"$multiply": ["$total_interactions", INTERACTION_WEIGHT]},
                        {"$multiply": [{"$size": "$unique_users"}, UNIQUE_USER_WEIGHT]},
                        {"$multiply": [{"$size": "$unique_subreddits"}, SUBREDDIT_DIVERSITY_WEIGHT]}
                    ]
                }}
            }
        },
        {
            "$project": {
                "author": "$_id.author",
                "month": "$_id.month",
                "user_grp": 1,
                "avg_comm_score": 1
            }
        }
    ]

    # Using batch size in the aggregation
    raw_scores = list(db[COMMENTS_COLLECTION_NAME].aggregate(pipeline, batchSize=BATCH_SIZE))
    df = pd.DataFrame(raw_scores)

    # Extract 'author' and 'month' from '_id', drop '_id' afterwards
    df['author'] = df['_id'].apply(lambda x: str(x.get('author')))  # Ensure author is a string
    df['month'] = df['_id'].apply(lambda x: x.get('month'))
    df.drop(columns=['_id'], inplace=True)

    return normalize_scores_with_threshold(df)

def normalize_scores_with_threshold(data):
    df = pd.DataFrame(data)
    threshold = df['avg_comm_score'].quantile(OUTLIER_THRESHOLD_RATIO)
    df['normalized_score'] = df['avg_comm_score'].apply(lambda x: min(x, threshold))
    df['normalized_score'] = (df['normalized_score'] - df['normalized_score'].min()) / (threshold - df['normalized_score'].min())
    return df

def update_database_with_scores(scores_df, collection_name):
    collection = db[collection_name]

    for index, row in scores_df.iterrows():
        query = {"author": row['author'], "created_day": {"$regex": "^" + row['month']}}
        new_values = {"$set": {"comm_score": row['avg_comm_score']}}

        if not OVERWRITE:
            query["comm_score"] = {"$exists": False}

        collection.update_many(query, new_values)

def main():
    all_scores = []
    for group in ["SW", "MH", "Otr"]:
        print(f"Processing {group}...")
        scores = calculate_communication_scores(group)
        scores['subreddit_grp'] = group
        all_scores.append(scores)

    combined_df = pd.concat(all_scores, ignore_index=True)
    update_database_with_scores(combined_df, COMMENTS_COLLECTION_NAME)
    update_database_with_scores(combined_df, SUBMISSIONS_COLLECTION_NAME)

    print("Database update complete.")

if __name__ == "__main__":
    main()
