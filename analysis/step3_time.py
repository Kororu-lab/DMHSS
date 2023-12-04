import pymongo
import pandas as pd
import matplotlib.pyplot as plt
import os
from tqdm import tqdm
import numpy as np

# Hyperparameters
DB_NAME = "reddit"
SUBMISSION_COLLECTION_NAME = "filtered_submissions_standard"
COMMENT_COLLECTION_NAME = "filtered_comments_standard"
TIME_WINDOW = "1D"
BATCH_SIZE = 10000
SUBREDDIT_GROUPS = ["SW", "MH", "Otr", "ALL"]
USER_GROUPS = ["SW", "MH", "Otr"]

def connect_to_mongodb(db_name, collection_name):
    client = pymongo.MongoClient()
    db = client[db_name]
    collection = db[collection_name]
    return collection

def batch_aggregate_user_activity(collection, batch_size, subreddit_group):
    pipeline = [
        {"$match": {"subreddit_grp": subreddit_group}} if subreddit_group != "ALL" else {"$match": {}},
        {"$project": {"author": 1, "created_utc": 1, "usr_grp": 1}},
        {"$sort": {"created_utc": 1}},
        {"$skip": 0},
        {"$limit": batch_size}
    ]
    skip = 0
    while True:
        pipeline[3]["$skip"] = skip
        batch = list(collection.aggregate(pipeline, allowDiskUse=True))
        if not batch:
            break
        yield batch
        skip += batch_size

def calculate_activity_scores(df, time_window):
    df['time_window'] = pd.to_datetime(df['created_utc'], unit='s')
    df.set_index('time_window', inplace=True)
    resampled_df = df.resample(time_window).count()
    resampled_df['activity_change_score'] = resampled_df['author'].diff().abs()
    return resampled_df['activity_change_score'].mean()

def plot_group_activity(data, group_name, save_dir="./step3/"):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    plt.figure(figsize=(12, 6))
    data.plot(kind='line', title=f"Average Activity Pattern for Group: {group_name}")
    plt.xlabel("Time Window")
    plt.ylabel("Average Activity Score")
    plt.savefig(os.path.join(save_dir, f"{group_name}_activity.png"))
    plt.close()

# Main Execution
for subreddit_group in SUBREDDIT_GROUPS:
    group_scores = {usr_grp: pd.DataFrame() for usr_grp in USER_GROUPS}
    for collection_name in ["submissions", "comments"]:
        collection = connect_to_mongodb(DB_NAME, SUBMISSION_COLLECTION_NAME if collection_name == "submissions" else COMMENT_COLLECTION_NAME)
        print(f"Processing {collection_name} data for subreddit group {subreddit_group}...")
        for batch in tqdm(batch_aggregate_user_activity(collection, BATCH_SIZE, subreddit_group)):
            df = pd.DataFrame(batch)
            df['time_window'] = pd.to_datetime(df['created_utc'], unit='s')
            df.set_index('time_window', inplace=True)
            for usr_grp in USER_GROUPS:
                group_df = df[df['usr_grp'] == usr_grp]
                if not group_df.empty:
                    score_series = group_df.resample(TIME_WINDOW).count()['author'].diff().abs()
                    if group_scores[usr_grp].empty:
                        group_scores[usr_grp] = score_series
                    else:
                        group_scores[usr_grp] = pd.concat([group_scores[usr_grp], score_series])
    
    # Plotting Group Activity
    for usr_grp, scores_df in group_scores.items():
        if not scores_df.empty:
            scores_df = scores_df.resample(TIME_WINDOW).mean()
            plot_group_activity(scores_df, f"{subreddit_group}_{usr_grp}")

print("Processing complete.")
