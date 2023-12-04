import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pymongo import MongoClient
from sklearn.preprocessing import MinMaxScaler
from tqdm import tqdm
import numpy as np

# Hyperparameters
DB_NAME = 'reddit'
COMMENTS_COLLECTION_NAME = 'filtered_comments_standard'
SUBMISSIONS_COLLECTION_NAME = 'filtered_submissions_standard'
INTERACTION_WEIGHT = 1
UNIQUE_USER_WEIGHT = 2
SUBREDDIT_DIVERSITY_WEIGHT = 3

client = MongoClient()
db = client[DB_NAME]

def calculate_communication_scores():
    pipeline = [
        {
            "$unionWith": {
                "coll": SUBMISSIONS_COLLECTION_NAME,
                "pipeline": [{"$project": {"author": 1, "usr_grp": 1, "subreddit_grp": 1, "created_utc": 1, "parent_author": 1, "created_day": 1}}]
            }
        },
        {
            "$addFields": {"month": {"$substr": ["$created_day", 0, 7]}}
        },
        {
            "$group": {
                "_id": {"author": "$author", "user_grp": "$usr_grp", "subreddit_grp": {"$ifNull": ["$subreddit_grp", "ALL"]}},
                "total_interactions": {"$sum": 1},
                "unique_users": {"$addToSet": "$parent_author"},
                "unique_subreddits": {"$addToSet": "$subreddit_grp"}
            }
        },
        {
            "$group": {
                "_id": {"author": "$_id.author", "user_grp": "$_id.user_grp"},
                "subreddit_grp": {"$first": "$_id.subreddit_grp"},
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
                "user_grp": "$_id.user_grp",
                "subreddit_grp": 1,
                "avg_comm_score": 1
            }
        }
    ]

    raw_scores = list(db[COMMENTS_COLLECTION_NAME].aggregate(pipeline))
    return normalize_scores_per_user(raw_scores)

def normalize_scores_per_user(data):
    df = pd.DataFrame(data)
    if 'subreddit_grp' not in df.columns:
        print("DataFrame does not contain 'subreddit_grp'. Check the aggregation pipeline.")
        return
    df['normalized_score'] = df.groupby('author')['avg_comm_score'].transform(lambda x: MinMaxScaler().fit_transform(x.values.reshape(-1, 1)).flatten())
    return df

def export_to_csv_and_plot(data, filename):
    output_dir = "./step4/"
    os.makedirs(output_dir, exist_ok=True)

    df = pd.DataFrame(data)
    if df.empty:
        print("DataFrame is empty. No data to plot.")
        return

    df = normalize_scores_per_user(df)

    # Order for rows and columns
    order = ["SW", "MH", "Otr", "ALL"]

    # Filter and reorder DataFrame
    df['subreddit_grp'] = pd.Categorical(df['subreddit_grp'], categories=order, ordered=True)
    df['user_grp'] = pd.Categorical(df['user_grp'], categories=order, ordered=True)
    df = df.sort_values(by=['user_grp', 'subreddit_grp'])

    # Pivot table for heatmap
    pivot_table = df.pivot_table(index='user_grp', columns='subreddit_grp', values='normalized_score', fill_value=0)

    plt.figure(figsize=(10, 8))
    sns.heatmap(pivot_table, annot=True, fmt=".2f", cmap="YlGnBu")
    plt.title("Per-User Normalized Communication Score Matrix")
    plt.savefig(os.path.join(output_dir, "per_user_normalized_communication_scores_matrix.png"))
    df.to_csv(os.path.join(output_dir, filename), index=False)

def main():
    print("Calculating communication scores...")
    scores_data = calculate_communication_scores()

    print("Exporting to CSV and creating a matrix plot...")
    export_to_csv_and_plot(scores_data, "normalized_communication_scores.csv")

    print("Process completed.")

if __name__ == "__main__":
    main()
