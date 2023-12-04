import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pymongo import MongoClient
from datetime import datetime
import numpy as np

# Hyperparameters
DB_NAME = 'reddit'
COMMENTS_COLLECTION_NAME = 'filtered_comments_standard'
SUBMISSIONS_COLLECTION_NAME = 'filtered_submissions_standard'
INTERACTION_WEIGHT = 1
UNIQUE_USER_WEIGHT = 2
SUBREDDIT_DIVERSITY_WEIGHT = 3
OUTLIER_THRESHOLD_RATIO = 0.80  # Top 20% are considered outliers
GROUPS_FOR_TIMEWISE_PLOT = ["SW", "MH", "Otr", "ALL"]  # Groups to include in timewise plot

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
            "$addFields": {"month": {"$substr": ["$created_day", 0, 7]}}  # Extract the month from 'created_day'
        },
        {
            "$group": {
                "_id": {"author": "$author", "user_grp": "$usr_grp", "subreddit_grp": {"$ifNull": ["$subreddit_grp", "ALL"]}, "month": "$month"},
                "total_interactions": {"$sum": 1},
                "unique_users": {"$addToSet": "$parent_author"},
                "unique_subreddits": {"$addToSet": "$subreddit_grp"}
            }
        },
        {
            "$group": {
                "_id": {"author": "$_id.author", "user_grp": "$_id.user_grp", "month": "$_id.month"},
                "subreddit_grp": {"$push": "$_id.subreddit_grp"},
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
                "month": "$_id.month",
                "avg_comm_score": 1
            }
        }
    ]

    raw_scores = list(db[COMMENTS_COLLECTION_NAME].aggregate(pipeline))
    return normalize_scores_with_threshold(raw_scores)

def normalize_scores_with_threshold(data):
    df = pd.DataFrame(data)
    threshold = df['avg_comm_score'].quantile(OUTLIER_THRESHOLD_RATIO)
    df['normalized_score'] = df['avg_comm_score'].apply(lambda x: min(x, threshold))
    df['normalized_score'] = (df['normalized_score'] - df['normalized_score'].min()) / (threshold - df['normalized_score'].min())
    return df

def plot_time_wise_scores(data):
    df = pd.DataFrame(data)
    df['month'] = pd.to_datetime(df['month']).dt.to_period('M')

    plt.figure(figsize=(15, 10))
    for group in GROUPS_FOR_TIMEWISE_PLOT:
        group_df = df[df['subreddit_grp'] == group]
        sns.lineplot(data=group_df, x='month', y='normalized_score', label=group)

    plt.title("Communication Score Over Time by Subreddit Group")
    plt.xlabel("Month")
    plt.ylabel("Normalized Communication Score")
    plt.xticks(rotation=45)
    plt.legend(title='Subreddit Group')
    plt.tight_layout()
    plt.savefig("./step4/time_wise_communication_scores.png")

def export_to_csv_and_plot(data, filename):
    output_dir = "./step4/"
    os.makedirs(output_dir, exist_ok=True)

    df = pd.DataFrame(data)
    if df.empty:
        print("DataFrame is empty. No data to plot.")
        return

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

    print("Creating time-wise plot...")
    plot_time_wise_scores(scores_data)

    print("Process completed.")

if __name__ == "__main__":
    main()
