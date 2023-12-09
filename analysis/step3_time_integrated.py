import pymongo
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
from tqdm import tqdm

# MongoDB Connection Parameters
DB_NAME = 'reddit'
COMMENTS_COLLECTION_NAME = 'filtered_comments_standard'
SUBMISSIONS_COLLECTION_NAME = 'filtered_submissions_standard'

# Hyperparameters for Score Calculation
INTERACTION_WEIGHT = 1
UNIQUE_USER_WEIGHT = 2
SUBREDDIT_DIVERSITY_WEIGHT = 3
TIME_BASED_WEIGHT = 4
OUTLIER_THRESHOLD_RATIO = 0.90
BATCH_SIZE = 500

client = pymongo.MongoClient()
db = client[DB_NAME]

def fetch_time_based_scores():
    pipeline = [
        {"$group": {
            "_id": {"author": "$author", "user_grp": "$user_grp"},
            "avg_time_based": {"$avg": "$time_based"}
        }},
        {"$project": {
            "author": "$_id.author",
            "user_grp": "$_id.user_grp",
            "avg_time_based": 1,
            "_id": 0  # Exclude the _id field
        }}
    ]
    time_scores = list(db[SUBMISSIONS_COLLECTION_NAME].aggregate(pipeline, batchSize=BATCH_SIZE))
    return pd.DataFrame(time_scores)

def normalize_scores_with_threshold(data):
    df = pd.DataFrame(data)
    threshold = df['avg_comm_score'].quantile(OUTLIER_THRESHOLD_RATIO)
    df['normalized_score'] = df['avg_comm_score'].apply(lambda x: min(x, threshold))
    df['normalized_score'] = (df['normalized_score'] - df['normalized_score'].min()) / (threshold - df['normalized_score'].min())
    return df

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
        {"$addFields": {"month": {"$substr": ["$created_day", 0, 7]}}},
        {
            "$group": {
                "_id": {"author": "$author", "month": "$month"},
                "total_interactions": {"$sum": 1},
                "unique_users": {"$addToSet": "$parent_author"},
                "unique_subreddits": {"$addToSet": "$subreddit_grp"}
            }
        },
        {
            "$group": {
                "_id": {"author": "$_id.author", "month": "$_id.month"},
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
                "author": 1,
                "user_grp": 1,
                "avg_comm_score": 1,
                "_id": 0
            }
        }
    ]

    raw_scores = list(db[COMMENTS_COLLECTION_NAME].aggregate(pipeline, batchSize=BATCH_SIZE))
    df = pd.DataFrame(raw_scores)

    # Normalize the scores
    df['normalized_comm_score'] = normalize_scores(df, 'avg_comm_score', OUTLIER_THRESHOLD_RATIO)['normalized_score']

    return df

def normalize_scores(dataframe, score_column, threshold_ratio):
    if score_column not in dataframe.columns:
        raise KeyError(f"Column '{score_column}' not found in DataFrame")
    
    threshold = dataframe[score_column].quantile(threshold_ratio)
    dataframe['normalized_score'] = np.minimum(dataframe[score_column], threshold)
    min_score = dataframe['normalized_score'].min()
    dataframe['normalized_score'] = (dataframe['normalized_score'] - min_score) / (threshold - min_score)
    return dataframe

def normalize_scores(dataframe, score_column, threshold_ratio):
    if score_column not in dataframe.columns:
        raise KeyError(f"Column '{score_column}' not found in DataFrame")
    
    threshold = dataframe[score_column].quantile(threshold_ratio)
    dataframe['normalized_score'] = np.minimum(dataframe[score_column], threshold)
    min_score = dataframe['normalized_score'].min()
    max_score = threshold  # Use threshold as the max for normalization
    dataframe['normalized_score'] = (dataframe['normalized_score'] - min_score) / (max_score - min_score)
    return dataframe

def integrate_scores(comm_df, time_df):
    # Debug: Print DataFrame columns and head
    print("Debug: comm_df columns and head", comm_df.columns, comm_df.head())
    print("Debug: time_df columns and head", time_df.columns, time_df.head())

    # Merge operation
    try:
        merged_df = pd.merge(comm_df, time_df, on=['author', 'user_grp'], how='inner')
    except KeyError as e:
        print("Merge operation failed due to missing key:", e)
        raise


    # Calculate integrated score
    merged_df['integrated_score'] = (
        merged_df['normalized_comm_score'] * (INTERACTION_WEIGHT + UNIQUE_USER_WEIGHT + SUBREDDIT_DIVERSITY_WEIGHT) +
        merged_df['avg_time_based'] * TIME_BASED_WEIGHT
    )

    # Normalize integrated score
    normalized_merged_df = normalize_scores(merged_df, 'integrated_score', OUTLIER_THRESHOLD_RATIO)
    return normalized_merged_df

def export_to_csv(dataframe, filename):
    output_dir = "./output/"
    os.makedirs(output_dir, exist_ok=True)
    dataframe.to_csv(os.path.join(output_dir, filename), index=False)

def plot_scores(dataframe, score_column, title, filename):
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=dataframe, x='user_grp', y=score_column, hue='user_grp')
    plt.title(title)
    plt.ylabel('Score')
    plt.xlabel('User Group')
    plt.legend(title='User Group')
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()

def main():
    # Fetch and calculate scores
    time_scores = fetch_time_based_scores()
    comm_scores = calculate_communication_scores()
    comm_scores = normalize_scores(comm_scores, 'avg_comm_score', OUTLIER_THRESHOLD_RATIO) # Normalize
    
    # Integrate and normalize scores
    integrated_scores = integrate_scores(comm_scores, time_scores)
    
    # Export to CSV
    export_to_csv(integrated_scores, "integrated_scores.csv")

    # Plotting scores
    plot_scores(integrated_scores, 'integrated_score', "Integrated User Scores", "integrated_scores.png")

if __name__ == "__main__":
    main()
