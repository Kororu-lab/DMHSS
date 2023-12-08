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

def normalize_scores(dataframe, score_column, threshold_ratio):
    threshold = dataframe[score_column].quantile(threshold_ratio)
    dataframe['normalized_score'] = np.minimum(dataframe[score_column], threshold)
    min_score = dataframe['normalized_score'].min()
    dataframe['normalized_score'] = (dataframe['normalized_score'] - min_score) / (threshold - min_score)
    return dataframe

def integrate_scores(comm_df, time_df):
    # Merge communication and time-based scores
    merged_df = pd.merge(comm_df, time_df, on=['author', 'user_grp'], how='inner')
    # Calculate integrated score
    merged_df['integrated_score'] = (
        merged_df['normalized_comm_score'] * (INTERACTION_WEIGHT + UNIQUE_USER_WEIGHT + SUBREDDIT_DIVERSITY_WEIGHT) +
        merged_df['avg_time_based'] * TIME_BASED_WEIGHT
    )
    # Normalize integrated score
    return normalize_scores(merged_df, 'integrated_score', OUTLIER_THRESHOLD_RATIO)

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
    
    # Normalize communication scores
    comm_scores = normalize_scores(comm_scores, 'total_interactions', OUTLIER_THRESHOLD_RATIO)
    
    # Integrate and normalize scores
    integrated_scores = integrate_scores(comm_scores, time_scores)
    
    # Export to CSV
    export_to_csv(integrated_scores, "integrated_scores.csv")

    # Plotting scores
    plot_scores(integrated_scores, 'integrated_score', "Integrated User Scores", "integrated_scores.png")

if __name__ == "__main__":
    main()
