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
                "pipeline": [{"$project": {"author": 1, "usr_grp": 1, "subreddit_grp": 1, "created_utc": 1, "parent_author": 1}}]
            }
        },
        {
            "$group": {
                "_id": {"author": "$author", "user_grp": "$usr_grp", "subreddit_grp": "$subreddit_grp"},
                "total_interactions": {"$sum": 1},
                "unique_users": {"$addToSet": "$parent_author"},
                "unique_subreddits": {"$addToSet": "$subreddit_grp"}
            }
        },
        {
            "$group": {
                "_id": {"author": "$_id.author", "user_grp": "$_id.user_grp"},
                "avg_comm_score": {"$avg": {
                    "$add": [
                        {"$multiply": ["$total_interactions", INTERACTION_WEIGHT]},
                        {"$multiply": [{"$size": "$unique_users"}, UNIQUE_USER_WEIGHT]},
                        {"$multiply": [{"$size": "$unique_subreddits"}, SUBREDDIT_DIVERSITY_WEIGHT]}
                    ]
                }},
                "subreddit_groups": {"$push": "$_id.subreddit_grp"}
            }
        },
        {
            "$project": {
                "author": "$_id.author",
                "user_grp": "$_id.user_grp",
                "avg_comm_score": 1,
                "subreddit_groups": 1
            }
        },
        {
            "$unwind": "$subreddit_groups"
        },
        {
            "$group": {
                "_id": {"author": "$author", "user_grp": "$user_grp", "subreddit_grp": "$subreddit_groups"},
                "avg_comm_score": {"$first": "$avg_comm_score"}
            }
        },
        {
            "$project": {
                "author": "$_id.author",
                "user_grp": "$_id.user_grp",
                "subreddit_grp": "$_id.subreddit_grp",
                "avg_comm_score": 1
            }
        },
        {
            "$unionWith": {
                "coll": COMMENTS_COLLECTION_NAME,
                "pipeline": [
                    {
                        "$group": {
                            "_id": {"author": "$author", "user_grp": "$usr_grp"},
                            "avg_comm_score_ALL": {"$avg": "$avg_comm_score"}
                        }
                    },
                    {
                        "$project": {
                            "author": "$_id.author",
                            "user_grp": "$_id.user_grp",
                            "subreddit_grp": "ALL",
                            "avg_comm_score": "$avg_comm_score_ALL"
                        }
                    }
                ]
            }
        }
    ]
    raw_scores = list(db[COMMENTS_COLLECTION_NAME].aggregate(pipeline))
    return normalize_scores_per_user(raw_scores)

def normalize_scores_per_user(data):
    """ Normalize scores to a range between 0 and 1 for each user """
    df = pd.DataFrame(data)
    df['normalized_score'] = df.groupby('author')['avg_comm_score'].transform(lambda x: MinMaxScaler().fit_transform(x.values.reshape(-1, 1)).flatten())
    return df

def apply_log_scale(data):
    """ Apply logarithmic scaling to the communication scores """
    data['log_comm_score'] = data['avg_comm_score'].apply(lambda x: np.log(x + 1))
    return data

def export_to_csv_and_plot(data, filename):
    # Create the output directory if it doesn't exist
    output_dir = "./step4/"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Convert data to DataFrame
    df = pd.DataFrame(data)
    if df.empty:
        print("DataFrame is empty. No data to plot.")
        return

    # Normalize the scores per user
    df = normalize_scores_per_user(df)

    # Ensure the DataFrame contains the expected columns
    expected_columns = ['user_grp', 'subreddit_grp', 'normalized_score']
    if not all(col in df.columns for col in expected_columns):
        print("Missing expected columns in DataFrame. Cannot create heatmap.")
        return

    # Create a pivot table for the heatmap
    pivot_table = df.pivot_table(index='user_grp', columns='subreddit_grp', values='normalized_score', fill_value=0)
    if pivot_table.empty:
        print("Pivot table is empty. No data to plot.")
        return

    # Plotting the heatmap
    plt.figure(figsize=(10, 8))
    sns.heatmap(pivot_table, annot=True, fmt=".2f", cmap="YlGnBu")
    plt.title("Per-User Normalized Communication Score Matrix")

    # Save the heatmap to the specified directory
    plt.savefig(os.path.join(output_dir, "per_user_normalized_communication_scores_matrix.png"))

    # Save the DataFrame to CSV in the specified directory
    df.to_csv(os.path.join(output_dir, filename), index=False)
    
def main():
    print("Calculating communication scores...")
    scores_data = calculate_communication_scores()

    print("Exporting to CSV and creating a matrix plot...")
    export_to_csv_and_plot(scores_data, "normalized_communication_scores.csv")

    print("Process completed.")

if __name__ == "__main__":
    main()
