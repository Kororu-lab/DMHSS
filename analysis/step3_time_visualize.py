import pymongo
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
from tqdm import tqdm

# MongoDB Connection Parameters
DB_NAME = "reddit"
SUBMISSION_COLLECTION_NAME = "stat_time"
COMMENT_COLLECTION_NAME = "stat_time"

def connect_to_mongodb(db_name):
    client = pymongo.MongoClient()
    db = client[db_name]
    return db

def fetch_and_process_data(collection):
    pipeline = [
        { "$group": {
            "_id": { "day": "$created_day", "group": "$subreddit_grp" },
            "avgTimeScore": { "$avg": "$time_based" }
        }},
        { "$sort": { "_id.day": 1 } }
    ]
    return list(collection.aggregate(pipeline))

def create_dataframe_from_aggregation(data):
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['_id'].apply(lambda x: x['day']))
    df['subreddit_grp'] = df['_id'].apply(lambda x: x['group'])
    df.drop('_id', axis=1, inplace=True)
    df.set_index(['date', 'subreddit_grp'], inplace=True)
    return df

def apply_enhanced_log_scale(df):
    # Apply a log scale transformation with a small constant
    constant = 1e-9  # This constant can be adjusted
    df['avgTimeScore'] = np.log(df['avgTimeScore'] + constant)
    return df

def create_plot(dataframe, output_dir, output_file):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    pivot_df = dataframe['avgTimeScore'].unstack()
    plt.figure(figsize=(12, 6))
    for group in tqdm(pivot_df.columns, desc="Plotting"):
        plt.plot(pivot_df.index, pivot_df[group], label=group)
    
    plt.title("Enhanced Log-Scaled Average Time-based Scores by Subreddit Group Over Time")
    plt.xlabel("Date")
    plt.ylabel("Enhanced Log-Scaled Average Time-based Score")
    plt.legend()
    plt.savefig(os.path.join(output_dir, output_file))
    plt.close()

# Main Execution
db = connect_to_mongodb(DB_NAME)

# Fetch and process data
submissions_data = fetch_and_process_data(db[SUBMISSION_COLLECTION_NAME])
comments_data = fetch_and_process_data(db[COMMENT_COLLECTION_NAME])

# Convert to DataFrame and apply enhanced log scale
df_submissions = apply_enhanced_log_scale(create_dataframe_from_aggregation(submissions_data))
df_comments = apply_enhanced_log_scale(create_dataframe_from_aggregation(comments_data))

# Plotting
create_plot(df_submissions, './step3/', 'submissions_time_based_scores_enhanced_log_scaled.png')
create_plot(df_comments, './step3/', 'comments_time_based_scores_enhanced_log_scaled.png')

print("Plotting complete.")
