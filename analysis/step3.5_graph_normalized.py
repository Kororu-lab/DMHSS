import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pymongo import MongoClient
import numpy as np

# Hyperparameters
DB_NAME = 'reddit'
COMMENTS_COLLECTION_NAME = 'stat_comments'
SUBMISSIONS_COLLECTION_NAME = 'stat_submissions'
INTERACTION_WEIGHT = 1
UNIQUE_USER_WEIGHT = 2
SUBREDDIT_DIVERSITY_WEIGHT = 3
OUTLIER_THRESHOLD_RATIO = 0.90  # Top 20% are considered outliers
BATCH_SIZE = 500  # Adjustable batch size for processing large datasets

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

def export_to_csv_and_plot(data, filename):
    output_dir = "./step4/"
    os.makedirs(output_dir, exist_ok=True)

    # Reordering the DataFrame
    column_order = ["SW", "MH", "Otr", "ALL"]
    row_order = ["SW", "MH", "Otr"]  # Add additional groups if necessary

    # Creating a pivot table with specified row and column order
    pivot_table = data.pivot_table(index='user_grp', columns='subreddit_grp', values='normalized_score', fill_value=0)
    pivot_table = pivot_table.reindex(row_order, axis="index")  # Reorder rows
    pivot_table = pivot_table.reindex(column_order, axis="columns")  # Reorder columns

    # Plotting
    plt.figure(figsize=(8, 4))  # Adjusted plot size (width x height)
    sns.heatmap(pivot_table, annot=True, fmt=".2f", cmap="YlGnBu")
    plt.title("Per-User Normalized Communication Score Matrix")
    plt.savefig(os.path.join(output_dir, filename.replace('.csv', '.png')))
    data.to_csv(os.path.join(output_dir, filename), index=False)


def plot_time_wise_scores(data):
    # Convert 'month' to datetime for plotting
    data['month'] = pd.to_datetime(data['month'], format='%Y-%m')
    
    # Define base color palettes
    base_palettes = {
        "SW": sns.light_palette("blue", n_colors=3),
        "MH": sns.light_palette("red", n_colors=3),
        "Otr": sns.light_palette("green", n_colors=3),
        "ALL": sns.light_palette("grey", n_colors=3)
    }

    # Determine the unique user groups present in the data
    unique_user_groups = data['user_grp'].unique()

    # Create a custom palette for the unique user groups
    custom_palette = {}
    for group in unique_user_groups:
        if group in base_palettes:
            color_palette = base_palettes[group]
            for idx, subreddit in enumerate(data[data['user_grp'] == group]['subreddit_grp'].unique()):
                if idx < len(color_palette):
                    custom_palette[subreddit] = color_palette[idx]

    plt.figure(figsize=(15, 10))
    sns.lineplot(data=data, x='month', y='normalized_score', hue='subreddit_grp', style='user_grp', palette=custom_palette, markers=True)
    plt.title("Time-wise Communication Scores by Subreddit and User Group")
    plt.xlabel("Month")
    plt.ylabel("Normalized Communication Score")
    plt.xticks(rotation=45)
    plt.legend(title='Group')
    plt.tight_layout()

    # Save the plot
    plt.savefig(os.path.join("./step4/", "time_wise_communication_scores.png"))

def main():
    all_scores = []
    for group in ["SW", "MH", "Otr", "ALL"]:
        print(f"Processing {group}...")
        scores = calculate_communication_scores(group)
        scores['subreddit_grp'] = group
        print(scores.head())  # Debugging: Verify the inclusion of 'user_grp'
        all_scores.append(scores)

    combined_df = pd.concat(all_scores, ignore_index=True)
    print("Combined DataFrame sample:")
    print(combined_df.head())  # Debugging: Inspect the combined DataFrame
    export_to_csv_and_plot(combined_df, "normalized_communication_scores.csv")
    export_to_csv_and_plot(combined_df, "normalized_communication_scores.csv")
    plot_time_wise_scores(combined_df)

if __name__ == "__main__":
    main()
