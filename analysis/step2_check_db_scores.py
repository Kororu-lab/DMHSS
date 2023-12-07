from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# MongoDB connection setup
client = MongoClient()
db = client['reddit']  # Adjust to your database name
submissions_collection_name = "stat_submissions"  # Adjust to your collection name
comments_collection_name = "stat_comments"       # Adjust to your collection name

# Define user and subreddit groups
user_grps = ['SW', 'MH', 'Otr']
subreddit_grps = ['SW', 'MH', 'Otr']

def load_and_aggregate_data(collection_name):
    collection = db[collection_name]
    cursor = collection.find()
    data = pd.DataFrame(list(cursor))
    
    # Filter out documents without scores
    data = data[data['pos_score'].notna()]

    # Aggregate data
    grouped = data.groupby(['user_grp', 'subreddit_grp']).agg({'pos_score': 'mean'}).reset_index()
    pivot_table = grouped.pivot('user_grp', 'subreddit_grp', 'pos_score')
    pivot_table['ALL'] = pivot_table.mean(axis=1)  # Calculate average for ALL
    pivot_table = pivot_table.reindex(columns=subreddit_grps + ['ALL'])

    return pivot_table

def plot_heatmap(data, title, output_dir):
    plt.figure(figsize=(10, 6))
    sns.heatmap(data, annot=True, cmap='viridis')
    plt.title(title)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/{title.replace(" ", "_")}.png')

# Load and aggregate data from MongoDB
submissions_data = load_and_aggregate_data(submissions_collection_name)
comments_data = load_and_aggregate_data(comments_collection_name)

# Directory to save plots
output_dir = './step2/db/'

# Generate and save matrix plots
plot_heatmap(submissions_data, 'Average POS Score Matrix - Submissions', output_dir)
plot_heatmap(comments_data, 'Average POS Score Matrix - Comments', output_dir)

print("Visualizations saved in:", output_dir)
