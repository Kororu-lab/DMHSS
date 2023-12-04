import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pymongo import MongoClient
from tqdm import tqdm

# Hyperparameters
INTERACTION_WEIGHT = 1
UNIQUE_USER_WEIGHT = 2
SUBREDDIT_DIVERSITY_WEIGHT = 3
COMMENTS_COLLECTION_NAME = 'filtered_comments_standard'  # Set your comments collection name here
SUBMISSIONS_COLLECTION_NAME = 'filtered_submissions_standard'  # Set your submissions collection name here

def calculate_communication_scores(db):
    """ Calculate communication scores based on interactions """
    pipeline = [
        {
            "$group": {
                "_id": {"author": "$author", "user_grp": "$usr_grp", "subreddit_grp": "$subreddit_grp"},
                "total_interactions": {"$sum": 1},
                "unique_users": {"$addToSet": "$parent_author"},
                "unique_subreddits": {"$addToSet": "$subreddit_grp"}
            }
        },
        {
            "$project": {
                "author": "$_id.author",
                "user_grp": "$_id.user_grp",
                "subreddit_grp": "$_id.subreddit_grp",
                "comm_score": {
                    "$add": [
                        {"$multiply": ["$total_interactions", INTERACTION_WEIGHT]},
                        {"$multiply": [{"$size": "$unique_users"}, UNIQUE_USER_WEIGHT]},
                        {"$multiply": [{"$size": "$unique_subreddits"}, SUBREDDIT_DIVERSITY_WEIGHT]}
                    ]
                }
            }
        }
    ]

    return list(db[COMMENTS_COLLECTION_NAME].aggregate(pipeline))

def export_to_csv_and_plot(data, filename):
    """ Export data to a CSV file and create a matrix plot """
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)

    pivot_table = df.pivot_table(index='user_grp', columns='subreddit_grp', values='comm_score', aggfunc='sum', fill_value=0)
    
    plt.figure(figsize=(10, 8))
    sns.heatmap(pivot_table, annot=True, fmt=".1f", cmap="YlGnBu")
    plt.title("Communication Score Matrix")
    plt.savefig("communication_scores_matrix.png")

def main():
    client = MongoClient()
    db = client['reddit']

    print("Calculating communication scores...")
    scores_data = calculate_communication_scores(db)

    print("Exporting to CSV and creating a matrix plot...")
    export_to_csv_and_plot(scores_data, "communication_scores.csv")

    print("Process completed. Data exported to 'communication_scores.csv' and plot saved to 'communication_scores_matrix.png'.")

if __name__ == "__main__":
    main()
