import pymongo
import csv
import matplotlib.pyplot as plt
import os
from collections import defaultdict

### Hyperparameters and Configurations ###
DB_NAME = "reddit"
OUTPUT_DIR = "./step1/"
BATCH_SIZE = 100  # Adjustable batch size for data processing

# Collection names
COLLECTION_SUBMISSIONS = "filtered_submissions_score2"
COLLECTION_COMMENTS = "filtered_comments_score2"

# Ensure output directory exists
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# MongoDB connection
client = pymongo.MongoClient()
db = client[DB_NAME]

# Function to process data from MongoDB
def process_data(collection):
    pipeline = [
        {"$match": {"author": {"$ne": "[deleted]"}}},
        {"$project": {"author": 1, "sentiment_score": 1, "subreddit_grp": 1, "usr_grp": 1}},
        {"$group": {
            "_id": {"author": "$author", "subreddit_grp": "$subreddit_grp", "usr_grp": "$usr_grp"},
            "avg_sentiment": {"$avg": "$sentiment_score"}
        }}
    ]
    return db[collection].aggregate(pipeline, allowDiskUse=True, batchSize=BATCH_SIZE)

# Process submissions and comments
submissions_data = process_data(COLLECTION_SUBMISSIONS)
comments_data = process_data(COLLECTION_COMMENTS)

# Combine data
user_scores = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
for data in submissions_data:
    author = data["_id"]["author"]
    subreddit_grp = data["_id"]["subreddit_grp"]
    usr_grp = data["_id"]["usr_grp"]
    user_scores[author][usr_grp][subreddit_grp].append(data["avg_sentiment"])

for data in comments_data:
    author = data["_id"]["author"]
    subreddit_grp = data["_id"]["subreddit_grp"]
    usr_grp = data["_id"]["usr_grp"]
    user_scores[author][usr_grp][subreddit_grp].append(data["avg_sentiment"])

# Calculate final average sentiment scores
final_scores = {}
for author, usr_grps in user_scores.items():
    final_scores[author] = {usr_grp: {subreddit_grp: sum(scores)/len(scores) for subreddit_grp, scores in subreddits.items()} for usr_grp, subreddits in usr_grps.items()}

# Save the results to CSV and plot
for usr_grp in ["SW", "MH", "Otr"]:
    for subreddit_grp in ["SW", "MH", "Otr"]:
        filename = OUTPUT_DIR + f"sentiment_scores_{usr_grp}_{subreddit_grp}.csv"
        with open(filename, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["User", "Average Sentiment Score"])
            for author, usr_grps in final_scores.items():
                if usr_grp in usr_grps and subreddit_grp in usr_grps[usr_grp]:
                    score = usr_grps[usr_grp][subreddit_grp]
                    writer.writerow([author, score])

        # Plot sentiment score distributions
        scores = [usr_grps[usr_grp][subreddit_grp] for author, usr_grps in final_scores.items() if usr_grp in usr_grps and subreddit_grp in usr_grps[usr_grp]]
        plt.figure()
        plt.hist(scores, bins=30, alpha=0.7, label=f"{usr_grp}-{subreddit_grp} Sentiment Scores")
        plt.title(f'Sentiment Score Distribution: {usr_grp} Users in {subreddit_grp}')
        plt.xlabel('Average Sentiment Score')
        plt.ylabel('Frequency')
        plt.legend()
        plt.savefig(OUTPUT_DIR + f"sentiment_distribution_{usr_grp}_{subreddit_grp}.png")

print("Analysis complete. Data and plots saved to the './step1/' directory.")
